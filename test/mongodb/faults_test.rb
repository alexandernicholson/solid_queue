# frozen_string_literal: true

require_relative "test_helper"

class MongoLimitedFaultJob < ActiveJob::Base
  limits_concurrency key: ->(key) { key }, to: 1, duration: 5.minutes

  def perform(*)
  end
end

class MongoFaultsTest < MongoTestCase
  FAULT_APP_NAME = "solid-queue-fault-tests"

  class CommandRecorder
    attr_reader :command_names

    def initialize
      @command_names = []
      @lock = Mutex.new
    end

    def started(event)
      @lock.synchronize { @command_names << event.command_name }
    end

    def succeeded(*)
    end

    def failed(*)
    end
  end

  def setup
    super
    skip "set MONGODB_FAULT_TESTS=1 to run server failpoint tests" unless ENV["MONGODB_FAULT_TESTS"] == "1"

    configured_app_name = SolidQueue::Mongo.client.options[:app_name]
    unless configured_app_name == FAULT_APP_NAME
      flunk "MONGODB_URI must include appName=#{FAULT_APP_NAME} when MONGODB_FAULT_TESTS=1"
    end

    @admin_database = SolidQueue::Mongo.client.use("admin").database
    disable_fail_command
  end

  def teardown
    disable_fail_command if @admin_database
    super
  end

  def test_claim_reads_back_applied_write_concern_error_without_reissuing_update_many
    active_jobs = 3.times.map { |number| MongoRaceJob.new("ambiguous-#{number}") }
    assert_equal 3, SolidQueue::Job.enqueue_all(active_jobs)
    process_id = BSON::ObjectId.new
    recorder = CommandRecorder.new

    with_command_recorder(recorder) do
      with_fail_command(
        failCommands: [ "update" ],
        writeConcernError: {
          code: 91,
          codeName: "ShutdownInProgress",
          errmsg: "injected acknowledgement loss after applying claim"
        }
      ) do
        claims = SolidQueue::ReadyExecution.claim([ "default" ], 3, process_id)

        assert_equal active_jobs.map(&:provider_job_id).sort, claims.map(&:job_id).sort
        assert_equal [ 1 ], claims.map(&:claim_generation).uniq
        assert_equal [ process_id.to_s ], claims.map(&:process_id).uniq
      end
    end

    assert_equal 1, recorder.command_names.count("update"), "an ambiguous update_many must only be issued once"
    claimed = SolidQueue::Mongo.collection(:jobs).find(state: "claimed").to_a
    assert_equal 3, claimed.size
    assert_equal 1, claimed.map { |job| job.fetch("claim_token") }.uniq.size
    assert_equal [ process_id ], claimed.map { |job| job.fetch("process_id") }.uniq
  end

  def test_transient_transaction_write_conflict_retries_the_body_without_duplicate_state
    document_id = BSON::ObjectId.new
    jobs.insert_one(_id: document_id, attempts: 0)
    body_runs = 0
    after_commits = 0
    events = capture_transaction_retry_events do
      with_fail_command(
        failCommands: [ "update" ],
        errorCode: 112,
        errorCodeName: "WriteConflict",
        errorLabels: [ "TransientTransactionError" ]
      ) do
        SolidQueue::Mongo.transaction(operation: "fault_transient_update") do
          body_runs += 1
          jobs.update_one(
            { _id: document_id },
            { "$inc" => { attempts: 1 } },
            **SolidQueue::Mongo.session_options
          )
          SolidQueue::Mongo.after_commit { after_commits += 1 }
        end
      end
    end

    assert_equal 2, body_runs
    assert_equal 1, jobs.find(_id: document_id).first.fetch("attempts")
    assert_equal 1, after_commits
    assert_equal 1, events.size
    assert_retry_event events.first, operation: "fault_transient_update",
      error_label: "TransientTransactionError", phase: :transaction, transaction_attempt: 1
  end

  def test_unknown_commit_result_retries_commit_without_repeating_body_or_after_commit
    document_id = BSON::ObjectId.new
    body_runs = 0
    after_commits = 0
    recorder = CommandRecorder.new
    events = nil

    with_command_recorder(recorder) do
      events = capture_transaction_retry_events do
        with_fail_command(
          failCommands: [ "commitTransaction" ],
          errorCode: 91,
          errorCodeName: "ShutdownInProgress",
          errorLabels: [ "UnknownTransactionCommitResult" ]
        ) do
          SolidQueue::Mongo.transaction(operation: "fault_unknown_commit") do
            body_runs += 1
            jobs.insert_one({ _id: document_id, committed: true }, **SolidQueue::Mongo.session_options)
            SolidQueue::Mongo.after_commit { after_commits += 1 }
          end
        end
      end
    end

    assert_equal 1, body_runs
    assert_equal true, jobs.find(_id: document_id).first.fetch("committed")
    assert_equal 1, after_commits
    assert_equal 2, recorder.command_names.count("commitTransaction")
    assert_equal 1, events.size
    assert_retry_event events.first, operation: "fault_unknown_commit",
      error_label: "UnknownTransactionCommitResult", phase: :commit, transaction_attempt: 1
  end

  def test_batch_and_new_jobs_survive_retry_after_their_first_insertions
    batch = nil
    active_job = MongoRaceJob.new("retried-batch")
    with_fail_command(
      failCommands: [ "commitTransaction" ],
      errorCode: 112,
      errorCodeName: "WriteConflict",
      errorLabels: [ "TransientTransactionError" ]
    ) do
      batch = SolidQueue::Batch.enqueue do
        active_job.enqueue
      end
    end

    persisted_batch = SolidQueue::Batch.find(batch.id)
    persisted_job = SolidQueue::Job.find(active_job.provider_job_id)
    assert_equal 1, persisted_batch.total_jobs
    assert_equal 1, persisted_batch.pending_jobs
    assert_equal batch.id, persisted_job.batch_id
    assert persisted_job.ready?
    assert_equal 1, jobs.count_documents(active_job_id: active_job.job_id)
  end

  def test_blocked_commit_is_bounded_by_transaction_deadline_and_emits_deadline_telemetry
    previous_timeout = SolidQueue.mongo_transaction_timeout
    SolidQueue.mongo_transaction_timeout = 0.3
    elapsed = nil
    events = []

    error = assert_raises(SolidQueue::Mongo::TransactionDeadlineExceeded) do
      capture_transaction_retry_events(events) do
        with_fail_command(
          failCommands: [ "commitTransaction" ],
          blockConnection: true,
          blockTimeMS: 5_000,
          errorCode: 91,
          errorCodeName: "ShutdownInProgress",
          errorLabels: [ "UnknownTransactionCommitResult" ]
        ) do
          started_at = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
          SolidQueue::Mongo.transaction(operation: "fault_blocked_commit") do
            jobs.insert_one(
              { _id: BSON::ObjectId.new, bounded: true },
              **SolidQueue::Mongo.session_options
            )
            sleep 0.4
          end
        ensure
          # Disabling a failpoint waits for its blocked server thread to exit.
          # Measure the client operation, not that administrative cleanup.
          elapsed = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started_at
        end
      end
    end

    assert_operator elapsed, :<, 2.0, "commit took #{elapsed.round(3)}s despite a 300ms deadline"
    assert_equal "fault_blocked_commit", error.operation
    deadline_event = events.find { |event| event.payload[:deadline_exceeded] }
    assert deadline_event, "expected deadline-exceeded transaction telemetry"
    assert_equal "fault_blocked_commit", deadline_event.payload[:operation]
    assert_equal :commit, deadline_event.payload[:phase]
    assert_includes [ "UnknownTransactionCommitResult", "DeadlineExceeded" ], deadline_event.payload[:error_label]
  ensure
    SolidQueue.mongo_transaction_timeout = previous_timeout if defined?(previous_timeout)
  end

  def test_caller_owned_transaction_retries_a_transient_enqueue_error
    active_job = MongoRaceJob.new("caller-owned-retry")
    attempts = 0

    with_fail_command(failCommands: [ "insert" ], errorCode: 112, errorLabels: [ "TransientTransactionError" ]) do
      SolidQueue::Mongo.client.start_session do |session|
        session.with_transaction do
          attempts += 1
          SolidQueue.with_mongo_session(session) { SolidQueue::Job.enqueue(active_job) }
        end
      end
    end

    assert_equal 2, attempts
    assert_equal 1, jobs.count_documents(active_job_id: active_job.job_id)
    assert active_job.successfully_enqueued?
  end

  def test_a_transient_conflict_while_dispatching_a_limited_job_does_not_redispatch_the_others
    keys = 5.times.map { |number| "dispatch-#{number}" }
    keys.each { |key| MongoLimitedFaultJob.set(wait: 1.second).perform_later(key) }
    jobs.update_many({ state: "scheduled" }, { "$set" => { scheduled_at: 1.minute.ago } })
    waits = 0
    original_wait = SolidQueue::Semaphore.method(:wait)
    fault = method(:with_fail_command)
    SolidQueue::Semaphore.singleton_class.define_method(:wait) do |job|
      waits += 1
      if waits == 4
        fault.call(failCommands: [ "update" ], errorCode: 112, errorLabels: [ "TransientTransactionError" ]) { original_wait.call(job) }
      else
        original_wait.call(job)
      end
    end

    SolidQueue::ScheduledExecution.dispatch_next_batch(10)

    assert_equal 5, jobs.count_documents(state: "ready")
    assert_equal 6, waits
  ensure
    SolidQueue::Semaphore.singleton_class.define_method(:wait, original_wait) if original_wait
  end

  def test_an_owner_that_loses_its_claim_during_a_retried_finalize_reports_failure
    MongoRaceJob.perform_later("lost-during-retry")
    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)
    attempts = 0
    owned_filter = claim.send(:ownership_filter)
    claim.define_singleton_method(:ownership_filter) do
      attempts += 1
      attempts == 1 ? owned_filter : owned_filter.merge(claim_token: "taken by a newer owner")
    end

    finalized = with_fail_command(failCommands: [ "commitTransaction" ], errorCode: 112, errorLabels: [ "TransientTransactionError" ]) do
      claim.failed_with(RuntimeError.new("stale"))
    end

    assert_equal 2, attempts
    assert_not finalized
    assert_equal "claimed", jobs.find(_id: claim.bson_id).first.fetch("state")
  end

  def test_enqueue_reports_a_transaction_deadline_as_an_enqueue_error
    previous_timeout = SolidQueue.mongo_transaction_timeout
    SolidQueue.mongo_transaction_timeout = 0.05
    active_job = MongoLimitedFaultJob.new("deadline")

    with_fail_command(failCommands: [ "commitTransaction" ], blockConnection: true, blockTimeMS: 500) do
      assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue(active_job) }
    end

    assert_not active_job.successfully_enqueued?
  ensure
    SolidQueue.mongo_transaction_timeout = previous_timeout if defined?(previous_timeout)
  end

  private
    def jobs
      SolidQueue::Mongo.collection(:jobs)
    end

    def with_fail_command(**data)
      configure_fail_command(mode: { times: 1 }, data: data.merge(appName: FAULT_APP_NAME))
      yield
    ensure
      disable_fail_command
    end

    def configure_fail_command(mode:, data: nil)
      command = { configureFailPoint: "failCommand", mode: mode }
      command[:data] = data if data
      @admin_database.command(command).first
    end

    def disable_fail_command
      configure_fail_command(mode: "off")
    rescue ::Mongo::Error
      # Teardown makes a second cleanup attempt before allowing a fault to
      # leak into another test.
      SolidQueue::Mongo.client.use("admin").database.command(
        configureFailPoint: "failCommand", mode: "off"
      ).first
    end

    def with_command_recorder(recorder)
      SolidQueue::Mongo.client.subscribe(::Mongo::Monitoring::COMMAND, recorder)
      yield
    ensure
      SolidQueue::Mongo.client.unsubscribe(::Mongo::Monitoring::COMMAND, recorder)
    end

    def capture_transaction_retry_events(events = [])
      subscriber = lambda do |*arguments|
        events << ActiveSupport::Notifications::Event.new(*arguments)
      end
      ActiveSupport::Notifications.subscribed(subscriber, "transaction_retry.solid_queue") { yield }
      events
    end

    def assert_retry_event(event, operation:, error_label:, phase:, transaction_attempt:)
      assert_equal operation, event.payload[:operation]
      assert_equal error_label, event.payload[:error_label]
      assert_equal phase, event.payload[:phase]
      assert_equal transaction_attempt, event.payload[:transaction_attempt]
      assert_equal false, event.payload[:deadline_exceeded]
      assert_kind_of ::Mongo::Error, event.payload[:error]
    end
end
