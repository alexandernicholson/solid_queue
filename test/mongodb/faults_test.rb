# frozen_string_literal: true

require_relative "test_helper"

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
    SolidQueue.mongo_transaction_timeout = 0.05
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
          end
        ensure
          # Disabling a failpoint waits for its blocked server thread to exit.
          # Measure the client operation, not that administrative cleanup.
          elapsed = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started_at
        end
      end
    end

    assert_operator elapsed, :<, 1.0, "transaction took #{elapsed.round(3)}s despite a 50ms deadline"
    assert_equal "fault_blocked_commit", error.operation
    deadline_event = events.find { |event| event.payload[:deadline_exceeded] }
    assert deadline_event, "expected deadline-exceeded transaction telemetry"
    assert_equal "fault_blocked_commit", deadline_event.payload[:operation]
    assert_equal :commit, deadline_event.payload[:phase]
    assert_includes [ "UnknownTransactionCommitResult", "DeadlineExceeded" ], deadline_event.payload[:error_label]
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
