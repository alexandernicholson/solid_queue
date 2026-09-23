# frozen_string_literal: true

require_relative "test_helper"
require "mocha/minitest"
require_relative "../shared/exactly_once_jobs"
require_relative "../shared/exactly_once_behaviour"

class MongoExactlyOnceTest < MongoTestCase
  include ExactlyOnceBehaviour

  FAULT_APP_NAME = "solid-queue-fault-tests"

  test "the exactly-once session is the perform's transaction session" do
    sessions = []
    SharedExactlyOnceJob.observer = ->(_) { sessions << [ SolidQueue.exactly_once_session, SolidQueue::Mongo.current_session, SolidQueue::Mongo.transaction_in_progress?(SolidQueue.exactly_once_session) ] }
    SharedExactlyOnceJob.perform_later("session")

    claim_and_perform

    session, current, in_progress = sessions.sole
    assert_kind_of ::Mongo::Session, session
    assert_same current, session
    assert in_progress
    assert_nil SolidQueue.exactly_once_session
  end

  test "a MongoDB job document stores its delivery mode" do
    active_job = SharedExactlyOnceJob.perform_later("stored")
    legacy = SharedPlainEffectJob.perform_later("legacy")
    SolidQueue::Mongo.collection(:jobs).update_one({ _id: SolidQueue::Mongo.id!(legacy.provider_job_id) }, { "$unset" => { delivery_mode: true } })

    assert_equal "exactly_once", SolidQueue::Mongo.collection(:jobs).find(_id: SolidQueue::Mongo.id!(active_job.provider_job_id)).first["delivery_mode"]
    assert_equal :at_least_once, SolidQueue::Job.find(legacy.provider_job_id).delivery_mode
  end

  test "a sweep skips a claim that a dead process's open transaction still locks and releases it on a later tick" do
    active_job = SharedExactlyOnceJob.perform_later("locked")
    claimed = claim(unregistered_process_id).sole
    holder = SolidQueue::Mongo.client.start_session
    holder.start_transaction(read_concern: { level: :snapshot }, write_concern: { w: :majority })
    SolidQueue::Mongo.collection(:jobs).update_one({ _id: SolidQueue::Mongo.id!(claimed.job_id) }, { "$set" => { started_at: Time.current } }, session: holder)

    events = capture_events("release_uncommitted.solid_queue") do
      SolidQueue::ForkSupervisor.new(SolidQueue::Configuration.new).send(:run_maintenance)
    end

    assert_equal [ [] ], events.map { |event| event.payload[:released] }
    assert_equal [ [ active_job.provider_job_id ] ], events.map { |event| event.payload[:locked] }
    assert SolidQueue::Job.find(active_job.provider_job_id).claimed?

    holder.abort_transaction
    SolidQueue::ForkSupervisor.new(SolidQueue::Configuration.new).send(:run_maintenance)

    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
    claim_and_perform
    assert_equal 1, SharedExactlyOnceEffects.count("locked")
  ensure
    holder&.abort_transaction if holder && SolidQueue::Mongo.transaction_in_progress?(holder)
    holder&.end_session
  end

  test "pruning a process whose exactly-once claim is still locked prunes it and leaves the claim to the orphan sweep" do
    active_job = SharedExactlyOnceJob.perform_later("pruned-locked")
    process_id = register_worker_process
    claimed = claim(process_id).sole
    holder = SolidQueue::Mongo.client.start_session
    holder.start_transaction(read_concern: { level: :snapshot }, write_concern: { w: :majority })
    SolidQueue::Mongo.collection(:jobs).update_one({ _id: SolidQueue::Mongo.id!(claimed.job_id) }, { "$set" => { started_at: Time.current } }, session: holder)

    events = capture_events("release_uncommitted.solid_queue") do
      travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune }
    end

    assert_nil SolidQueue::Process.find_by(id: process_id)
    assert_equal [ [ active_job.provider_job_id ] ], events.map { |event| event.payload[:locked] }
    assert SolidQueue::Job.find(active_job.provider_job_id).claimed?

    holder.abort_transaction
    SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
  ensure
    holder&.abort_transaction if holder && SolidQueue::Mongo.transaction_in_progress?(holder)
    holder&.end_session
  end

  test "a graceful release of a locked exactly-once claim doesn't wait for the lock" do
    active_job = SharedExactlyOnceJob.perform_later("graceful-locked")
    process = SolidQueue::Process.find_by(id: register_worker_process)
    claimed = claim(process.id).sole
    holder = SolidQueue::Mongo.client.start_session
    holder.start_transaction(read_concern: { level: :snapshot }, write_concern: { w: :majority })
    SolidQueue::Mongo.collection(:jobs).update_one({ _id: SolidQueue::Mongo.id!(claimed.job_id) }, { "$set" => { started_at: Time.current } }, session: holder)

    started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    process.deregister

    assert_operator ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started, :<, 2
    assert_nil SolidQueue::Process.find_by(id: process.id)
    assert SolidQueue::Job.find(active_job.provider_job_id).claimed?
  ensure
    holder&.abort_transaction if holder && SolidQueue::Mongo.transaction_in_progress?(holder)
    holder&.end_session
  end

  test "releasing an uncommitted claim is a hinted single-document update with a bounded wait" do
    SharedExactlyOnceJob.perform_later("hinted")
    process_id = register_worker_process
    claim(process_id)
    commands = []
    recorder = Class.new do
      define_method(:started) { |event| commands << event.command if event.command_name.to_s == "findAndModify" }
      define_method(:succeeded) { |*| }
      define_method(:failed) { |*| }
    end.new

    SolidQueue::Mongo.client.subscribe(::Mongo::Monitoring::COMMAND, recorder)
    begin
      SolidQueue::ClaimedExecution.fail_for_process(process_id, SolidQueue::Processes::ProcessMissingError.new)
    ensure
      SolidQueue::Mongo.client.unsubscribe(::Mongo::Monitoring::COMMAND, recorder)
    end

    command = commands.sole
    assert_equal({ "_id" => 1 }, command["hint"].to_h)
    assert_equal SolidQueue::ClaimedExecution::RELEASE_UNCOMMITTED_MAX_TIME_MS, command["maxTimeMS"]
    assert_nil command["autocommit"]
  end

  test "a perform that rescues its timeout commits once when the timeout leaves headroom below the transaction lifetime" do
    skip_unless_fault_tests

    admin = SolidQueue::Mongo.client.use("admin").database
    lifetime = admin.command(getParameter: 1, transactionLifetimeLimitSeconds: 1).first["transactionLifetimeLimitSeconds"]
    assert_operator SolidQueue.exactly_once_timeout, :<=, (lifetime - 10).seconds
    SharedTimeoutReschedulingExactlyOnceJob.performed = Concurrent::Array.new
    admin.command(setParameter: 1, transactionLifetimeLimitSeconds: 2)

    within, within_retries = perform_rescuing_its_timeout("within", exactly_once_timeout: 1.second)

    assert within.finished?
    assert_empty within_retries
    assert_equal 1, jobs_count("SharedExactlyOnceFollowUpJob")
    claim_and_perform
    assert_equal 1, SharedExactlyOnceEffects.count("follow-up-within")

    beyond, beyond_retries = perform_rescuing_its_timeout("beyond", exactly_once_timeout: 4.seconds, mongo_transaction_timeout: 0.1)

    assert beyond.failed?
    assert_equal "SolidQueue::Mongo::TransactionDeadlineExceeded", beyond.failed_execution.exception_class
    assert_equal [ "TransientTransactionError" ], beyond_retries.map { |event| event.payload[:error_label] }
    assert_equal 1, jobs_count("SharedExactlyOnceFollowUpJob")
    assert_equal %w[ within beyond ], SharedTimeoutReschedulingExactlyOnceJob.performed
  ensure
    admin&.command(setParameter: 1, transactionLifetimeLimitSeconds: lifetime) if lifetime
  end

  test "a transient transaction error reruns the perform in a new transaction and commits once" do
    skip_unless_fault_tests

    SharedExactlyOnceJob.perform_later("transient", enqueuing: true)
    claimed = claim(worker_process_id).sole
    retries = capture_events("transaction_retry.solid_queue") do
      with_fail_command(failCommands: [ "insert" ], errorCode: 112, errorLabels: [ "TransientTransactionError" ]) { claimed.perform }
    end

    assert_equal [ "perform_exactly_once" ], retries.map { |event| event.payload[:operation] }
    assert_equal [ "transient", "transient" ], SharedExactlyOnceJob.performed
    assert_equal 1, SharedExactlyOnceEffects.count("transient")
    assert_equal 1, jobs_count("SharedExactlyOnceChildJob")
  end

  test "a transient commit error reruns the perform and commits once" do
    skip_unless_fault_tests

    active_job = SharedExactlyOnceJob.perform_later("commit")
    claimed = claim(worker_process_id).sole
    with_fail_command(failCommands: [ "commitTransaction" ], errorCode: 112, errorLabels: [ "TransientTransactionError" ]) { claimed.perform }

    assert_equal [ "commit", "commit" ], SharedExactlyOnceJob.performed
    assert_equal 1, SharedExactlyOnceEffects.count("commit")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  test "a forked worker killed mid-perform commits nothing and its claim runs once more" do
    skip "fork is unavailable" unless ::Process.respond_to?(:fork)

    active_job = SharedExactlyOnceJob.perform_later("forked", enqueuing: true)
    process_id = register_worker_process
    dead_sessions.delete_many({})
    pid = fork do
      SolidQueue::Mongo.after_fork!
      SharedExactlyOnceJob.observer = ->(_) do
        dead_sessions.insert_one(lsid: SolidQueue.exactly_once_session.session_id)
        ::Process.kill(:KILL, ::Process.pid)
      end
      SolidQueue::ReadyExecution.claim([ "*" ], 1, process_id).each(&:perform)
      exit!(0)
    end
    _, status = ::Process.waitpid2(pid)
    assert status.signaled?

    assert_equal 0, SharedExactlyOnceEffects.count("forked")
    assert_equal 0, jobs_count("SharedExactlyOnceChildJob")
    error = SolidQueue::Processes::ProcessExitError.new(status)
    events = capture_events("release_uncommitted.solid_queue") { SolidQueue::ClaimedExecution.fail_for_process(process_id, error) }
    assert_equal [ [ active_job.provider_job_id ] ], events.map { |event| event.payload[:locked] }
    assert SolidQueue::Job.find(active_job.provider_job_id).claimed?

    kill_dead_sessions
    wait_until_released { SolidQueue::ClaimedExecution.fail_for_process(process_id, error) }

    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
    claim_and_perform
    claim_and_perform
    assert_equal 1, SharedExactlyOnceEffects.count("forked")
    assert_equal 1, SharedExactlyOnceEffects.count("child-forked")
    assert_equal 1, jobs_count("SharedExactlyOnceChildJob")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  ensure
    kill_dead_sessions
  end

  private
    def unregistered_process_id
      BSON::ObjectId.new
    end

    def failed_nested_attempt_keeps_enclosing_writes?
      false
    end

    def perform_rescuing_its_timeout(name, **settings)
      active_job = SharedTimeoutReschedulingExactlyOnceJob.perform_later(name)
      retries = SolidQueue.with(**settings) do
        capture_events("transaction_retry.solid_queue") { claim_and_perform_ignoring(SolidQueue::Mongo::TransactionDeadlineExceeded) }
      end
      [ SolidQueue::Job.find(active_job.provider_job_id), retries ]
    end

    def jobs_count(class_name)
      SolidQueue::Mongo.collection(:jobs).count_documents(class_name: class_name)
    end

    def with_failing_completion
      SolidQueue::ClaimedExecution.any_instance.stubs(:finalize_success).raises(SharedExactlyOnceError, "completion")
      yield
    ensure
      SolidQueue::ClaimedExecution.any_instance.unstub(:finalize_success)
    end

    def dead_sessions
      SolidQueue::Mongo.client["solid_queue_test_dead_sessions"]
    end

    def kill_dead_sessions
      lsids = dead_sessions.find.map { |document| document["lsid"] }
      SolidQueue::Mongo.client.use("admin").database.command(killSessions: lsids) if lsids.any?
      dead_sessions.delete_many({})
    end

    def wait_until_released
      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + 10
      until SolidQueue::Mongo.collection(:jobs).count_documents(state: "claimed").zero?
        flunk "the claim was not released" if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
        yield
        sleep 0.05
      end
    end

    def skip_unless_fault_tests
      skip "set MONGODB_FAULT_TESTS=1 to run server failpoint tests" unless ENV["MONGODB_FAULT_TESTS"] == "1"
      skip "MONGODB_URI must include appName=#{FAULT_APP_NAME}" unless SolidQueue::Mongo.client.options[:app_name] == FAULT_APP_NAME
    end

    def with_fail_command(**data)
      admin = SolidQueue::Mongo.client.use("admin").database
      admin.command(configureFailPoint: "failCommand", mode: { times: 1 }, data: data.merge(appName: FAULT_APP_NAME))
      yield
    ensure
      admin.command(configureFailPoint: "failCommand", mode: "off")
    end
end
