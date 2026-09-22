# frozen_string_literal: true

require_relative "test_helper"
require_relative "runtime_support"

class MongoRuntimeTest < MongoTestCase
  include MongoRuntimeSupport

  def setup
    super
    runtime_results.delete_many({})
    @child_pids = []
  end

  def teardown
    @child_pids.each { |pid| stop_pid(pid, signal: :KILL) }
    runtime_results.delete_many({})
    super
  end

  def test_thread_worker_executes_an_actual_job_inline
    active_job = enqueue_runtime_job("thread-inline")

    run_worker(mode: :inline, threads: 2).start

    result = wait_for_runtime_event("thread-inline", "completed")
    assert_equal Process.pid, result["pid"]
    wait_until { SolidQueue::Job.find_by_active_job_id(active_job.job_id).state == "finished" }
  end

  def test_async_worker_executes_an_actual_job_on_its_runtime_thread
    active_job = enqueue_runtime_job("thread-async")
    worker = run_worker(mode: :async, threads: 1)

    worker.start
    result = wait_for_runtime_event("thread-async", "completed")

    assert_not_equal Thread.current.object_id, result["thread_id"]
    wait_until { SolidQueue::Job.find_by_active_job_id(active_job.job_id).state == "finished" }
  ensure
    worker&.stop
  end

  def test_fiber_worker_executes_actual_jobs_in_distinct_fibers
    with_fiber_isolation do
      jobs = 2.times.map { |index| enqueue_runtime_job("fiber-#{index}") }

      run_worker(mode: :inline, fibers: 2).start

      results = jobs.map.with_index { |_job, index| wait_for_runtime_event("fiber-#{index}", "completed") }
      assert_equal 2, results.map { |result| result["fiber_id"] }.uniq.size
      jobs.each { |job| wait_until { SolidQueue::Job.find_by_active_job_id(job.job_id).state == "finished" } }
    end
  end

  def test_fork_worker_reconnects_the_driver_and_executes_an_actual_job
    parent_client = SolidQueue::Mongo.client
    active_job = enqueue_runtime_job("fork")
    worker = run_worker(mode: :fork, threads: 1)

    pid = worker.start
    @child_pids << pid
    result = wait_for_runtime_event("fork", "completed")

    assert_equal pid, result["pid"]
    assert_not_equal parent_client.object_id, result["client_id"]
    wait_until { SolidQueue::Job.find_by_active_job_id(active_job.job_id).state == "finished" }

    stop_pid(pid)
    @child_pids.delete(pid)
  end

  def test_sigkill_then_heartbeat_prune_fails_the_claim_without_retrying_it
    token = "killed"
    gate = "hold-killed-job"
    active_job = enqueue_runtime_job(token, gate: gate)
    worker = run_worker(mode: :fork, threads: 1)

    pid = worker.start
    @child_pids << pid
    wait_for_runtime_event(token, "started")
    process = wait_for_registered_worker(pid)
    assert_equal "claimed", SolidQueue::Job.find_by_active_job_id(active_job.job_id).state

    stop_pid(pid, signal: :KILL)
    @child_pids.delete(pid)

    SolidQueue::Mongo.collection(:processes).update_one(
      { _id: SolidQueue::Mongo.id!(process.id) },
      { "$set" => { last_heartbeat_at: 1.hour.ago } }
    )
    SolidQueue::Process.prune

    failed = SolidQueue::Job.find_by_active_job_id(active_job.job_id)
    assert_equal "failed", failed.state
    assert_equal "SolidQueue::Processes::ProcessPrunedError", failed.error.fetch("exception_class")
    assert_nil runtime_results.find(token: token, event: "completed").first

    run_worker(mode: :inline, threads: 1).start
    assert_equal "failed", SolidQueue::Job.find_by_active_job_id(active_job.job_id).state
    assert_nil runtime_results.find(token: token, event: "completed").first
  end

  def test_orphan_recovery_fails_a_claim_whose_process_record_is_missing
    active_job = enqueue_runtime_job("orphan")
    missing_process_id = BSON::ObjectId.new
    claim = SolidQueue::ReadyExecution.claim([ "runtime" ], 1, missing_process_id).fetch(0)

    SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)

    failed = SolidQueue::Job.find_by_active_job_id(active_job.job_id)
    assert_equal claim.job_id, failed.id
    assert_equal "failed", failed.state
    assert_equal "SolidQueue::Processes::ProcessMissingError", failed.error.fetch("exception_class")
    assert_nil runtime_results.find(token: "orphan", event: "started").first
  end

  def test_supervisor_maintenance_fails_claims_orphaned_after_boot
    active_job = enqueue_runtime_job("late-orphan")
    SolidQueue::ReadyExecution.claim([ "runtime" ], 1, BSON::ObjectId.new)
    supervisor = SolidQueue::Supervisor.allocate

    supervisor.send(:run_maintenance)

    assert_equal "failed", SolidQueue::Job.find_by_active_job_id(active_job.job_id).state
  end

  def test_registration_that_wins_the_orphan_scan_race_preserves_the_claim
    active_job = enqueue_runtime_job("registered-owner")
    owner_id = BSON::ObjectId.new
    claim = SolidQueue::ReadyExecution.claim([ "runtime" ], 1, owner_id).fetch(0)
    registered = Queue.new
    start = Queue.new

    registrar = Thread.new do
      start.pop
      registration = SolidQueue::Process.register(
        _id: owner_id,
        kind: "Worker",
        name: "race-owner",
        pid: Process.pid,
        hostname: "test"
      )
      registered << registration
      registration
    rescue Exception => error
      registered << error
      raise
    end
    sweeper = Thread.new do
      start.pop
      registration = registered.pop
      raise registration if registration.is_a?(Exception)

      SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
    end
    2.times { start << true }
    registrar.value
    sweeper.value

    current = SolidQueue::Job.find_by_active_job_id(active_job.job_id)
    assert_equal claim.job_id, current.id
    assert_equal "claimed", current.state
    assert_equal owner_id.to_s, current.process_id
  ensure
    SolidQueue::Process.find(owner_id).deregister if owner_id && SolidQueue::Process.find_by(_id: owner_id)
  end

  def test_stale_owner_cannot_finalize_a_newer_claim
    active_job = enqueue_runtime_job("stale-owner")
    first_process = SolidQueue::Process.register(kind: "Worker", name: "first-owner", pid: Process.pid, hostname: "test")
    second_process = SolidQueue::Process.register(kind: "Worker", name: "second-owner", pid: Process.pid, hostname: "test")

    stale_claim = SolidQueue::ReadyExecution.claim([ "runtime" ], 1, first_process.id).fetch(0)
    assert stale_claim.release
    current_claim = SolidQueue::ReadyExecution.claim([ "runtime" ], 1, second_process.id).fetch(0)

    assert_not stale_claim.failed_with(RuntimeError.new("stale owner"))
    current = SolidQueue::Job.find_by_active_job_id(active_job.job_id)
    assert_equal "claimed", current.state
    assert_equal second_process.id, current.process_id
    assert_equal current_claim.claim_generation, current.claim_generation

    assert current_claim.failed_with(RuntimeError.new("current owner"))
    assert_equal "failed", SolidQueue::Job.find_by_active_job_id(active_job.job_id).state
  ensure
    first_process&.deregister
    second_process&.deregister
  end

  def test_transaction_sessions_are_isolated_between_fibers
    with_fiber_isolation do
      collection = SolidQueue::Mongo.collection(:jobs)
      observations = []
      sessions = 2.times.map { SolidQueue::Mongo.client.start_session }
      fibers = sessions.each_with_index.map do |session, index|
        Fiber.new do
          session.start_transaction
          SolidQueue.with_mongo_session(session) do
            collection.insert_one({ active_job_id: "fiber-session-#{index}" }, session: session)
            observations << [ index, SolidQueue::Mongo.current_session.object_id ]
            Fiber.yield
            observations << [ index, SolidQueue::Mongo.current_session.object_id ]
          end
          session.abort_transaction
        ensure
          session.end_session
        end
      end

      fibers.each(&:resume)
      fibers.reverse_each(&:resume)

      sessions.each_with_index do |session, index|
        assert_equal [ session.object_id ], observations.select { |owner, _| owner == index }.map(&:last).uniq
        assert_equal 0, collection.count_documents(active_job_id: "fiber-session-#{index}")
      end
    end
  end

  def test_backend_cli_check_boots_without_active_record
    stdout, stderr, status = run_cli_check

    assert status.success?, "CLI failed:\n#{stdout}\n#{stderr}"
    assert_includes stdout, "Solid Queue configuration is valid."
    assert_not_includes stderr, "Active Record was loaded"
  end

  private
    def wait_for_registered_worker(pid)
      wait_until do
        SolidQueue::Process.find_by(kind: "Worker", pid: pid)
      end
      SolidQueue::Process.find_by(kind: "Worker", pid: pid)
    end

    def with_fiber_isolation
      previous = ActiveSupport::IsolatedExecutionState.isolation_level
      ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
      yield
    ensure
      ActiveSupport::IsolatedExecutionState.isolation_level = previous
    end
end
