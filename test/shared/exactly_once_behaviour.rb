# frozen_string_literal: true

module ExactlyOnceBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedExactlyOnceEffects.clear
      SharedExactlyOnceJob.observer = nil
      SharedExactlyOnceJob.performed = Concurrent::Array.new
      SharedCrashingExactlyOnceJob.starts = Concurrent::Array.new
      SharedAlwaysDyingExactlyOnceJob.starts = Concurrent::Array.new
      SharedLimitedExactlyOnceJob.crashing = false
      SharedDeduplicatedExactlyOnceJob.crashing = false
    end

    teardown do
      SharedExactlyOnceEffects.clear
      SolidQueue::ExecutionHooks.clear
    end
  end

  def test_delivers_exactly_once_is_stored_on_its_jobs
    assert_equal :exactly_once, SharedExactlyOnceJob.delivery_mode
    assert_equal :exactly_once, Class.new(SharedExactlyOnceJob).new.delivery_mode

    assert_equal :exactly_once, SolidQueue::Job.find(SharedExactlyOnceJob.perform_later("marked").provider_job_id).delivery_mode
    assert_equal :at_least_once, SolidQueue::Job.find(SharedPlainEffectJob.perform_later("unmarked").provider_job_id).delivery_mode
  end

  def test_a_queued_job_keeps_the_mode_it_was_enqueued_with_when_its_class_changes
    exactly_once = SharedExactlyOnceJob.perform_later("switched-off", raising: true)
    at_least_once = SharedPlainEffectJob.perform_later("switched-on", raising: true)
    SharedExactlyOnceJob.delivery_mode = :at_least_once
    SharedPlainEffectJob.delivers :exactly_once

    2.times { assert_raises(SharedExactlyOnceError) { claim_and_perform } }

    assert_equal [ 0, 1 ], %w[ switched-off switched-on ].map { |name| SharedExactlyOnceEffects.count(name) }
    assert SolidQueue::Job.find(exactly_once.provider_job_id).failed?
    assert SolidQueue::Job.find(at_least_once.provider_job_id).failed?
  ensure
    SharedExactlyOnceJob.delivers :exactly_once
    SharedPlainEffectJob.delivery_mode = nil
  end

  def test_effects_enqueues_and_completion_commit_together
    observed = {}
    SharedExactlyOnceJob.observer = ->(job) do
      observed[:effects] = SharedExactlyOnceEffects.count_elsewhere("committed")
      observed[:status] = Thread.new { SolidQueue.app_executor.wrap { SolidQueue::Job.find(job.provider_job_id).status } }.value
    end
    active_job = SharedExactlyOnceJob.perform_later("committed", enqueuing: true)

    events = capture_events("perform_exactly_once.solid_queue") { claim_and_perform }

    assert_equal({ effects: 0, status: :claimed }, observed)
    assert_equal 1, SharedExactlyOnceEffects.count("committed")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_equal 0, SolidQueue::ClaimedExecution.count
    child = SolidQueue::Job.find_by(class_name: "SharedExactlyOnceChildJob")
    assert child.ready?
    assert_equal 1, events.size
    payload = events.first.payload
    assert_equal active_job.provider_job_id.to_s, payload[:job_id].to_s
    assert_equal worker_process_id.to_s, payload[:process_id].to_s
    assert_equal "SharedExactlyOnceJob", payload[:display_name]
    assert_equal :committed, payload[:outcome]
    assert_equal 50.seconds, payload[:run_time_limit]

    claim_and_perform
    assert_equal 1, SharedExactlyOnceEffects.count("child-committed")
  end

  def test_a_perform_that_raises_rolls_back_its_effects_and_enqueues_and_records_the_failure
    failures = []
    SolidQueue.on_failure { |execution, error| failures << [ execution.job_id.to_s, error.message ] }
    active_job = SharedExactlyOnceJob.perform_later("raised", raising: true, enqueuing: true)

    events = capture_events("perform_exactly_once.solid_queue") do
      assert_raises(SharedExactlyOnceError) { claim_and_perform }
    end

    assert_equal 0, SharedExactlyOnceEffects.count("raised")
    assert_equal 0, jobs_count("SharedExactlyOnceChildJob")
    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SharedExactlyOnceError", job.failed_execution.exception_class
    assert_equal [ [ active_job.provider_job_id.to_s, "raised" ] ], failures
    assert_equal [ :rolled_back ], events.map { |event| event.payload[:outcome] }
    assert_equal 0, SolidQueue::ClaimedExecution.count
  end

  def test_a_completion_that_fails_rolls_back_the_effects_and_enqueues_of_the_perform
    active_job = SharedExactlyOnceJob.perform_later("uncompleted", enqueuing: true)

    events = capture_events("perform_exactly_once.solid_queue") do
      error = assert_raises(SharedExactlyOnceError) { with_failing_completion { claim_and_perform } }
      assert_equal "completion", error.message
    end

    assert_equal [ "uncompleted" ], SharedExactlyOnceJob.performed
    assert_equal 0, SharedExactlyOnceEffects.count("uncompleted")
    assert_equal 0, jobs_count("SharedExactlyOnceChildJob")
    assert SolidQueue::Job.find(active_job.provider_job_id).failed?
    assert_equal [ :rolled_back ], events.map { |event| event.payload[:outcome] }
  end

  def test_a_rescued_error_rolls_back_the_attempt_and_commits_its_retry_with_the_completion
    active_job = SharedRetriedExactlyOnceJob.perform_later("retried")

    claim_and_perform

    assert_equal 0, SharedExactlyOnceEffects.count("retried")
    assert_equal 0, jobs_count("SharedExactlyOnceChildJob")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert SolidQueue::Job.find_by(active_job_id: active_job.job_id, finished_at: nil).ready?

    claim_and_perform
    claim_and_perform

    assert_equal 1, SharedExactlyOnceEffects.count("retried")
    assert_equal 1, SharedExactlyOnceEffects.count("child-retried")
  end

  def test_a_rescued_attempt_rolls_back_its_writes_and_commits_what_follows_with_the_completion
    active_job = SharedReschedulingExactlyOnceJob.perform_later("rescheduled")

    events = capture_events("perform_exactly_once.solid_queue") { claim_and_perform }

    assert_equal [ :committed ], events.map { |event| event.payload[:outcome] }
    assert_equal 0, SharedExactlyOnceEffects.count("rescheduled")
    assert_equal 0, jobs_count("SharedExactlyOnceChildJob")
    assert_equal 1, jobs_count("SharedExactlyOnceFollowUpJob")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?

    claim_and_perform
    assert_equal 1, SharedExactlyOnceEffects.count("follow-up-rescheduled")
  end

  def test_every_nested_attempt_rolls_back_on_its_own_failure
    active_job = SharedNestedAttemptsExactlyOnceJob.perform_later("nested")

    claim_and_perform

    expected = failed_nested_attempt_keeps_enclosing_writes? ? [ 1, 1, 0, 1, 1 ] : [ 0, 0, 0, 1, 1 ]
    assert_equal expected, %w[ before outer inner rescued after ].map { |step| SharedExactlyOnceEffects.count("nested-#{step}") }
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_within_attempt_outside_an_exactly_once_perform_just_yields
    assert_equal :value, ActiveJob::DeliveryModes.within_attempt { :value }
    assert_raises(SharedExactlyOnceError) { ActiveJob::DeliveryModes.within_attempt { raise SharedExactlyOnceError } }

    active_job = SharedPlainAttemptJob.perform_later("plain-attempt")
    claim_and_perform

    assert_equal 1, SharedExactlyOnceEffects.count("plain-attempt")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_a_discarded_error_rolls_back_the_attempt_and_finishes_it
    active_job = SharedDiscardedExactlyOnceJob.perform_later("discarded")

    events = capture_events("perform_exactly_once.solid_queue") { claim_and_perform }

    assert_equal 0, SharedExactlyOnceEffects.count("discarded")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_equal [ :committed ], events.map { |event| event.payload[:outcome] }
  end

  def test_a_worker_that_dies_mid_perform_commits_nothing_and_its_claim_is_released_to_run_once_more
    { fork_exit: ->(process_id) { SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error) },
      orphaned: ->(_) { SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new) },
      pruned: ->(_) { travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune } } }.each do |path, death|
      name = path.to_s
      active_job = SharedCrashingExactlyOnceJob.perform_later(name)
      process_id = path == :orphaned ? unregistered_process_id : register_worker_process
      dying_claim = claim(process_id).sole
      Thread.new { SolidQueue.app_executor.wrap { dying_claim.perform } }.join

      children = jobs_count("SharedExactlyOnceChildJob")
      assert_equal 0, SharedExactlyOnceEffects.count(name), path
      assert SolidQueue::Job.find(active_job.provider_job_id).claimed?, path

      events = capture_events(/(release_uncommitted|fail_many_claimed|death_recovery)\.solid_queue/) { death.call(process_id) }

      assert SolidQueue::Job.find(active_job.provider_job_id).ready?, path
      assert_equal 1, SolidQueue::Job.find(active_job.provider_job_id).arguments["executions"], path
      assert_equal children, jobs_count("SharedExactlyOnceChildJob"), path
      assert_equal [ "release_uncommitted.solid_queue" ], events.map(&:name), path
      payload = events.first.payload
      assert_equal [ active_job.provider_job_id.to_s ], payload[:job_ids].map(&:to_s), path
      assert_equal [ active_job.provider_job_id.to_s ], payload[:released].map(&:to_s), path
      assert_empty payload[:exhausted], path
      assert_empty payload[:locked], path
      assert_equal [ process_id.to_s ], payload[:process_ids].map(&:to_s), path
      assert_equal({ active_job.provider_job_id.to_s => "SharedCrashingExactlyOnceJob" }, payload[:display_names].transform_keys(&:to_s), path)
      assert_kind_of SolidQueue::Processes::ProcessExitError, payload[:error] if path == :fork_exit

      claim_and_perform
      claim_and_perform

      assert_equal [ name, name ], SharedCrashingExactlyOnceJob.starts.select { |start| start == name }, path
      assert_equal 1, SharedExactlyOnceEffects.count(name), path
      assert_equal 1, SharedExactlyOnceEffects.count("child-#{name}"), path
      assert SolidQueue::Job.find(active_job.provider_job_id).finished?, path
      assert_equal children + 1, jobs_count("SharedExactlyOnceChildJob"), path
    end
  end

  def test_a_claim_released_after_every_death_fails_once_its_process_death_cap_is_reached
    [ [ nil, SharedAlwaysDyingExactlyOnceJob, 3 ], [ { attempts: 2 }, SharedAlwaysDyingExactlyOnceJob, 2 ], [ { attempts: 5 }, SharedCappedDyingExactlyOnceJob, 1 ] ].each do |settings, job_class, runs|
      name = "#{job_class.name}-#{runs}"
      active_job = job_class.perform_later(name)
      events = []

      SolidQueue.with(retry_on_process_death: settings) do
        6.times do
          process_id = register_worker_process
          dying_claim = claim(process_id).first or break
          Thread.new { SolidQueue.app_executor.wrap { dying_claim.perform } }.join
          events += capture_events("release_uncommitted.solid_queue") { SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error) }
        end
      end

      job = SolidQueue::Job.find(active_job.provider_job_id)
      assert job.failed?, name
      assert_equal "SolidQueue::Processes::ProcessExitError", job.failed_execution.exception_class, name
      assert_equal runs, job_class.starts.count(name), name
      assert_equal 0, SharedExactlyOnceEffects.count(name), name
      assert_equal [ [ active_job.provider_job_id.to_s ] ] * (runs - 1) + [ [] ], events.map { |event| event.payload[:released].map(&:to_s) }, name
      assert_equal [ [] ] * (runs - 1) + [ [ active_job.provider_job_id.to_s ] ], events.map { |event| event.payload[:exhausted].map(&:to_s) }, name
    end
  end

  def test_an_exactly_once_perform_is_limited_to_the_exactly_once_timeout
    events = capture_events("perform_exactly_once.solid_queue") do
      SharedLimitedRunTimeExactlyOnceJob.perform_later
      claim_and_perform
      SolidQueue.with(max_run_time: 10.seconds) do
        SharedExactlyOnceJob.perform_later("globally-limited")
        claim_and_perform
      end
    end
    assert_equal [ 5.seconds, 10.seconds ], events.map { |event| event.payload[:run_time_limit] }

    SolidQueue.with(exactly_once_timeout: 0.2.seconds) do
      SharedExactlyOnceJob.observer = ->(_) { sleep 5 }
      active_job = SharedExactlyOnceJob.perform_later("slow", enqueuing: true)
      started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

      events = capture_events("perform_exactly_once.solid_queue") do
        assert_raises(SolidQueue::Processes::RunTimeExceededError) { claim_and_perform }
      end

      assert_operator ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started, :<, 2
      assert_equal [ [ 0.2.seconds, :rolled_back ] ], events.map { |event| event.payload.values_at(:run_time_limit, :outcome) }
      job = SolidQueue::Job.find(active_job.provider_job_id)
      assert job.failed?
      assert_equal "SolidQueue::Processes::RunTimeExceededError", job.failed_execution.exception_class
      assert_equal 0, SharedExactlyOnceEffects.count("slow")
      assert_equal 0, jobs_count("SharedExactlyOnceChildJob")
    end
  end

  def test_a_longer_run_time_limit_is_capped_to_the_exactly_once_timeout
    exactly_once = Class.new(ActiveJob::Base) { delivers :exactly_once }
    exactly_once.limits_run_time max: 4.hours
    assert_equal 4.hours, exactly_once.run_time_limit

    active_job = SharedLongRunTimeExactlyOnceJob.perform_later
    assert_equal 50.seconds, SolidQueue::Job.find(active_job.provider_job_id).run_time_limit

    events = capture_events("perform_exactly_once.solid_queue") { claim_and_perform }

    assert_equal [ [ 50.seconds, :committed ] ], events.map { |event| event.payload.values_at(:run_time_limit, :outcome) }
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_the_exactly_once_timeout_must_be_a_positive_duration
    [ 0, -1, "60", nil ].each do |timeout|
      assert_raises(ArgumentError) { SolidQueue.exactly_once_timeout = timeout }
    end
    assert_equal 50.seconds, SolidQueue.exactly_once_timeout
  end

  def test_death_paths_release_exactly_once_claims_and_fail_the_others
    exactly_once = SharedExactlyOnceJob.perform_later("released")
    plain = SharedPlainEffectJob.perform_later("failed")
    process_id = register_worker_process
    assert_equal 2, claim(process_id, limit: 2).size

    events = capture_events(/(release_uncommitted|fail_many_claimed)\.solid_queue/) do
      SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error)
    end

    assert SolidQueue::Job.find(exactly_once.provider_job_id).ready?
    assert SolidQueue::Job.find(plain.provider_job_id).failed?
    assert_equal %w[ fail_many_claimed.solid_queue release_uncommitted.solid_queue ], events.map(&:name).sort
    assert_equal [ plain.provider_job_id.to_s ], events.find { |event| event.name.start_with?("fail_many") }.payload[:job_ids].map(&:to_s)
  end

  def test_a_graceful_release_returns_an_unperformed_exactly_once_claim
    active_job = SharedExactlyOnceJob.perform_later("graceful")
    process_id = register_worker_process
    claim(process_id)

    SolidQueue::ClaimedExecution.release_for_process(process_id)

    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
    claim_and_perform
    assert_equal 1, SharedExactlyOnceEffects.count("graceful")
  end

  def test_a_stale_owner_does_not_run_an_exactly_once_job
    active_job = SharedExactlyOnceJob.perform_later("stale")
    stale_process_id = register_worker_process
    stale_claim = claim(stale_process_id).sole
    SolidQueue::ClaimedExecution.fail_for_process(stale_process_id, process_exit_error)
    current_claim = claim(register_worker_process).sole

    events = capture_events("perform_exactly_once.solid_queue") { stale_claim.perform }
    current_claim.perform

    assert_equal [ :conflict ], events.map { |event| event.payload[:outcome] }
    assert_equal [ "stale" ], SharedExactlyOnceJob.performed
    assert_equal 1, SharedExactlyOnceEffects.count("stale")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_performing_a_claim_twice_runs_it_once
    active_job = SharedExactlyOnceJob.perform_later("twice")
    performed_claim = claim(worker_process_id).sole

    performed_claim.perform
    performed_claim.perform

    assert_equal [ "twice" ], SharedExactlyOnceJob.performed
    assert_equal 1, SharedExactlyOnceEffects.count("twice")
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_concurrency_limited_exactly_once_jobs_keep_their_semaphore_semantics
    first = SharedLimitedExactlyOnceJob.perform_later("first")
    second = SharedLimitedExactlyOnceJob.perform_later("second", raising: true)
    assert SolidQueue::Job.find(second.provider_job_id).blocked?

    SharedLimitedExactlyOnceJob.crashing = true
    crashed_process_id = register_worker_process
    crashed_claim = claim(crashed_process_id).sole
    Thread.new { SolidQueue.app_executor.wrap { crashed_claim.perform } }.join
    SolidQueue::ClaimedExecution.fail_for_process(crashed_process_id, process_exit_error)
    SharedLimitedExactlyOnceJob.crashing = false

    assert SolidQueue::Job.find(first.provider_job_id).ready?
    assert SolidQueue::Job.find(second.provider_job_id).blocked?

    claim_and_perform
    assert SolidQueue::Job.find(first.provider_job_id).finished?
    assert SolidQueue::Job.find(second.provider_job_id).ready?

    assert_raises(SharedExactlyOnceError) { claim_and_perform }
    assert SolidQueue::Job.find(second.provider_job_id).failed?

    third = SharedLimitedExactlyOnceJob.perform_later("third")
    assert SolidQueue::Job.find(third.provider_job_id).ready?
    assert_equal [ 1, 0, 0 ], %w[ first second third ].map { |name| SharedExactlyOnceEffects.count(name) }
  end

  def test_a_claim_failed_at_the_crash_loop_cap_releases_its_semaphore_and_counts_as_failed_in_its_batch
    first = second = nil
    batch = SolidQueue::Batch.enqueue do
      first = SharedLimitedExactlyOnceJob.perform_later("capped-first")
      second = SharedLimitedExactlyOnceJob.perform_later("capped-second")
    end
    assert SolidQueue::Job.find(second.provider_job_id).blocked?

    SharedLimitedExactlyOnceJob.crashing = true
    process_id = register_worker_process
    crashed_claim = claim(process_id).sole
    Thread.new { SolidQueue.app_executor.wrap { crashed_claim.perform } }.join
    SharedLimitedExactlyOnceJob.crashing = false
    SolidQueue.with(retry_on_process_death: { attempts: 1 }) do
      SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error)
    end

    assert SolidQueue::Job.find(first.provider_job_id).failed?
    assert SolidQueue::Job.find(second.provider_job_id).ready?
    claim_and_perform

    batch = SolidQueue::Batch.find_by(id: batch.id)
    assert batch.finished?
    assert_equal [ 1, 1 ], [ batch.completed_jobs, batch.failed_jobs ]
    assert_equal [ 0, 1 ], %w[ capped-first capped-second ].map { |name| SharedExactlyOnceEffects.count(name) }
  end

  def test_batched_exactly_once_jobs_count_toward_their_batch
    batch = SolidQueue::Batch.enqueue do
      SharedExactlyOnceJob.perform_later("batched-success")
      SharedExactlyOnceJob.perform_later("batched-failure", raising: true)
    end

    crashed_process_id = register_worker_process
    assert_equal 2, claim(crashed_process_id, limit: 2).size
    SolidQueue::ClaimedExecution.fail_for_process(crashed_process_id, process_exit_error)
    assert_not SolidQueue::Batch.find_by(id: batch.id).finished?
    assert_equal 0, SolidQueue::FailedExecution.count

    3.times { claim_and_perform_ignoring(SharedExactlyOnceError) }

    batch = SolidQueue::Batch.find_by(id: batch.id)
    assert batch.finished?
    assert_equal 2, batch.total_jobs
    assert_equal 1, batch.completed_jobs
    assert_equal 1, batch.failed_jobs
    assert_equal [ 1, 0 ], %w[ batched-success batched-failure ].map { |name| SharedExactlyOnceEffects.count(name) }
  end

  def test_deduplicated_exactly_once_jobs_hold_their_key_until_they_finish
    SharedDeduplicatedExactlyOnceJob.crashing = true
    SharedDeduplicatedExactlyOnceJob.perform_later("deduplicated")
    crashed_process_id = register_worker_process
    crashed_claim = claim(crashed_process_id).sole
    Thread.new { SolidQueue.app_executor.wrap { crashed_claim.perform } }.join
    SolidQueue::ClaimedExecution.fail_for_process(crashed_process_id, process_exit_error)
    SharedDeduplicatedExactlyOnceJob.crashing = false

    assert_not SharedDeduplicatedExactlyOnceJob.perform_later("deduplicated")
    claim_and_perform

    assert_equal 1, SharedExactlyOnceEffects.count("deduplicated")
    assert SharedDeduplicatedExactlyOnceJob.perform_later("deduplicated")
  end

  def test_at_least_once_jobs_are_unaffected
    raised = SharedPlainEffectJob.perform_later("plain-raised", raising: true)
    events = capture_events("perform_exactly_once.solid_queue") do
      assert_raises(SharedExactlyOnceError) { claim_and_perform }
    end

    assert_empty events
    assert_equal 1, SharedExactlyOnceEffects.count("plain-raised")
    assert SolidQueue::Job.find(raised.provider_job_id).failed?

    died = SharedPlainEffectJob.perform_later("plain-died")
    process_id = register_worker_process
    claim(process_id)
    SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error)
    assert SolidQueue::Job.find(died.provider_job_id).failed?
  end

  def test_exactly_once_performs_run_inside_around_perform_hooks
    order = []
    SolidQueue.around_perform { |_, &block| order << :before; block.call; order << :after }
    SharedExactlyOnceJob.observer = ->(_) { order << :perform }
    SharedExactlyOnceJob.perform_later("hooked")

    claim_and_perform

    assert_equal %i[ before perform after ], order
    assert_equal 1, SharedExactlyOnceEffects.count("hooked")
  end

  private
    def worker_process_id
      @worker_process_id ||= register_worker_process
    end

    def register_worker_process
      SolidQueue::Process.register(kind: "Worker", name: "exactly-once-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def claim(process_id, limit: 1)
      SolidQueue::ReadyExecution.claim([ "*" ], limit, process_id)
    end

    def claim_and_perform
      claim(worker_process_id).each(&:perform)
    end

    def claim_and_perform_ignoring(error_class)
      claim_and_perform
    rescue error_class
    end

    def process_exit_error
      _, status = ::Process.wait2(::Process.spawn("exit 7"))
      SolidQueue::Processes::ProcessExitError.new(status)
    end

    def capture_events(name)
      events = []
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      ActiveSupport::Notifications.subscribed(subscriber, name) { yield }
      events
    end
end
