# frozen_string_literal: true

module RunTimeBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedRunTimeLimitedJob.observer = nil
      SharedUnlimitedRunTimeJob.observer = nil
      SharedStubbornRunTimeJob.observer = nil
      SharedRetriedRunTimeJob.attempts = 0
    end
  end

  def test_limits_run_time_rejects_a_limit_that_is_not_positive
    job_class = Class.new(ActiveJob::Base)

    assert_raises(ArgumentError) { job_class.limits_run_time max: 0 }
    assert_raises(ArgumentError) { job_class.limits_run_time max: -1.second }
    assert_raises(ArgumentError) { job_class.limits_run_time max: "1 minute" }

    job_class.limits_run_time max: 1.minute
    assert_equal 1.minute, job_class.run_time_limit
  end

  def test_the_global_settings_reject_invalid_durations
    assert_raises(ArgumentError) { SolidQueue.max_run_time = 0 }
    assert_raises(ArgumentError) { SolidQueue.max_run_time = "5 minutes" }
    assert_raises(ArgumentError) { SolidQueue.run_time_grace = -1 }
    assert_raises(ArgumentError) { SolidQueue.run_time_grace = nil }
    assert_nil SolidQueue.max_run_time
    assert_equal 30.seconds, SolidQueue.run_time_grace

    SolidQueue.with(max_run_time: 5.minutes, run_time_grace: 0) do
      assert_equal 5.minutes, SolidQueue.max_run_time
      assert_equal 0, SolidQueue.run_time_grace
    end
  end

  def test_the_run_time_exceeded_error_is_a_timeout_error
    assert_operator SolidQueue::Processes::RunTimeExceededError, :<, Timeout::Error
  end

  def test_a_job_running_past_its_limit_is_interrupted_and_failed
    active_job = SharedShortRunTimeJob.perform_later(5)

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    error = assert_raises(SolidQueue::Processes::RunTimeExceededError) { claim_and_perform }

    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 2
    assert_match(/0\.1 seconds/, error.message)
    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SolidQueue::Processes::RunTimeExceededError", job.failed_execution.exception_class
  end

  def test_the_shorter_of_the_job_and_global_limits_applies
    SolidQueue.with(max_run_time: 0.1.seconds) do
      SharedRunTimeLimitedJob.perform_later(5)
      assert_raises(SolidQueue::Processes::RunTimeExceededError) { claim_and_perform }

      SharedUnlimitedRunTimeJob.perform_later(5)
      assert_raises(SolidQueue::Processes::RunTimeExceededError) { claim_and_perform }
    end

    SolidQueue.with(max_run_time: 1.hour) do
      SharedShortRunTimeJob.perform_later(5)
      assert_raises(SolidQueue::Processes::RunTimeExceededError) { claim_and_perform }
    end
  end

  def test_active_job_retries_handle_the_timeout
    SharedRetriedRunTimeJob.perform_later
    drain_ready_jobs

    assert_equal 2, SharedRetriedRunTimeJob.attempts
  end

  def test_starting_a_limited_claim_records_its_start_and_timeout
    freeze_time
    observed = {}
    observer = ->(job_id) { observed[job_id.to_s] = claim_timestamps(job_id) }
    SharedRunTimeLimitedJob.observer = observer
    SharedUnlimitedRunTimeJob.observer = observer

    limited = unlimited = globally_limited = nil
    SolidQueue.with(run_time_grace: 45.seconds) do
      SolidQueue.with(max_run_time: 1.hour) do
        limited = SharedRunTimeLimitedJob.perform_later
        claim_and_perform
      end
      SolidQueue.with(max_run_time: 5.minutes) do
        globally_limited = SharedRunTimeLimitedJob.perform_later
        claim_and_perform
      end
      unlimited = SharedUnlimitedRunTimeJob.perform_later
      claim_and_perform
    end

    assert_equal [ Time.current, 10.minutes.from_now + 45.seconds ], observed[limited.provider_job_id.to_s]
    assert_equal [ Time.current, 5.minutes.from_now + 45.seconds ], observed[globally_limited.provider_job_id.to_s]
    assert_equal [ nil, nil ], observed[unlimited.provider_job_id.to_s]
  end

  def test_the_sweep_fails_started_claims_once_their_timeout_and_grace_pass
    swept = []
    SharedRunTimeLimitedJob.observer = ->(_) do
      swept << travel(10.minutes) { SolidQueue::ClaimedExecution.fail_timed_out }
      swept << travel(11.minutes) { SolidQueue::ClaimedExecution.fail_timed_out }
    end
    active_job = SharedRunTimeLimitedJob.perform_later

    events = capture_events("run_time_exceeded.solid_queue") { claim_and_perform }

    assert_equal [ 0, 1 ], swept
    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SolidQueue::Processes::RunTimeExceededError", job.failed_execution.exception_class
    assert_equal 1, events.size
    payload = events.first.payload
    assert_equal active_job.provider_job_id.to_s, payload[:job_id].to_s
    assert_equal worker_process_id.to_s, payload[:process_id].to_s
    assert_equal 10.minutes, payload[:max_run_time]
    assert_kind_of Time, payload[:started_at]
    assert_equal "SharedRunTimeLimitedJob", payload[:display_name]
  end

  def test_the_sweep_reports_only_claims_it_failed_itself
    swept = events = nil
    SharedRunTimeLimitedJob.observer = ->(_) do
      SolidQueue::ClaimedExecution.any_instance.stubs(:failed_with).returns(false)
      events = capture_events("run_time_exceeded.solid_queue") { swept = travel(11.minutes) { SolidQueue::ClaimedExecution.fail_timed_out } }
    ensure
      SolidQueue::ClaimedExecution.any_instance.unstub(:failed_with)
    end
    active_job = SharedRunTimeLimitedJob.perform_later

    claim_and_perform

    assert_equal 0, swept
    assert_empty events
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_a_started_limited_claim_does_not_run_again_when_performed_twice
    claim = nil
    runs = 0
    SharedRunTimeLimitedJob.observer = ->(_) do
      runs += 1
      claim.perform if runs == 1
    end
    active_job = SharedRunTimeLimitedJob.perform_later
    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, worker_process_id).first

    claim.perform

    assert_equal 1, runs
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_the_sweep_ignores_claims_without_a_limit
    swept = nil
    SharedUnlimitedRunTimeJob.observer = ->(_) { swept = travel(1.year) { SolidQueue::ClaimedExecution.fail_timed_out } }
    active_job = SharedUnlimitedRunTimeJob.perform_later

    claim_and_perform

    assert_equal 0, swept
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_supervisor_maintenance_fails_a_job_that_swallowed_its_timeout
    SharedStubbornRunTimeJob.observer = ->(_) do
      sleep 0.05
      SolidQueue::ForkSupervisor.new(SolidQueue::Configuration.new).send(:run_maintenance)
    end
    active_job = SharedStubbornRunTimeJob.perform_later

    events = SolidQueue.with(run_time_grace: 0) do
      capture_events("run_time_exceeded.solid_queue") { claim_and_perform }
    end

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SolidQueue::Processes::RunTimeExceededError", job.failed_execution.exception_class
    assert_equal [ 0.1.seconds ], events.map { |event| event.payload[:max_run_time] }
  end

  def test_a_limited_claim_that_started_is_not_released_for_a_second_run
    SharedRunTimeLimitedJob.observer = ->(_) { SolidQueue::ClaimedExecution.release_for_process(worker_process_id) }
    active_job = SharedRunTimeLimitedJob.perform_later

    claim_and_perform

    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_empty SolidQueue::ReadyExecution.claim([ "default" ], 1, worker_process_id)
  end

  def test_a_released_limited_claim_does_not_run
    performed = []
    SharedRunTimeLimitedJob.observer = ->(job_id) { performed << job_id }
    active_job = SharedRunTimeLimitedJob.perform_later
    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, worker_process_id).first
    claim.release

    claim.perform

    assert_empty performed
    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
  end

  private
    def worker_process_id
      @worker_process_id ||= SolidQueue::Process.register(kind: "Worker", name: "run-time-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def claim_and_perform
      SolidQueue::ReadyExecution.claim([ "default" ], 1, worker_process_id).each(&:perform)
    end

    def drain_ready_jobs
      10.times do
        claims = SolidQueue::ReadyExecution.claim([ "*" ], 10, worker_process_id)
        return if claims.empty?

        claims.each(&:perform)
      end
      flunk "ready jobs did not drain"
    end

    def capture_events(name)
      events = []
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      ActiveSupport::Notifications.subscribed(subscriber, name) { yield }
      events
    end
end
