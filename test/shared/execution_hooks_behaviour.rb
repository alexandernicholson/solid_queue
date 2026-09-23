# frozen_string_literal: true

module ExecutionHooksBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SolidQueue::ExecutionHooks.clear
      SharedHookedJob.events = []
    end

    teardown do
      SolidQueue::ExecutionHooks.clear
    end
  end

  def test_around_perform_hooks_wrap_perform_in_registration_order
    seen = []
    SolidQueue.around_perform do |execution, &block|
      seen << [ :outer, execution.job_id.to_s, execution.process_id.to_s, execution.job.class_name ]
      block.call
      SharedHookedJob.events += [ :outer_after ]
    end
    SolidQueue.around_perform do |_execution, &block|
      SharedHookedJob.events += [ :inner_before ]
      block.call
      SharedHookedJob.events += [ :inner_after ]
    end
    active_job = SharedHookedJob.perform_later

    claim_and_perform

    assert_equal [ [ :outer, active_job.provider_job_id.to_s, worker_process_id.to_s, "SharedHookedJob" ] ], seen
    assert_equal [ :inner_before, :perform, :inner_after, :outer_after ], SharedHookedJob.events
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  def test_on_failure_hooks_receive_the_execution_and_error_when_perform_records_a_failure
    failures = []
    SolidQueue.on_failure { |execution, error| failures << [ execution.job_id.to_s, error.class, error.message ] }
    SharedHookedJob.perform_later
    failing = SharedHookedJob.perform_later("failure")

    assert_raises(ArgumentError) { 2.times { claim_and_perform } }

    assert_equal [ [ failing.provider_job_id.to_s, ArgumentError, "hooked failure" ] ], failures
  end

  def test_an_on_failure_hook_error_is_reported_without_masking_the_job_error
    reported = []
    SolidQueue.on_failure { |*| raise "hook broke" }
    SolidQueue.on_failure { |_, error| reported << error.message }
    active_job = SharedHookedJob.perform_later("failure")

    thread_errors = capture_events("thread_error.solid_queue") do
      SolidQueue.with(on_thread_error: ->(_) { }) do
        assert_raises(ArgumentError) { claim_and_perform }
      end
    end

    assert_equal [ "hooked failure" ], reported
    assert_equal [ "hook broke" ], thread_errors.map { |event| event.payload[:error].message }
    assert SolidQueue::Job.find(active_job.provider_job_id).failed?
  end

  def test_a_claim_is_failed_when_an_around_perform_hook_raises_before_performing
    SolidQueue.around_perform { |*| raise IOError, "hook unavailable" }
    active_job = SharedHookedJob.perform_later

    assert_raises(IOError) { claim_and_perform }

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "IOError", job.failed_execution.exception_class
    assert_empty SharedHookedJob.events
  end

  def test_a_claim_is_failed_when_an_around_perform_hook_skips_the_perform
    SolidQueue.around_perform { |*| }
    active_job = SharedHookedJob.perform_later

    assert_raises(SolidQueue::ExecutionHooks::NotPerformedError) { claim_and_perform }

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SolidQueue::ExecutionHooks::NotPerformedError", job.failed_execution.exception_class
  end

  def test_the_runner_returns_the_wrapped_value_and_passes_its_arguments
    calls = []
    SolidQueue.around_poll do |worker, &block|
      calls << [ :first, worker ]
      block.call
      :ignored
    end
    SolidQueue.around_claim do |worker, &block|
      calls << [ :claim, worker ]
      block.call
    end
    SolidQueue.around_poll do |worker, &block|
      calls << [ :second, worker ]
      block.call
    end

    assert_equal 42, SolidQueue::ExecutionHooks.run(:around_poll, :worker) { 42 }
    assert_equal [ :claimed ], SolidQueue::ExecutionHooks.run(:around_claim, :worker) { [ :claimed ] }
    assert_equal [ [ :first, :worker ], [ :second, :worker ], [ :claim, :worker ] ], calls
    assert_nil SolidQueue::ExecutionHooks.run(:around_poll, :worker) { nil }
  end

  def test_a_hook_that_calls_its_block_twice_runs_the_wrapped_code_once
    SolidQueue.around_perform do |_execution, &block|
      block.call
      block.call
    end
    SharedHookedJob.perform_later

    claim_and_perform

    assert_equal [ :perform ], SharedHookedJob.events
  end

  def test_the_runner_yields_directly_without_hooks_and_rejects_unknown_kinds
    assert_equal :direct, SolidQueue::ExecutionHooks.run(:around_poll, :worker) { :direct }
    assert_raises(ArgumentError) { SolidQueue::ExecutionHooks.run(:around_everything, :worker) { } }
    assert_raises(ArgumentError) { SolidQueue.around_perform }
  end

  def test_clear_removes_every_hook
    SolidQueue.around_perform { |*, &block| block.call }
    SolidQueue.on_failure { |*| }
    SolidQueue.around_claim { |*, &block| block.call }
    SolidQueue.around_poll { |*, &block| block.call }

    SolidQueue::ExecutionHooks.clear

    assert SolidQueue::ExecutionHooks::KINDS.none? { |kind| SolidQueue::ExecutionHooks.registered?(kind) }
  end

  private
    def worker_process_id
      @worker_process_id ||= SolidQueue::Process.register(kind: "Worker", name: "hooks-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def claim_and_perform
      SolidQueue::ReadyExecution.claim([ "default" ], 1, worker_process_id).each(&:perform)
    end

    def capture_events(name)
      events = []
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      ActiveSupport::Notifications.subscribed(subscriber, name) { yield }
      events
    end
end
