# frozen_string_literal: true

module DeathRecoveryBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedDeathRecoveryJob.executions_seen = []
    end
  end

  def test_retry_on_process_death_requires_a_positive_attempts_cap
    [ {}, { attempts: 0 }, { attempts: "3" }, { attempts: 1.5 }, true, 3 ].each do |settings|
      assert_raises(ArgumentError) { SolidQueue.retry_on_process_death = settings }
    end
    assert_nil SolidQueue.retry_on_process_death

    with_death_recovery(attempts: 4) do
      assert_equal({ attempts: 4 }, SolidQueue.retry_on_process_death)
    end
    SolidQueue.with(retry_on_process_death: { "attempts" => 2 }) do
      assert_equal({ attempts: 2 }, SolidQueue.retry_on_process_death)
    end
  end

  def test_claims_failed_by_process_death_stay_failed_by_default
    active_job = SharedDeathRecoveryJob.perform_later
    process_id = register_worker_process
    claim(process_id)

    events = capture_events("death_recovery.solid_queue") do
      SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error)
    end

    assert_empty events
    assert SolidQueue::Job.find(active_job.provider_job_id).failed?
  end

  def test_a_job_failed_when_its_process_exited_is_retried_and_counts_the_interrupted_run
    active_job = SharedDeathRecoveryJob.perform_later
    process_id = register_worker_process
    claim(process_id)

    events = with_death_recovery(attempts: 3) do
      capture_events("death_recovery.solid_queue") do
        SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error)
      end
    end

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.ready?
    assert_equal 1, job.arguments["executions"]
    assert_equal 1, events.size
    assert_equal [ active_job.provider_job_id.to_s ], events.first.payload[:job_ids].map(&:to_s)
    assert_equal [ active_job.provider_job_id.to_s ], events.first.payload[:retried].map(&:to_s)
    assert_empty events.first.payload[:exhausted]

    claim(register_worker_process).each(&:perform)
    assert_equal [ 2 ], SharedDeathRecoveryJob.executions_seen
  end

  def test_orphaned_claims_are_retried_until_the_cap_is_reached
    active_job = SharedDeathRecoveryJob.perform_later

    events = with_death_recovery(attempts: 2) do
      capture_events("death_recovery.solid_queue") do
        2.times do
          claim(unregistered_process_id)
          SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
        end
      end
    end

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SolidQueue::Processes::ProcessMissingError", job.failed_execution.exception_class
    assert_equal 1, job.arguments["executions"]
    assert_equal [ [ active_job.provider_job_id.to_s ], [] ], events.map { |event| event.payload[:retried].map(&:to_s) }
    assert_equal [ [], [ active_job.provider_job_id.to_s ] ], events.map { |event| event.payload[:exhausted].map(&:to_s) }
  end

  def test_claims_of_a_pruned_process_are_retried
    active_job = SharedDeathRecoveryJob.perform_later
    claim(register_worker_process)

    with_death_recovery(attempts: 2) do
      travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune }
    end

    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
  end

  def test_jobs_failed_for_other_reasons_are_not_retried
    active_job = SharedDeathRecoveryJob.perform_later
    process_id = register_worker_process
    claim(process_id)

    events = with_death_recovery(attempts: 3) do
      capture_events("death_recovery.solid_queue") do
        SolidQueue::ClaimedExecution.fail_for_process(process_id, RuntimeError.new("not a death"))
      end
    end

    assert_empty events
    assert SolidQueue::Job.find(active_job.provider_job_id).failed?
  end

  def test_recovery_skips_jobs_whose_recorded_failure_is_not_a_process_death
    active_job = SharedDeathRecoveryJob.perform_later
    process_id = register_worker_process
    claim(process_id)
    SolidQueue::ClaimedExecution.fail_for_process(process_id, RuntimeError.new("not a death"))

    with_death_recovery(attempts: 3) do
      SolidQueue::DeathRecovery.recover([ active_job.provider_job_id ], SolidQueue::Processes::ProcessMissingError.new)
    end

    assert SolidQueue::Job.find(active_job.provider_job_id).failed?
  end

  private
    def with_death_recovery(**options, &block)
      SolidQueue.with(retry_on_process_death: options, &block)
    end

    def register_worker_process
      SolidQueue::Process.register(kind: "Worker", name: "death-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def claim(process_id)
      SolidQueue::ReadyExecution.claim([ "default" ], 10, process_id)
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
