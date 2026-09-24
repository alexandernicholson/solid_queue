# frozen_string_literal: true

module DeliveryGuaranteesBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      @performed = Concurrent::Array.new
      SharedDeliveryJob.recorder = ->(key) { @performed << key }
      SharedInterruptedDeliveryJob.starts = Concurrent::Array.new
      SharedInterruptedDeliveryJob.completions = Concurrent::Array.new
      SharedSweptDeliveryJob.performed = Concurrent::Array.new
      SharedSweptDeliveryJob.observer = nil
      SharedAtMostOnceJob.starts = Concurrent::Array.new
      SharedAtMostOnceJob.observer = nil
    end

    teardown do
      SharedDeliveryJob.recorder = nil
    end
  end

  def test_concurrent_claimers_perform_every_job_exactly_once
    keys = 120.times.map { |number| "job-#{number}" }
    ActiveJob.perform_all_later(keys.map { |key| SharedDeliveryJob.new(key) })

    claimers = 6.times.map do
      Thread.new do
        SolidQueue.app_executor.wrap do
          process_id = register_worker_process
          loop do
            claims = claim(process_id, limit: 4)
            break if claims.empty?

            claims.each(&:perform)
          end
        end
      end
    end
    claimers.each(&:join)

    assert_equal keys.sort, @performed.sort
    assert_equal keys.size, @performed.uniq.size
    assert_equal 0, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::ClaimedExecution.count
  end

  def test_a_finished_job_is_never_claimed_or_performed_again
    active_job = SharedDeliveryJob.perform_later("finished")
    process_id = register_worker_process
    finished_claim = claim(process_id).sole
    finished_claim.perform

    assert_not finished_claim.release
    assert_not finished_claim.failed_with(RuntimeError.new("late failure"))
    with_death_recovery(attempts: 3) do
      SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error)
      SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
      travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune }
    end
    3.times { assert_empty claim(register_worker_process) }
    assert_equal [ 0, 0 ], SolidQueue.work_off.to_a

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.finished?
    assert_not job.failed?
    assert_equal [ "finished" ], @performed
  end

  def test_a_stale_owner_cannot_finish_fail_or_release_a_claim_held_by_a_newer_owner
    active_job = SharedDeliveryJob.perform_later("contended")
    stale_process_id = register_worker_process
    stale_claim = claim(stale_process_id).sole
    with_death_recovery(attempts: 3) do
      SolidQueue::ClaimedExecution.fail_for_process(stale_process_id, process_exit_error)
    end
    current_process_id = register_worker_process
    current_claim = claim(current_process_id).sole
    assert_equal stale_claim.job_id.to_s, current_claim.job_id.to_s

    assert_not stale_claim.release
    assert_not stale_claim.failed_with(RuntimeError.new("stale failure"))
    stale_claim.perform

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.claimed?
    assert_equal current_process_id.to_s, job.claimed_execution.process_id.to_s

    current_claim.perform

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.finished?
    assert_not job.failed?
  end

  def test_a_claim_whose_worker_died_mid_perform_runs_again_exactly_once_more
    { fork_exit: ->(process_id) { SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error) },
      orphaned: ->(_) { SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new) },
      pruned: ->(_) { travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune } } }.each do |path, death|
      active_job = SharedInterruptedDeliveryJob.perform_later(path.to_s)
      process_id = path == :orphaned ? unregistered_process_id : register_worker_process
      dying_claim = claim(process_id).sole
      Thread.new { SolidQueue.app_executor.wrap { dying_claim.perform } }.join

      assert SolidQueue::Job.find(active_job.provider_job_id).claimed?, path

      events = with_death_recovery(attempts: 2) do
        capture_events("death_recovery.solid_queue") { death.call(process_id) }
      end

      assert_equal [ [ active_job.provider_job_id.to_s ] ], events.map { |event| event.payload[:retried].map(&:to_s) }, path
      claim(register_worker_process).each(&:perform)
      with_death_recovery(attempts: 2) do
        SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
        travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune }
      end
      assert_empty claim(register_worker_process), path

      assert_equal [ path.to_s, path.to_s ], SharedInterruptedDeliveryJob.starts.select { |key| key == path.to_s }, path
      assert_equal [ path.to_s ], SharedInterruptedDeliveryJob.completions.select { |key| key == path.to_s }, path
      assert SolidQueue::Job.find(active_job.provider_job_id).finished?, path
    end
  end

  def test_a_job_past_its_run_time_limit_is_failed_by_the_sweep_and_not_retried_as_a_process_death
    SharedSweptDeliveryJob.observer = ->(_) do
      sleep 0.05
      SolidQueue::ForkSupervisor.new(SolidQueue::Configuration.new).send(:run_maintenance)
    end
    active_job = SharedSweptDeliveryJob.perform_later("swept")

    events = with_death_recovery(attempts: 3) do
      SolidQueue.with(run_time_grace: 0) do
        capture_events(/(run_time_exceeded|death_recovery)\.solid_queue/) { claim(register_worker_process).each(&:perform) }
      end
    end

    job = SolidQueue::Job.find(active_job.provider_job_id)
    assert job.failed?
    assert_equal "SolidQueue::Processes::RunTimeExceededError", job.failed_execution.exception_class
    assert_equal [ "run_time_exceeded.solid_queue" ], events.map(&:name)
    assert_empty claim(register_worker_process)
    assert_equal [ "swept" ], SharedSweptDeliveryJob.performed
  end

  def test_delivery_modes_default_to_at_least_once_and_reject_anything_else
    assert_equal :at_least_once, SolidQueue.default_delivery_mode
    assert_nil SharedDeliveryJob.delivery_mode
    assert_equal :at_least_once, SharedDeliveryJob.new("default").delivery_mode
    assert_equal :at_most_once, SharedAtMostOnceJob.new("class").delivery_mode
    assert_equal :at_most_once, Class.new(SharedAtMostOnceJob).new.delivery_mode
    assert_equal :exactly_once, Class.new(ActiveJob::Base) { delivers "exactly_once" }.delivery_mode

    [ :twice, "sometimes", nil, 1 ].each do |mode|
      assert_raises(ArgumentError) { Class.new(ActiveJob::Base).delivers(mode) }
      assert_raises(ArgumentError) { SolidQueue.default_delivery_mode = mode }
    end
    assert_equal :at_least_once, SolidQueue.default_delivery_mode
  end

  def test_the_delivery_mode_is_resolved_and_stored_at_enqueue
    by_default = SolidQueue.with(default_delivery_mode: :at_most_once) { SharedDeliveryJob.perform_later("global") }
    by_class = SharedAtMostOnceJob.perform_later("class")
    by_instance = SharedPerInstanceDeliveryJob.perform_later(:exactly_once)

    assert_equal :at_least_once, SolidQueue.default_delivery_mode
    assert_equal [ :at_most_once, :at_most_once, :exactly_once ], [ by_default, by_class, by_instance ].map { |job| SolidQueue::Job.find(job.provider_job_id).delivery_mode }
    assert_raises(ArgumentError) { SharedPerInstanceDeliveryJob.perform_later(:twice) }
  end

  def test_an_at_most_once_job_never_runs_again_once_it_started
    { fork_exit: ->(process_id) { SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error) },
      orphaned: ->(_) { SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new) },
      pruned: ->(_) { travel(SolidQueue.process_alive_threshold + 1.minute) { SolidQueue::Process.prune } } }.each do |path, death|
      active_job = SharedAtMostOnceJob.perform_later(path.to_s, dying: true)
      process_id = path == :orphaned ? unregistered_process_id : register_worker_process
      dying_claim = claim(process_id).sole
      Thread.new { SolidQueue.app_executor.wrap { dying_claim.perform } }.join

      events = with_death_recovery(attempts: 3) do
        capture_events("death_recovery.solid_queue") { death.call(process_id) }
      end

      job = SolidQueue::Job.find(active_job.provider_job_id)
      assert job.failed?, path
      assert_empty events, path
      assert_empty claim(register_worker_process), path
      assert_equal [ path.to_s ], SharedAtMostOnceJob.starts.select { |key| key == path.to_s }, path
    end
  end

  def test_a_started_at_most_once_claim_is_not_released_or_performed_again
    process_id = register_worker_process
    SharedAtMostOnceJob.observer = ->(_) { SolidQueue::ClaimedExecution.release_for_process(process_id) }
    active_job = SharedAtMostOnceJob.perform_later("graceful")
    started_claim = claim(process_id).sole

    started_claim.perform
    started_claim.perform

    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_empty claim(register_worker_process)
    assert_equal [ "graceful" ], SharedAtMostOnceJob.starts
  end

  def test_an_unstarted_at_most_once_claim_is_released_on_graceful_shutdown_and_runs_once
    active_job = SharedAtMostOnceJob.perform_later("unstarted")
    process_id = register_worker_process
    released_claim = claim(process_id).sole
    SolidQueue::ClaimedExecution.release_for_process(process_id)

    released_claim.perform
    claim(register_worker_process).each(&:perform)

    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_equal [ "unstarted" ], SharedAtMostOnceJob.starts
  end

  def test_retries_on_process_death_requires_a_positive_integer
    [ 0, -1, "2", 1.5, nil ].each do |attempts|
      assert_raises(ArgumentError) { Class.new(ActiveJob::Base).retries_on_process_death(attempts: attempts) }
    end
    assert_equal 2, SharedCappedDeathRetryJob.process_death_attempts
    assert_nil SharedDeliveryJob.process_death_attempts
  end

  def test_a_job_class_cap_retries_its_jobs_without_the_global_setting
    active_job = SharedCappedDeathRetryJob.perform_later
    plain = SharedDeliveryJob.perform_later("uncapped")

    events = 2.times.map do
      process_id = register_worker_process
      claim(process_id, limit: 2)
      capture_events("death_recovery.solid_queue") { SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error) }
    end.flatten

    assert_nil SolidQueue.retry_on_process_death
    assert SolidQueue::Job.find(active_job.provider_job_id).failed?
    assert SolidQueue::Job.find(plain.provider_job_id).failed?
    assert_equal [ [ active_job.provider_job_id.to_s ], [ active_job.provider_job_id.to_s ] ], events.map { |event| event.payload[:job_ids].map(&:to_s) }
    assert_equal [ [ active_job.provider_job_id.to_s ], [] ], events.map { |event| event.payload[:retried].map(&:to_s) }
    assert_equal [ [], [ active_job.provider_job_id.to_s ] ], events.map { |event| event.payload[:exhausted].map(&:to_s) }
  end

  def test_a_job_class_cap_wins_over_the_global_setting
    strict = SharedStrictDeathRetryJob.perform_later
    plain = SharedDeliveryJob.perform_later("global")
    process_id = register_worker_process
    claim(process_id, limit: 2)

    events = with_death_recovery(attempts: 3) do
      capture_events("death_recovery.solid_queue") { SolidQueue::ClaimedExecution.fail_for_process(process_id, process_exit_error) }
    end

    assert SolidQueue::Job.find(strict.provider_job_id).failed?
    assert SolidQueue::Job.find(plain.provider_job_id).ready?
    assert_equal [ plain.provider_job_id.to_s ], events.sole.payload[:retried].map(&:to_s)
    assert_equal [ strict.provider_job_id.to_s ], events.sole.payload[:exhausted].map(&:to_s)
  end

  private
    def with_death_recovery(**options, &block)
      SolidQueue.with(retry_on_process_death: options, &block)
    end

    def register_worker_process
      SolidQueue::Process.register(kind: "Worker", name: "delivery-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def claim(process_id, limit: 10)
      SolidQueue::ReadyExecution.claim([ "*" ], limit, process_id)
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
