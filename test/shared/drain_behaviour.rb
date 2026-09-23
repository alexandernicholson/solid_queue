# frozen_string_literal: true

module DrainBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedWorkOffJob.performed = []
      SharedSlowWorkOffJob.events = Concurrent::Array.new
    end
  end

  def test_counting_due_scheduled_jobs_across_queues_and_priorities
    [ [ "default", 1 ], [ "default", 50 ], [ "other", 5 ] ].each do |queue_name, priority|
      SharedWorkOffJob.set(queue: queue_name, priority: priority, wait: 1.hour).perform_later("due")
    end
    SharedWorkOffJob.set(queue: "default", priority: 1, wait: 3.hours).perform_later("later")

    assert_equal 0, SolidQueue::ScheduledExecution.due_count_across("*")

    travel_to 2.hours.from_now do
      assert_equal 3, SolidQueue::ScheduledExecution.due_count_across("*")
      assert_equal 2, SolidQueue::ScheduledExecution.due_count_across([ "default" ])
      assert_equal 1, SolidQueue::ScheduledExecution.due_count_across([ "oth*" ])
      assert_equal 2, SolidQueue::ScheduledExecution.due_count_across("*", priority: 0..10)
      assert_equal 1, SolidQueue::ScheduledExecution.due_count_across([ "default" ], priority: ..10)
    end
  end

  def test_a_worker_that_exits_on_complete_stops_once_its_jobs_are_done
    3.times { |index| SharedWorkOffJob.perform_later("job-#{index}") }
    worker = SolidQueue::Worker.new(queues: "default", threads: 2, polling_interval: 0.05, exit_on_complete: true)

    events = capture_drained_events do
      worker.start
      wait_until_stopped(worker)
    end

    assert_equal %w[ job-0 job-1 job-2 ], SharedWorkOffJob.performed.sort
    assert_equal 1, events.size
    assert_equal [ "default" ], events.first.payload[:queues]
    assert_equal worker.name, events.first.payload[:name]
  ensure
    worker&.stop
  end

  def test_a_draining_worker_waits_for_its_running_jobs
    SharedSlowWorkOffJob.perform_later(0.5)
    worker = SolidQueue::Worker.new(queues: "default", threads: 2, polling_interval: 0.05, exit_on_complete: true)

    subscriber = ->(*) { SharedSlowWorkOffJob.events << :drained }
    ActiveSupport::Notifications.subscribed(subscriber, "drained.solid_queue") do
      worker.start
      wait_until_stopped(worker)
    end

    assert_equal [ :performed, :drained ], SharedSlowWorkOffJob.events.to_a
  ensure
    worker&.stop
  end

  def test_a_draining_worker_waits_for_due_scheduled_jobs_to_be_dispatched
    active_job = SharedWorkOffJob.set(wait: 0.2.seconds).perform_later("scheduled")
    sleep 0.3
    worker = SolidQueue::Worker.new(queues: "default", threads: 1, polling_interval: 0.05, exit_on_complete: true)

    worker.start
    sleep 0.4
    assert worker.alive?
    assert_not worker.drained?

    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    wait_until_stopped(worker)

    assert_equal [ "scheduled" ], SharedWorkOffJob.performed
    assert_equal :finished, job_status(active_job)
  ensure
    worker&.stop
  end

  def test_jobs_outside_the_worker_queues_priority_range_or_due_time_do_not_hold_it_back
    other_queue = SharedWorkOffJob.set(queue: "other").perform_later("other queue")
    low_priority = SharedWorkOffJob.set(priority: 50).perform_later("low priority")
    future = SharedWorkOffJob.set(wait: 1.hour).perform_later("future")
    SharedWorkOffJob.set(priority: 5).perform_later("in range")
    worker = SolidQueue::Worker.new(queues: "default", threads: 1, polling_interval: 0.05, max_priority: 10, exit_on_complete: true)

    worker.start
    wait_until_stopped(worker)

    assert_equal [ "in range" ], SharedWorkOffJob.performed
    assert_equal [ :ready, :ready, :scheduled ], [ other_queue, low_priority, future ].map { |job| job_status(job) }
  ensure
    worker&.stop
  end

  def test_a_worker_without_exit_on_complete_keeps_running_when_idle
    worker = SolidQueue::Worker.new(queues: "default", threads: 1, polling_interval: 0.05)

    events = capture_drained_events do
      worker.start
      sleep 0.4
    end

    assert worker.alive?
    assert_not worker.drained?
    assert_empty events
  ensure
    worker&.stop
  end

  def test_draining_stops_a_fork_mode_supervisor
    assert_supervisor_stops_after_draining(mode: :fork)
  end

  def test_draining_stops_a_standalone_async_supervisor
    assert_supervisor_stops_after_draining(mode: :async)
  end

  def test_draining_stops_an_embedded_async_supervisor
    jobs = enqueue_for_supervisor
    supervisor = SolidQueue::Supervisor.start(mode: :async, standalone: false, **drain_configuration)

    wait_until(timeout: 15) { !supervisor.instance_variable_get(:@thread).alive? }

    assert_equal [ :finished ] * jobs.size, jobs.map { |job| job_status(job) }
    assert_equal 0, registered_process_count
  ensure
    supervisor&.stop
  end

  private
    def assert_supervisor_stops_after_draining(mode:)
      jobs = enqueue_for_supervisor
      pid = fork do
        SolidQueue.after_fork!
        SolidQueue::Supervisor.start(mode: mode, **drain_configuration)
        exit!(0)
      end

      status = wait_for_exit(pid)

      assert status.success?, "supervisor exited with #{status.inspect}"
      assert_equal [ :finished ] * jobs.size, jobs.map { |job| job_status(job) }
      assert_equal 0, registered_process_count
    end

    def enqueue_for_supervisor
      jobs = [ SharedWorkOffJob.perform_later("now"), SharedWorkOffJob.set(wait: 0.1.seconds).perform_later("due") ]
      sleep 0.2
      jobs
    end

    def drain_configuration
      {
        workers: [ { queues: "default", threads: 2, polling_interval: 0.05, exit_on_complete: true } ],
        dispatchers: [ { polling_interval: 0.05, batch_size: 10, concurrency_maintenance: false } ],
        skip_recurring: true
      }
    end

    def wait_until_stopped(worker, timeout: 10)
      wait_until(timeout: timeout) { !worker.alive? }
    end

    def wait_until(timeout: 5)
      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + timeout
      until yield
        flunk "condition not met within #{timeout} seconds" if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
        sleep 0.05
      end
    end

    def wait_for_exit(pid, timeout: 20)
      Timeout.timeout(timeout) { ::Process.waitpid2(pid).last }
    rescue Timeout::Error
      ::Process.kill(:KILL, pid)
      ::Process.waitpid(pid)
      flunk "supervisor #{pid} did not stop after draining"
    end

    def capture_drained_events
      events = Concurrent::Array.new
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      ActiveSupport::Notifications.subscribed(subscriber, "drained.solid_queue") { yield }
      events
    end
end
