# frozen_string_literal: true

module WorkOffBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedWorkOffJob.performed = []
      SharedRegistrationProbeJob.seen = nil
    end
  end

  def test_work_off_performs_ready_jobs_and_reports_successes_and_failures
    SharedWorkOffJob.perform_later("a")
    failing = SharedFailingWorkOffJob.perform_later
    SharedWorkOffJob.perform_later("b")

    result = SolidQueue.work_off

    assert_instance_of SolidQueue::WorkOff::Result, result
    assert_equal 2, result.successes
    assert_equal 1, result.failures
    assert_equal [ 2, 1 ], result.to_a
    assert_equal %w[ a b ], SharedWorkOffJob.performed.sort
    assert_equal :failed, job_status(failing)
    assert_equal 0, SolidQueue::ReadyExecution.aggregated_count_across("*")
  end

  def test_work_off_with_nothing_to_do
    assert_equal [ 0, 0 ], SolidQueue.work_off.to_a
  end

  def test_work_off_stops_at_the_limit
    5.times { |index| SharedWorkOffJob.perform_later("job-#{index}") }

    assert_equal [ 3, 0 ], SolidQueue.work_off(limit: 3).to_a
    assert_equal %w[ job-0 job-1 job-2 ], SharedWorkOffJob.performed
    assert_equal 2, SolidQueue::ReadyExecution.aggregated_count_across("*")
  end

  def test_work_off_dispatches_due_scheduled_jobs_and_leaves_future_ones
    SharedWorkOffJob.set(wait: 0.1.seconds).perform_later("due")
    future = SharedWorkOffJob.set(wait: 1.hour).perform_later("future")
    sleep 0.2

    assert_equal [ 1, 0 ], SolidQueue.work_off.to_a
    assert_equal [ "due" ], SharedWorkOffJob.performed
    assert_equal :scheduled, job_status(future)
  end

  def test_work_off_dispatches_due_jobs_before_claiming_so_priorities_hold
    SharedWorkOffJob.set(priority: 10).perform_later("ready")
    SharedWorkOffJob.set(priority: 1, wait: 0.1.seconds).perform_later("due")
    sleep 0.2

    assert_equal [ 1, 0 ], SolidQueue.work_off(limit: 1).to_a
    assert_equal [ "due" ], SharedWorkOffJob.performed
  end

  def test_work_off_picks_up_jobs_that_become_due_while_it_runs
    SharedChainingWorkOffJob.perform_later("chained")

    assert_equal [ 2, 0 ], SolidQueue.work_off.to_a
    assert_equal [ "chained" ], SharedWorkOffJob.performed
  end

  def test_work_off_takes_only_the_given_queues_and_priority_range
    SharedWorkOffJob.set(queue: "a", priority: 1).perform_later("a-1")
    SharedWorkOffJob.set(queue: "a", priority: 20).perform_later("a-20")
    SharedWorkOffJob.set(queue: "b", priority: 1).perform_later("b-1")
    SharedWorkOffJob.set(queue: "b", priority: 1, wait: 0.1.seconds).perform_later("b-scheduled")
    sleep 0.2

    assert_equal [ 1, 0 ], SolidQueue.work_off(queues: [ "a" ], priority: 0..10).to_a
    assert_equal [ "a-1" ], SharedWorkOffJob.performed
    assert_equal 3, SolidQueue::ReadyExecution.aggregated_count_across("*")
  end

  def test_work_off_runs_jobs_under_a_transient_worker_registration
    SharedRegistrationProbeJob.perform_later

    SolidQueue.work_off

    assert_match(/\Awork_off-#{::Process.pid}/, SharedRegistrationProbeJob.seen)
    assert_nil SolidQueue::Process.find_by(name: SharedRegistrationProbeJob.seen)
  end

  def test_work_off_deregisters_when_a_job_aborts_it
    SharedRegistrationProbeJob.perform_later
    aborting = SharedAbortingWorkOffJob.perform_later
    SharedWorkOffJob.perform_later("after")

    assert_raises(SharedWorkOffAbort) { SolidQueue.work_off }

    assert_equal :failed, job_status(aborting)
    assert_nil SolidQueue::Process.find_by(name: SharedRegistrationProbeJob.seen)
    assert_empty SharedWorkOffJob.performed
  end

  def test_work_off_releases_its_file_descriptors
    SolidQueue.work_off
    GC.disable
    open_before = Dir.children("/dev/fd").size

    20.times { SolidQueue.work_off }

    assert_operator Dir.children("/dev/fd").size - open_before, :<, 5
  ensure
    GC.enable
  end

  def test_work_off_reports_its_outcome
    SharedWorkOffJob.perform_later("a")
    SharedFailingWorkOffJob.perform_later

    events = []
    subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
    ActiveSupport::Notifications.subscribed(subscriber, "work_off.solid_queue") { SolidQueue.work_off(queues: "default", limit: 10) }

    assert_equal 1, events.size
    assert_equal({ queues: [ "default" ], limit: 10, priority: nil, successes: 1, failures: 1 }, events.first.payload.slice(:queues, :limit, :priority, :successes, :failures))
  end
end
