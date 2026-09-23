# frozen_string_literal: true

require "rake"

module OperationsBehaviour
  extend ActiveSupport::Concern

  EXECUTIONS_TO_CLEAR = %w[ BlockedExecution ScheduledExecution ReadyExecution FailedExecution ].freeze

  included do
    setup do
      SharedWorkOffJob.performed = []
    end
  end

  def test_latency_and_count_of_ready_jobs_waiting_longer_than_an_age
    assert_equal 0, SolidQueue::ReadyExecution.latency
    assert_equal 0, SolidQueue::ReadyExecution.count_waiting_longer_than(300)

    SharedWorkOffJob.set(queue: "a").perform_later("old")
    SharedWorkOffJob.set(queue: "b").perform_later("old")

    travel 10.minutes do
      SharedWorkOffJob.perform_later("new")

      assert_equal 2, SolidQueue::ReadyExecution.count_waiting_longer_than(300)
      assert_equal 0, SolidQueue::ReadyExecution.count_waiting_longer_than(900)
      assert_in_delta 600, SolidQueue::ReadyExecution.latency, 2
    end
  end

  def test_latency_counts_from_when_a_scheduled_job_became_due
    SharedWorkOffJob.set(wait: 1.hour).perform_later("scheduled")

    travel 62.minutes do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)

      assert_equal 0, SolidQueue::ReadyExecution.count_waiting_longer_than(300)
      assert_equal 1, SolidQueue::ReadyExecution.count_waiting_longer_than(60)
      assert_in_delta 120, SolidQueue::ReadyExecution.latency, 2
    end
  end

  def test_claimed_jobs_do_not_count_towards_latency
    SharedWorkOffJob.perform_later("claimed")
    SolidQueue::ReadyExecution.claim("*", 1, claiming_process_id)

    travel 10.minutes do
      assert_equal 0, SolidQueue::ReadyExecution.count_waiting_longer_than(300)
      assert_equal 0, SolidQueue::ReadyExecution.latency
    end
  end

  def test_discarding_every_execution_type_in_one_queue_leaves_claimed_jobs_and_other_queues
    cleared = enqueue_in_every_state("a")
    kept = enqueue_in_every_state("b")

    discarded = EXECUTIONS_TO_CLEAR.sum { |name| "SolidQueue::#{name}".constantize.discard_all_in_queue("a") }

    assert_equal 4, discarded
    assert_equal({ claimed: :claimed, failed: nil, ready: nil, blocked: nil, scheduled: nil }, statuses_of(cleared))
    assert_equal({ claimed: :claimed, failed: :failed, ready: :ready, blocked: :blocked, scheduled: :scheduled }, statuses_of(kept))
  end

  def test_claimed_executions_cannot_be_discarded_by_queue
    enqueue_in_every_state("a")

    assert_raises(SolidQueue::Execution::UndiscardableError) { SolidQueue::ClaimedExecution.discard_all_in_queue("a") }
  end

  def test_discarding_all_in_batches_returns_the_number_discarded
    2.times { SharedWorkOffJob.perform_later("discarded") }

    assert_equal 2, SolidQueue::ReadyExecution.discard_all_in_batches
  end

  def test_check_latency_task_passes_when_no_ready_job_waited_too_long
    SharedWorkOffJob.perform_later("fresh")

    out, err, status = invoke_task("solid_queue:check_latency")

    assert_nil status
    assert_equal "OK: no ready jobs have waited longer than 300 seconds.\n", out
    assert_empty err
  end

  def test_check_latency_task_fails_with_the_count_and_oldest_age
    2.times { SharedWorkOffJob.perform_later("stale") }

    travel 10.minutes do
      events = []
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      out, err, status = ActiveSupport::Notifications.subscribed(subscriber, "check_latency.solid_queue") { invoke_task("solid_queue:check_latency", "300") }

      assert_equal 1, status
      assert_empty out
      assert_match(/\A2 ready jobs have waited longer than 300 seconds; the oldest has waited (599|600|601) seconds\.\n\z/, err)
      assert_equal 1, events.size
      assert_equal({ max_age: 300, count: 2 }, events.first.payload.slice(:max_age, :count))
      assert_in_delta 600, events.first.payload[:latency], 2
    end
  end

  def test_check_latency_task_takes_a_max_age
    SharedWorkOffJob.perform_later("waiting")

    travel 2.minutes do
      assert_equal 1, invoke_task("solid_queue:check_latency", "60").last
      assert_nil invoke_task("solid_queue:check_latency", "180").last
    end
  end

  def test_clear_task_discards_one_queue
    cleared = enqueue_in_every_state("a")
    kept = enqueue_in_every_state("b")

    out, err, status = invoke_task("solid_queue:clear", "a")

    assert_nil status
    assert_empty err
    assert_equal "Discarded 4 jobs from queue a.\n", out
    assert_equal({ claimed: :claimed, failed: nil, ready: nil, blocked: nil, scheduled: nil }, statuses_of(cleared))
    assert_equal({ claimed: :claimed, failed: :failed, ready: :ready, blocked: :blocked, scheduled: :scheduled }, statuses_of(kept))
  end

  def test_clear_task_discards_every_queue_when_none_is_given
    jobs = [ enqueue_in_every_state("a"), enqueue_in_every_state("b") ]

    out, = invoke_task("solid_queue:clear")

    assert_equal "Discarded 8 jobs from all queues.\n", out
    jobs.each do |cleared|
      assert_equal({ claimed: :claimed, failed: nil, ready: nil, blocked: nil, scheduled: nil }, statuses_of(cleared))
    end
  end

  def test_clear_task_never_releases_blocked_jobs_to_run
    enqueue_in_every_state("a")
    enqueue_in_every_state("b")

    events = []
    subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
    ActiveSupport::Notifications.subscribed(subscriber, "release_blocked.solid_queue") do
      invoke_task("solid_queue:clear", "a")
      invoke_task("solid_queue:clear")
    end

    assert_empty events.select { |event| event.payload[:released] }
  end

  private
    def enqueue_in_every_state(queue_name)
      failed = SharedFailingWorkOffJob.set(queue: queue_name).perform_later
      SolidQueue.work_off(queues: queue_name)

      claimed = SharedWorkOffJob.set(queue: queue_name).perform_later("claimed")
      SolidQueue::ReadyExecution.claim([ queue_name ], 1, claiming_process_id)

      {
        claimed: claimed,
        failed: failed,
        ready: SharedLimitedWorkOffJob.set(queue: queue_name).perform_later(queue_name),
        blocked: SharedLimitedWorkOffJob.set(queue: queue_name).perform_later(queue_name),
        scheduled: SharedWorkOffJob.set(queue: queue_name, wait: 1.hour).perform_later("scheduled")
      }
    end

    def statuses_of(jobs)
      jobs.transform_values { |active_job| job_status(active_job) }
    end

    def invoke_task(name, *arguments)
      previous_application = Rake.application
      Rake.application = Rake::Application.new
      Rake::Task.define_task(:environment)
      load File.expand_path("../../lib/solid_queue/tasks.rb", __dir__)

      status = nil
      out, err = capture_io do
        Rake.application[name].invoke(*arguments)
      rescue SystemExit => error
        status = error.status
      end

      [ out, err, status ]
    ensure
      Rake.application = previous_application
    end
end
