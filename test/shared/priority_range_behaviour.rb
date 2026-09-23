# frozen_string_literal: true

module PriorityRangeBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedWorkOffJob.performed = []
    end
  end

  def test_claiming_with_a_priority_range_takes_only_jobs_within_it
    enqueue_with_priorities "default", 0, 3, 5, 7, 10

    claimed = SolidQueue::ReadyExecution.claim("*", 10, claiming_process_id, priority: 3..7)

    assert_equal [ 3, 5, 7 ], claimed_priorities(claimed)
    assert_equal 2, SolidQueue::ReadyExecution.aggregated_count_across("*")
  end

  def test_claiming_with_open_ended_ranges
    enqueue_with_priorities "default", 0, 3, 5, 7, 10

    assert_equal [ 7, 10 ], claimed_priorities(SolidQueue::ReadyExecution.claim("*", 10, claiming_process_id, priority: 7..))
    assert_equal [ 0 ], claimed_priorities(SolidQueue::ReadyExecution.claim("*", 10, claiming_process_id, priority: ...3))
    assert_equal [ 3, 5 ], claimed_priorities(SolidQueue::ReadyExecution.claim("*", 10, claiming_process_id, priority: ..5))
  end

  def test_claiming_with_a_range_takes_the_highest_priority_jobs_within_it_first
    enqueue_with_priorities "default", 9, 1, 6, 4

    claimed = SolidQueue::ReadyExecution.claim("*", 2, claiming_process_id, priority: 2..10)

    assert_equal [ 4, 6 ], claimed_priorities(claimed)
  end

  def test_claiming_named_and_prefixed_queues_with_a_range_follows_queue_order
    enqueue_with_priorities "urgent", 1, 20
    enqueue_with_priorities "batch_a", 2, 30
    enqueue_with_priorities "other", 3

    claimed = SolidQueue::ReadyExecution.claim([ "urgent", "batch*" ], 10, claiming_process_id, priority: 0..10)

    assert_equal [ [ "urgent", 1 ], [ "batch_a", 2 ] ], claimed.map { |execution| [ execution.job.queue_name, execution.job.priority ] }.sort_by(&:last)
    assert_equal 3, SolidQueue::ReadyExecution.aggregated_count_across("*")
  end

  def test_claiming_with_an_empty_range_claims_nothing
    enqueue_with_priorities "default", 0, 5

    assert_empty SolidQueue::ReadyExecution.claim("*", 10, claiming_process_id, priority: 5..1)
    assert_equal 2, SolidQueue::ReadyExecution.aggregated_count_across("*")
  end

  def test_claiming_without_a_range_takes_every_priority
    enqueue_with_priorities "default", 0, 5, 50

    assert_equal [ 0, 5, 50 ], claimed_priorities(SolidQueue::ReadyExecution.claim("*", 10, claiming_process_id, priority: nil))
  end

  def test_counting_ready_jobs_within_a_range
    enqueue_with_priorities "default", 0, 5, 50
    enqueue_with_priorities "other", 5

    assert_equal 4, SolidQueue::ReadyExecution.aggregated_count_across("*")
    assert_equal 2, SolidQueue::ReadyExecution.aggregated_count_across("*", priority: 1..10)
    assert_equal 1, SolidQueue::ReadyExecution.aggregated_count_across([ "default" ], priority: 1..10)
    assert_equal 1, SolidQueue::ReadyExecution.aggregated_count_across([ "oth*" ], priority: 5..)
    assert_equal 1, SolidQueue::ReadyExecution.aggregated_count_across("*", priority: ..0)
  end

  private
    def enqueue_with_priorities(queue_name, *priorities)
      priorities.each { |priority| SharedWorkOffJob.set(queue: queue_name, priority: priority).perform_later("#{queue_name}-#{priority}") }
    end

    def claimed_priorities(claimed)
      claimed.map { |execution| execution.job.priority }.sort
    end
end
