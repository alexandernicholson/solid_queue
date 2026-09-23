# frozen_string_literal: true

module DeduplicationBehaviour
  extend ActiveSupport::Concern

  included do
    setup do
      SharedDeduplicatedJob.performed = []
      SharedRetriedDeduplicatedJob.attempts = 0
    end
  end

  def test_a_duplicate_of_a_pending_job_is_not_enqueued_and_reports_the_job_holding_its_key
    first = SharedDeduplicatedJob.perform_later("order-1")
    duplicate = SharedDeduplicatedJob.new("order-1")
    events = capture_deduplication_events { assert_equal false, duplicate.enqueue }

    assert first.successfully_enqueued?
    assert_not duplicate.successfully_enqueued?
    assert_instance_of SolidQueue::Job::DuplicateError, duplicate.enqueue_error
    assert_nil duplicate.provider_job_id
    assert SharedDeduplicatedJob.perform_later("order-2").successfully_enqueued?
    assert_equal 2, job_count(SharedDeduplicatedJob)
    assert_equal 1, events.size
    assert_equal first.provider_job_id.to_s, events.first.payload[:job_id].to_s
    assert_equal first.deduplication_key, events.first.payload[:deduplication_key]
  end

  def test_concurrent_enqueues_of_one_key_persist_exactly_one_job
    run_concurrently(8) { SharedDeduplicatedJob.perform_later("race") }

    assert_equal 1, job_count(SharedDeduplicatedJob)
  end

  def test_a_duplicate_does_not_join_a_batch
    SharedDeduplicatedJob.perform_later("batched")
    batch = SolidQueue::Batch.enqueue { SharedDeduplicatedJob.perform_later("batched") }

    assert_equal 0, SolidQueue::Batch.find_by(id: batch.id).total_jobs
  end

  def test_the_key_frees_when_the_job_finishes_or_after_its_window
    SharedDeduplicatedJob.perform_later("done")
    SharedWindowedDeduplicatedJob.perform_later("done")
    drain_ready_jobs

    assert SharedDeduplicatedJob.perform_later("done")
    assert_not SharedWindowedDeduplicatedJob.perform_later("done")

    expire_deduplication_key(SharedWindowedDeduplicatedJob.new("done").deduplication_key)
    assert SharedWindowedDeduplicatedJob.perform_later("done")
  end

  def test_automatic_retries_keep_the_key_and_each_attempt_runs_once
    SharedRetriedDeduplicatedJob.perform_later("retried")
    claim_and_perform

    assert_not SharedRetriedDeduplicatedJob.perform_later("retried")

    drain_ready_jobs

    assert_equal 2, SharedRetriedDeduplicatedJob.attempts
    assert SharedRetriedDeduplicatedJob.perform_later("retried")
  end

  def test_a_failed_job_holds_its_key_until_it_is_discarded
    active_job = SharedFailingDeduplicatedJob.perform_later("failed")
    assert_raises(RuntimeError) { claim_and_perform }

    assert_not SharedFailingDeduplicatedJob.perform_later("failed")

    SolidQueue::Job.find(active_job.provider_job_id).discard
    assert SharedFailingDeduplicatedJob.perform_later("failed")
  end

  def test_bulk_enqueue_keeps_one_job_per_key
    active_jobs = [ SharedDeduplicatedJob.new("bulk"), SharedDeduplicatedJob.new("bulk"), SharedDeduplicatedJob.new("other") ]

    assert_equal 2, SolidQueue::Job.enqueue_all(active_jobs)
    assert_equal [ true, false, true ], active_jobs.map(&:successfully_enqueued?)
    assert_instance_of SolidQueue::Job::DuplicateError, active_jobs.second.enqueue_error
  end

  def test_a_job_discarded_by_its_concurrency_limit_releases_its_key
    SharedLimitedDeduplicatedJob.perform_later("holder")
    discarded = SharedLimitedDeduplicatedJob.perform_later("discarded")

    assert_nil discarded.provider_job_id
    assert SharedLimitedDeduplicatedJob.perform_later("discarded").provider_job_id.nil?
    assert_equal 0, deduplication_count(discarded.deduplication_key)
  end

  def test_a_deduplicated_job_that_started_is_not_released_for_a_second_run
    process = SolidQueue::Process.register(kind: "Worker", name: "deduplication-worker", pid: ::Process.pid, hostname: "test")
    active_job = SharedInterruptedDeduplicatedJob.perform_later(process.id.to_s)

    SolidQueue::ReadyExecution.claim([ "default" ], 1, process.id).first.perform

    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_empty SolidQueue::ReadyExecution.claim([ "default" ], 1, other_process_id)
  ensure
    process&.deregister
  end

  def test_a_released_deduplicated_claim_does_not_run
    active_job = SharedDeduplicatedJob.perform_later("released", "stale owner")
    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, other_process_id).first
    claim.release

    claim.perform

    assert_empty SharedDeduplicatedJob.performed
    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
  end

  private
    def claim_and_perform
      SolidQueue::ReadyExecution.claim([ "default" ], 1, other_process_id).each(&:perform)
    end

    def drain_ready_jobs
      10.times do
        claims = SolidQueue::ReadyExecution.claim([ "*" ], 10, other_process_id)
        return if claims.empty?

        claims.each(&:perform)
      end
      flunk "ready jobs did not drain"
    end

    def run_concurrently(count)
      ready = Queue.new
      start = Queue.new
      errors = Queue.new
      threads = count.times.map do
        Thread.new do
          ready << true
          start.pop
          yield
        rescue Exception => error
          errors << error
        end
      end
      count.times { ready.pop }
      count.times { start << true }
      threads.each(&:join)
      raise errors.pop unless errors.empty?
    end

    def capture_deduplication_events
      events = []
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      ActiveSupport::Notifications.subscribed(subscriber, "enqueue_duplicate.solid_queue") { yield }
      events
    end
end
