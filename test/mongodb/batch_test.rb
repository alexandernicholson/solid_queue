# frozen_string_literal: true

require_relative "test_helper"
require "mocha/minitest"

class MongoNativeBatchJob < ActiveJob::Base
  def perform(*)
  end
end

class MongoNativeRetryOnceJob < ActiveJob::Base
  class RetryableError < StandardError; end

  retry_on RetryableError, wait: 0, attempts: 2
  class_attribute :attempts, default: Hash.new(0)

  def perform(key)
    count = self.class.attempts.fetch(key, 0)
    self.class.attempts = self.class.attempts.merge(key => count + 1)
    raise RetryableError, key if count.zero?
  end
end

class MongoNativeBatchCallbackJob < ActiveJob::Base
  class EnqueueError < StandardError; end

  class_attribute :fail_enqueues, default: false
  before_enqueue { raise EnqueueError, "callback enqueue failed" if self.class.fail_enqueues }

  def perform
  end
end

class MongoNativeTerminalBatchJob < ActiveJob::Base
  class_attribute :fail_execution, default: true

  def perform
    raise "terminal batch failure" if self.class.fail_execution
  end
end

class MongoNativeBatchTest < MongoTestCase
  def setup
    super
    MongoNativeRetryOnceJob.attempts = Hash.new(0)
    MongoNativeBatchCallbackJob.fail_enqueues = false
  end

  test "checking a batch for outstanding attempts examines at most one indexed marker" do
    batch = SolidQueue::Batch.enqueue { 20.times { MongoNativeBatchJob.perform_later } }

    assert SolidQueue::BatchExecution.outstanding_for_batch?(batch.id)

    explained = SolidQueue::BatchExecution.outstanding_query(batch.id).explain
    assert_includes explained.dig("queryPlanner", "winningPlan").to_s, "batch_execution_attempts"
    assert_operator explained.dig("executionStats", "totalDocsExamined"), :<=, 1
  end

  test "manual retry of a finished failed batch does not reopen its accounting" do
    active_job = nil
    batch = SolidQueue::Batch.enqueue { active_job = MongoNativeTerminalBatchJob.perform_later }
    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)
    assert_raises(RuntimeError) { claim.perform }
    assert batch.reload.failed?
    finished_at = batch.finished_at

    MongoNativeTerminalBatchJob.fail_execution = false
    assert SolidQueue::FailedExecution.find_failed(active_job.provider_job_id).retry
    drain_ready_jobs

    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
    assert_equal finished_at, batch.reload.finished_at
    assert_equal 1, batch.failed_jobs
    assert_equal 0, batch.pending_jobs
  ensure
    MongoNativeTerminalBatchJob.fail_execution = true
  end

  test "automatic retry attempts count as one logical batch job" do
    batch = SolidQueue::Batch.enqueue do
      MongoNativeRetryOnceJob.perform_later("logical-job")
    end

    drain_ready_jobs
    batch.reload

    assert batch.succeeded?
    assert_equal 1, batch.total_jobs
    assert_equal 1, batch.completed_jobs
    assert_equal 0, batch.failed_jobs
    assert_equal 0, batch.pending_jobs
    assert_equal 2, batch.jobs.size
    assert_equal 1, batch.jobs.map(&:active_job_id).uniq.size
  end

  test "concurrent addition and final completion cannot finish a batch with pending work" do
    10.times do |iteration|
      original_active_job = nil
      batch = SolidQueue::Batch.enqueue do
        original_active_job = MongoNativeBatchJob.perform_later("original-#{iteration}")
      end
      original = SolidQueue::Job.find(original_active_job.provider_job_id)
      ready = Queue.new
      start = Queue.new

      add_thread = Thread.new do
        ready << true
        start.pop
        batch.enqueue do
          MongoNativeBatchJob.perform_later("added-#{iteration}")
        end
        :accepted
      rescue SolidQueue::Batch::AlreadyFinished
        :rejected
      end
      finish_thread = Thread.new do
        ready << true
        start.pop
        original.finished!
      end
      2.times { ready.pop }
      2.times { start << true }
      addition_result = add_thread.value
      finish_thread.value

      batch.reload
      if addition_result == :accepted
        assert_not batch.finished?, "accepted work must keep the batch open"
        assert_equal 1, batch.pending_jobs
        batch.jobs.find(&:ready?).finished!
        assert batch.reload.finished?
      else
        assert_equal :rejected, addition_result
        assert batch.finished?
        assert_equal 0, batch.pending_jobs
      end
    end
  end

  test "callback enqueue failure cannot roll back the terminal job and sweep retries completion" do
    MongoNativeBatchCallbackJob.fail_enqueues = true
    active_job = nil
    batch = SolidQueue::Batch.enqueue(on_finish: MongoNativeBatchCallbackJob) do
      active_job = MongoNativeBatchJob.perform_later("terminal")
    end
    terminal_job_id = active_job.provider_job_id

    SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0).perform

    assert_equal "finished", SolidQueue::Job.find(terminal_job_id).state
    assert_equal 0, batch.reload.pending_jobs
    assert_not batch.finished?, "callback enqueue and batch finish must roll back without undoing the job outcome"
    assert_equal 0, SolidQueue::Mongo.collection(:jobs).count_documents(class_name: "MongoNativeBatchCallbackJob")

    MongoNativeBatchCallbackJob.fail_enqueues = false
    SolidQueue::Batch.sweep_stalled(stalled_for: 0.seconds, batch_size: 10)

    assert batch.reload.finished?
    assert_equal 1, SolidQueue::Mongo.collection(:jobs).count_documents(
      class_name: "MongoNativeBatchCallbackJob",
      state: "ready"
    )
  end

  test "stale terminal markers are removed and their batch is finalized by sweep" do
    active_job = nil
    batch = SolidQueue::Batch.enqueue do
      active_job = MongoNativeBatchJob.perform_later("stale-marker")
    end
    job = SolidQueue::Job.find(active_job.provider_job_id)

    SolidQueue::Mongo.collection(:jobs).update_one(
      { _id: job.bson_id },
      { "$set" => { state: "finished", finished_at: Time.current } }
    )
    assert_equal 1, batch.reload.pending_jobs

    SolidQueue::Batch.sweep_stalled(stalled_for: 0.seconds, batch_size: 10)

    assert batch.reload.finished?
    assert_equal 0, batch.pending_jobs
    assert_equal 1, batch.completed_jobs
  end

  test "the stalled sweep finishes a batch queued behind more than a page of active batches" do
    3.times { SolidQueue::Batch.enqueue { MongoNativeBatchJob.perform_later } }
    stalled = SolidQueue::Batch.enqueue { MongoNativeBatchJob.perform_later("stalled") }
    SolidQueue::Mongo.collection(:batch_executions).delete_many(batch_id: stalled.bson_id, kind: "attempt")

    SolidQueue::Batch.sweep_stalled(stalled_for: 1.hour, batch_size: 2)

    assert stalled.reload.finished?
  end

  test "a committed batch stays persisted when starting it fails" do
    batch = SolidQueue::Batch.new
    SolidQueue::Batch.any_instance.stubs(:start).raises(::Mongo::Error::SocketError, "lost after commit")

    assert_raises(::Mongo::Error::SocketError) { batch.enqueue { MongoNativeBatchJob.perform_later } }

    assert batch.persisted?
    assert_equal 1, SolidQueue::Batch.collection.count_documents(_id: batch.bson_id)
  end

  test "retention removes completed batches and all logical accounting markers" do
    active_job = nil
    batch = SolidQueue::Batch.enqueue do
      active_job = MongoNativeBatchJob.perform_later("retained")
    end
    SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0).perform
    batch.reload

    assert batch.succeeded?
    assert_equal 1, batch.total_jobs
    assert_equal 1, batch.completed_jobs
    assert_equal 1, SolidQueue::Mongo.collection(:batch_executions).count_documents(
      batch_id: batch.bson_id,
      kind: "logical"
    )

    SolidQueue::Batch.collection.update_one(
      { _id: batch.bson_id },
      { "$set" => { finished_at: 2.hours.ago } }
    )
    SolidQueue::Batch.clear_finished_in_batches(batch_size: 1, finished_before: 1.hour.ago)

    assert_nil SolidQueue::Batch.find_by(_id: batch.bson_id)
    assert_equal 0, SolidQueue::Mongo.collection(:batch_executions).count_documents(batch_id: batch.bson_id)
    assert SolidQueue::Job.find(active_job.provider_job_id).finished?
  end

  test "batch state scopes do not treat missing terminal fields as present" do
    batch_id = BSON::ObjectId.new
    SolidQueue::Batch.collection.insert_one(
      _id: batch_id,
      active_job_batch_id: SecureRandom.uuid,
      created_at: Time.current,
      updated_at: Time.current
    )

    assert_equal [ batch_id ], SolidQueue::Batch.unfinished.map(&:bson_id)
    assert_empty SolidQueue::Batch.finished
    assert_empty SolidQueue::Batch.succeeded
    assert_empty SolidQueue::Batch.failed
    assert_empty SolidQueue::Batch.enqueued
  end

  private
    def drain_ready_jobs
      10.times do
        claims = SolidQueue::ReadyExecution.claim([ "*" ], 10, BSON::ObjectId.new)
        return if claims.empty?

        claims.each(&:perform)
      end
      flunk "ready jobs did not drain"
    end
end
