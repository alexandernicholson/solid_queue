# frozen_string_literal: true

require_relative "test_helper"

class MongoNativeLifecycleJob < ActiveJob::Base
  class_attribute :performed_values, default: []

  def perform(value)
    self.class.performed_values += [ value ]
  end
end

class MongoNativeFailureJob < ActiveJob::Base
  class Error < StandardError; end

  class_attribute :fail, default: true

  def perform(value)
    raise Error, value if self.class.fail

    MongoNativeLifecycleJob.performed_values += [ value ]
  end
end

class MongoNativeOversizedLimitedJob < ActiveJob::Base
  limits_concurrency key: ->(*) { "oversized" }, to: 1, duration: 5.minutes

  def perform(*)
  end
end

class MongoNativeHugeFailureJob < ActiveJob::Base
  class Error < StandardError; end

  def perform
    raise Error, "x" * (SolidQueue::Job::BSON_MAX_DOCUMENT_BYTES + 1)
  end
end

class MongoNativeLifecycleTest < MongoTestCase
  def setup
    super
    MongoNativeLifecycleJob.performed_values = []
    MongoNativeFailureJob.fail = true
  end

  test "single and bulk jobs retain payloads and finish through a real claim" do
    single = MongoNativeLifecycleJob.perform_later("single")
    bulk_jobs = [ MongoNativeLifecycleJob.new("bulk-1"), MongoNativeLifecycleJob.new("bulk-2") ]

    assert_equal 2, SolidQueue::Job.enqueue_all(bulk_jobs)
    assert single.successfully_enqueued?
    assert bulk_jobs.all?(&:successfully_enqueued?)

    claims = SolidQueue::ReadyExecution.claim([ "default" ], 10, BSON::ObjectId.new)
    assert_equal 3, claims.size
    claims.each(&:perform)

    assert_equal %w[bulk-1 bulk-2 single], MongoNativeLifecycleJob.performed_values.sort
    assert_equal 3, SolidQueue::Mongo.collection(:jobs).count_documents(state: "finished")
  end

  test "a future job remains scheduled until the scheduler dispatches it" do
    active_job = MongoNativeLifecycleJob.new("scheduled")
    job = SolidQueue::Job.enqueue(active_job, scheduled_at: 1.hour.from_now)

    assert_equal "scheduled", job.state
    assert_empty SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new)

    SolidQueue::Mongo.collection(:jobs).update_one(
      { _id: job.bson_id },
      { "$set" => { scheduled_at: 1.second.ago } }
    )
    assert_equal 1, SolidQueue::ScheduledExecution.dispatch_next_batch(10)

    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)
    claim.perform

    assert_equal [ "scheduled" ], MongoNativeLifecycleJob.performed_values
    assert_equal "finished", SolidQueue::Job.find(job.id).state
  end

  test "failed jobs can be retried or discarded without leaving stale state" do
    retry_job = MongoNativeFailureJob.perform_later("retry")
    discard_job = MongoNativeFailureJob.perform_later("discard")

    claims = SolidQueue::ReadyExecution.claim([ "default" ], 10, BSON::ObjectId.new)
    assert_equal 2, claims.size
    claims.each { |claim| assert_raises(MongoNativeFailureJob::Error) { claim.perform } }

    retry_execution = SolidQueue::FailedExecution.find_failed(retry_job.provider_job_id)
    discard_execution = SolidQueue::FailedExecution.find_failed(discard_job.provider_job_id)
    assert retry_execution.retry
    assert discard_execution.discard

    MongoNativeFailureJob.fail = false
    SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0).perform

    assert_equal [ "retry" ], MongoNativeLifecycleJob.performed_values
    assert_equal "finished", SolidQueue::Job.find(retry_job.provider_job_id).state
    assert_nil SolidQueue::Job.find_by(_id: discard_execution.bson_id)
  end

  test "Active Job integers beyond BSON int64 survive enqueue, failure, and retry" do
    value = 1 << 80
    active_job = MongoNativeFailureJob.perform_later(value)
    first_claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)

    assert_raises(MongoNativeFailureJob::Error) { first_claim.perform }
    assert SolidQueue::FailedExecution.find_failed(active_job.provider_job_id).retry

    MongoNativeFailureJob.fail = false
    SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0).perform

    assert_equal [ value ], MongoNativeLifecycleJob.performed_values
    assert_equal "finished", SolidQueue::Job.find(active_job.provider_job_id).state
  end

  test "oversized constrained batch enqueue rolls back job marker and semaphore" do
    active_job = MongoNativeOversizedLimitedJob.new(
      "x" * SolidQueue::Job::BSON_MAX_DOCUMENT_BYTES
    )

    assert_raises(SolidQueue::Job::EnqueueError) do
      SolidQueue::Batch.enqueue do
        SolidQueue::Job.enqueue(active_job)
      end
    end

    assert_not active_job.successfully_enqueued?
    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::Batch.count
    assert_equal 0, SolidQueue::Mongo.collection(:batch_executions).count_documents
    assert_equal 0, SolidQueue::Mongo.collection(:semaphores).count_documents
  end

  test "very large exception is bounded and finalizes the claim as failed" do
    active_job = MongoNativeHugeFailureJob.perform_later
    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)

    assert_raises(MongoNativeHugeFailureJob::Error) { claim.perform }

    document = SolidQueue::Job.collection.find(_id: SolidQueue::Mongo.id!(active_job.provider_job_id)).first
    assert_equal "failed", document.fetch("state")
    assert_operator document.dig("error", "message").bytesize, :<=, SolidQueue::FailedExecution::MAX_MESSAGE_BYTES
    assert_operator document.dig("error", "exception_class").bytesize, :<=, SolidQueue::FailedExecution::MAX_EXCEPTION_CLASS_BYTES
    assert_operator document.dig("error", "backtrace").size, :<=, SolidQueue::FailedExecution::MAX_BACKTRACE_LINES
    assert_operator BSON::Document.new(document.fetch("error")).to_bson.to_s.bytesize,
      :<=, SolidQueue::FailedExecution::MAX_ERROR_BYTES
    assert_operator BSON::Document.new(document).to_bson.to_s.bytesize, :<=, SolidQueue::Job::BSON_MAX_DOCUMENT_BYTES
  end

  test "execution class queries only see jobs in their own state" do
    ready = MongoNativeLifecycleJob.perform_later("ready")
    MongoNativeLifecycleJob.set(wait: 1.hour).perform_later("scheduled")

    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::FailedExecution.count
    assert_equal 0, SolidQueue::ClaimedExecution.count
    assert_nil SolidQueue::FailedExecution.find_by(active_job_id: ready.job_id)
    assert_raises(SolidQueue::RecordNotFound) { SolidQueue::FailedExecution.find(ready.provider_job_id) }
    assert_equal ready.provider_job_id, SolidQueue::ReadyExecution.find(ready.provider_job_id).id

    assert_equal 0, SolidQueue::FailedExecution.delete_all
    assert_equal 1, SolidQueue::ReadyExecution.delete_all
    assert_equal 1, SolidQueue::Job.count
  end

  test "queue order, escaped prefixes, and pauses constrain claims" do
    jobs = [
      build_job("critical-low", queue: "critical.foo", priority: 5),
      build_job("critical-high", queue: "critical.foo", priority: 1),
      build_job("not-a-prefix-match", queue: "criticalXfoo", priority: 0),
      build_job("default", queue: "default", priority: 0)
    ]
    SolidQueue::Job.enqueue_all(jobs)

    SolidQueue::Queue.find_by_name("critical.foo").pause
    paused_claims = SolidQueue::ReadyExecution.claim([ "critical.*", "default" ], 10, BSON::ObjectId.new)
    assert_equal [ "default" ], paused_claims.map { |claim| claim.job.arguments.fetch("arguments").first }

    SolidQueue::Queue.find_by_name("critical.foo").resume
    default_later = build_job("default-later", queue: "default", priority: 0)
    SolidQueue::Job.enqueue_all([ default_later ])
    resumed_claims = SolidQueue::ReadyExecution.claim([ "critical.*", "default" ], 2, BSON::ObjectId.new)
    assert_equal %w[critical-high critical-low], resumed_claims.map { |claim| claim.job.arguments.fetch("arguments").first }

    remaining = SolidQueue::ReadyExecution.claim([ "*" ], 10, BSON::ObjectId.new)
    assert_equal %w[not-a-prefix-match default-later], remaining.map { |claim| claim.job.arguments.fetch("arguments").first }
  end

  private
    def build_job(value, queue:, priority:)
      MongoNativeLifecycleJob.new(value).tap do |job|
        job.queue_name = queue
        job.priority = priority
      end
    end
end
