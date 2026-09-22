# frozen_string_literal: true

require_relative "test_helper"

class MongoNativeLimitedJob < ActiveJob::Base
  limits_concurrency key: ->(key) { key }, to: 2, duration: 5.minutes

  def perform(*)
  end
end

class MongoNativeSingleSlotJob < ActiveJob::Base
  limits_concurrency key: ->(key) { key }, to: 1, duration: 5.minutes

  def perform(*)
  end
end

class MongoNativeDiscardingJob < ActiveJob::Base
  limits_concurrency key: ->(key) { key }, to: 1, duration: 5.minutes, on_conflict: :discard

  def perform(*)
  end
end

class MongoNativeConcurrencyTest < MongoTestCase
  test "concurrent enqueue never grants more semaphore slots than the limit" do
    concurrently(12) { MongoNativeLimitedJob.perform_later("shared") }

    assert_equal 2, job_count(state: "ready")
    assert_equal 10, job_count(state: "blocked")

    semaphore = SolidQueue::Semaphore.find_by(key: concurrency_key(MongoNativeLimitedJob, "shared"))
    assert_equal 0, semaphore.value
  end

  test "finishing a claimed job releases exactly one blocked job" do
    3.times { MongoNativeLimitedJob.perform_later("release") }
    assert_equal 2, job_count(state: "ready")
    assert_equal 1, job_count(state: "blocked")

    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)
    claim.perform

    assert_equal 1, job_count(state: "finished")
    assert_equal 2, job_count(state: "ready")
    assert_equal 0, job_count(state: "blocked")
    assert_equal 0, SolidQueue::Semaphore.find_by(key: concurrency_key(MongoNativeLimitedJob, "release")).value
  end

  test "destroying an unclaimed ready job releases its semaphore exactly once" do
    previous = SolidQueue.preserve_finished_jobs
    SolidQueue.preserve_finished_jobs = false
    first, second, third = 3.times.map { MongoNativeSingleSlotJob.perform_later("destroy-ready") }
    first_job = SolidQueue::Job.find(first.provider_job_id)

    assert first_job.finished!
    assert_nil SolidQueue::Job.find_by_active_job_id(first.job_id)
    assert_equal 1, job_count(state: "ready")
    assert_equal 1, job_count(state: "blocked")
    assert_raises(SolidQueue::RecordNotFound) { first_job.finished! }
    assert_equal 1, job_count(state: "ready")
    assert_equal 1, job_count(state: "blocked")

    SolidQueue::Job.find(second.provider_job_id).finished!
    assert_equal 1, job_count(state: "ready")
    assert_equal 0, job_count(state: "blocked")
    assert_equal third.provider_job_id, SolidQueue::Mongo.collection(:jobs).find(state: "ready").first.fetch("_id").to_s
  ensure
    SolidQueue.preserve_finished_jobs = previous
  end

  test "discard conflict removes the rejected attempt without consuming another slot" do
    accepted = MongoNativeDiscardingJob.perform_later("discard")
    rejected = MongoNativeDiscardingJob.perform_later("discard")

    assert SolidQueue::Job.find(accepted.provider_job_id).ready?
    assert_nil rejected.provider_job_id
    assert_equal 1, SolidQueue::Job.count
    assert_equal 0, SolidQueue::Semaphore.find_by(key: concurrency_key(MongoNativeDiscardingJob, "discard")).value
  end

  test "concurrent expiry maintenance promotes only one waiter for an expired slot" do
    5.times { MongoNativeSingleSlotJob.perform_later("expired") }
    key = concurrency_key(MongoNativeSingleSlotJob, "expired")
    assert_equal 1, job_count(state: "ready")
    assert_equal 4, job_count(state: "blocked")

    SolidQueue::Mongo.collection(:semaphores).update_one(
      { key: key },
      { "$set" => { expires_at: 1.minute.ago } }
    )
    SolidQueue::Mongo.collection(:jobs).update_many(
      { state: "blocked", concurrency_key: key },
      { "$set" => { expires_at: 1.minute.ago } }
    )

    concurrently(8) do
      SolidQueue::Semaphore.expire(batch_size: 10)
      SolidQueue::BlockedExecution.unblock(10)
    end
    SolidQueue::BlockedExecution.unblock(10)

    assert_equal 2, job_count(state: "ready")
    assert_equal 3, job_count(state: "blocked")
    assert_equal 0, SolidQueue::Semaphore.find_by(key: key).value
  end

  private
    def job_count(state:)
      SolidQueue::Mongo.collection(:jobs).count_documents(state: state)
    end

    def concurrency_key(job_class, argument)
      job_class.new(argument).concurrency_key
    end
end
