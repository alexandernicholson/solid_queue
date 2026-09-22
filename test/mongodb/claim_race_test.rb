# frozen_string_literal: true

require_relative "test_helper"

class MongoClaimRaceTest < MongoTestCase
  JOB_COUNT = 200
  CONSUMER_COUNT = 8

  def test_concurrent_consumers_claim_every_ready_attempt_at_most_once
    active_jobs = JOB_COUNT.times.map { |number| MongoRaceJob.new(number) }
    assert_equal JOB_COUNT, SolidQueue::Job.enqueue_all(active_jobs)

    claims = Queue.new
    concurrently(CONSUMER_COUNT) do
      SolidQueue::ReadyExecution.claim("*", JOB_COUNT, BSON::ObjectId.new.to_s).each do |claim|
        claims << claim.job_id
      end
    end

    claimed_job_ids = []
    claimed_job_ids << claims.pop until claims.empty?

    assert_equal JOB_COUNT, claimed_job_ids.size
    assert_equal JOB_COUNT, claimed_job_ids.uniq.size
    assert_equal JOB_COUNT, SolidQueue::Mongo.collection(:jobs).count_documents(state: "claimed")
    assert_equal 0, SolidQueue::Mongo.collection(:jobs).count_documents(state: "ready")
  end
end
