# frozen_string_literal: true

require_relative "test_helper"
require "mocha/minitest"
require_relative "../shared/run_time_jobs"
require_relative "../shared/run_time_behaviour"

class MongoRunTimeTest < MongoTestCase
  include RunTimeBehaviour

  test "a document that leaves the claimed state carries no timeout" do
    active_job = SharedShortRunTimeJob.perform_later(5)
    assert_raises(SolidQueue::Processes::RunTimeExceededError) { claim_and_perform }
    job_id = SolidQueue::Mongo.id!(active_job.provider_job_id)

    assert_nil jobs.find(_id: job_id).first["timeout_at"]

    jobs.update_one({ _id: job_id }, { "$set" => { timeout_at: 1.hour.ago } })
    SolidQueue::Job.find(job_id).dispatch_bypassing_concurrency_limits

    document = jobs.find(_id: job_id).first
    assert_equal "ready", document["state"]
    assert_nil document["timeout_at"]
  end

  private
    def jobs
      SolidQueue::Mongo.collection(:jobs)
    end

    def claim_timestamps(job_id)
      document = jobs.find(_id: SolidQueue::Mongo.id!(job_id), state: "claimed").first
      [ document["started_at"], document["timeout_at"] ]
    end
end
