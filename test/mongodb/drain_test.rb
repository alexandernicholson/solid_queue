# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/work_off_jobs"
require_relative "../shared/drain_behaviour"

class MongoDrainTest < MongoTestCase
  include DrainBehaviour

  private
    def registered_process_count
      SolidQueue::Mongo.collection(:processes).count_documents({})
    end

    def job_status(active_job)
      SolidQueue::Job.find(active_job.provider_job_id).status
    end
end
