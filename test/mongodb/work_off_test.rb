# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/work_off_jobs"
require_relative "../shared/work_off_behaviour"

class MongoWorkOffTest < MongoTestCase
  include WorkOffBehaviour

  private
    def job_status(active_job)
      SolidQueue::Job.find(active_job.provider_job_id).status
    end
end
