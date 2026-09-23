# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/work_off_jobs"
require_relative "../../shared/drain_behaviour"

class SolidQueue::DrainTest < ActiveSupport::TestCase
  include DrainBehaviour

  self.use_transactional_tests = false

  private
    def registered_process_count
      skip_active_record_query_cache { SolidQueue::Process.count }
    end

    def job_status(active_job)
      skip_active_record_query_cache { SolidQueue::Job.find(active_job.provider_job_id).status }
    end
end
