# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/work_off_jobs"
require_relative "../../shared/work_off_behaviour"

class SolidQueue::WorkOffTest < ActiveSupport::TestCase
  include WorkOffBehaviour

  setup do
    SolidQueue.on_thread_error = silent_on_thread_error_for([ ExpectedTestError, SharedWorkOffError ], @_on_thread_error)
  end

  private
    def job_status(active_job)
      skip_active_record_query_cache { SolidQueue::Job.find(active_job.provider_job_id).status }
    end
end
