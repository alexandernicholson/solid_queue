# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/run_time_jobs"
require_relative "../../shared/run_time_behaviour"

class SolidQueue::RunTimeTest < ActiveSupport::TestCase
  include RunTimeBehaviour

  self.use_transactional_tests = false

  private
    def claim_timestamps(job_id)
      SolidQueue::ClaimedExecution.find_by!(job_id: job_id).then { |claim| [ claim.started_at, claim.timeout_at ] }
    end
end
