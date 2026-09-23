# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/work_off_jobs"
require_relative "../../shared/operations_behaviour"

class SolidQueue::OperationsTest < ActiveSupport::TestCase
  include OperationsBehaviour

  setup do
    SolidQueue.on_thread_error = silent_on_thread_error_for([ ExpectedTestError, SharedWorkOffError ], @_on_thread_error)
  end

  private
    def claiming_process_id
      SolidQueue::Process.register(kind: "Worker", name: "operations-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def job_status(active_job)
      skip_active_record_query_cache { SolidQueue::Job.find_by(id: active_job.provider_job_id)&.status }
    end
end
