# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/work_off_jobs"
require_relative "../shared/operations_behaviour"

class MongoOperationsTest < MongoTestCase
  include OperationsBehaviour

  private
    def claiming_process_id
      SolidQueue::Process.register(kind: "Worker", name: "operations-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def job_status(active_job)
      SolidQueue::Job.find(active_job.provider_job_id).status
    rescue SolidQueue::RecordNotFound
      nil
    end
end
