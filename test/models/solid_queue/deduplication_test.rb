# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/deduplication_jobs"
require_relative "../../shared/deduplication_behaviour"

class SolidQueue::DeduplicationTest < ActiveSupport::TestCase
  include DeduplicationBehaviour

  self.use_transactional_tests = false

  private
    def job_count(job_class)
      SolidQueue::Job.where(class_name: job_class.name).count
    end

    def deduplication_count(key)
      SolidQueue::Deduplication.where(key: key).count
    end

    def expire_deduplication_key(key)
      SolidQueue::Deduplication.where(key: key).update_all(expires_at: 1.minute.ago)
    end

    def other_process_id
      SolidQueue::Process.register(kind: "Worker", name: "other-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end
end
