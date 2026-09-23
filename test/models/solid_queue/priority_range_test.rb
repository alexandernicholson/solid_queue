# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/work_off_jobs"
require_relative "../../shared/priority_range_behaviour"

class SolidQueue::PriorityRangeTest < ActiveSupport::TestCase
  include PriorityRangeBehaviour

  private
    def claiming_process_id
      SolidQueue::Process.register(kind: "Worker", name: "priority-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end
end
