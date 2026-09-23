# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/work_off_jobs"
require_relative "../shared/priority_range_behaviour"

class MongoPriorityRangeTest < MongoTestCase
  include PriorityRangeBehaviour

  private
    def claiming_process_id
      BSON::ObjectId.new
    end
end
