# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/death_recovery_jobs"
require_relative "../shared/death_recovery_behaviour"

class MongoDeathRecoveryTest < MongoTestCase
  include DeathRecoveryBehaviour

  private
    def unregistered_process_id
      BSON::ObjectId.new
    end
end
