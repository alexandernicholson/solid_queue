# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/death_recovery_jobs"
require_relative "../../shared/death_recovery_behaviour"

class SolidQueue::DeathRecoveryTest < ActiveSupport::TestCase
  include DeathRecoveryBehaviour

  self.use_transactional_tests = false

  private
    def unregistered_process_id
      SolidQueue::Process.maximum(:id).to_i + 1_000
    end
end
