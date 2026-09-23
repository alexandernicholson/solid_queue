# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/delivery_guarantees_jobs"
require_relative "../../shared/delivery_guarantees_behaviour"

class SolidQueue::DeliveryGuaranteesTest < ActiveSupport::TestCase
  include DeliveryGuaranteesBehaviour

  self.use_transactional_tests = false

  private
    def unregistered_process_id
      SolidQueue::Process.maximum(:id).to_i + 1_000
    end
end
