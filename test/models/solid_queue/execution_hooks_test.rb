# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/execution_hooks_jobs"
require_relative "../../shared/execution_hooks_behaviour"

class SolidQueue::ExecutionHooksTest < ActiveSupport::TestCase
  include ExecutionHooksBehaviour

  self.use_transactional_tests = false
end
