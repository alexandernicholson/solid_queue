# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/execution_hooks_jobs"
require_relative "../shared/execution_hooks_behaviour"

class MongoExecutionHooksTest < MongoTestCase
  include ExecutionHooksBehaviour
end
