# frozen_string_literal: true

require "timeout"

module SolidQueue
  module Processes
    class RunTimeExceededError < Timeout::Error
      def self.for(max_run_time)
        new("Job exceeded its maximum run time of #{max_run_time.inspect}")
      end
    end
  end
end
