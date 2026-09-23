# frozen_string_literal: true

module ActiveJob
  module RunTimeLimit
    extend ActiveSupport::Concern

    included do
      class_attribute :run_time_limit, instance_accessor: false
    end

    class_methods do
      def limits_run_time(max:)
        raise ArgumentError, "max must be a positive duration, got #{max.inspect}" unless max.is_a?(Numeric) && max.positive?

        self.run_time_limit = max
      end
    end
  end
end
