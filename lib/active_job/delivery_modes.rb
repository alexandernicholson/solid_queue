# frozen_string_literal: true

module ActiveJob
  module DeliveryModes
    extend ActiveSupport::Concern

    MODES = %i[ at_least_once at_most_once exactly_once ].freeze
    EXECUTION_KEY = :solid_queue_exactly_once_execution

    included do
      class_attribute :delivery_mode, instance_accessor: false, instance_predicate: false
      class_attribute :process_death_attempts, instance_accessor: false, instance_predicate: false

      around_perform(prepend: true) { |_job, block| DeliveryModes.within_attempt(&block) }
    end

    class << self
      def mode!(mode)
        unless mode.respond_to?(:to_sym) && MODES.include?(mode.to_sym)
          raise ArgumentError, "delivery mode must be one of #{MODES.map(&:inspect).join(", ")}, got #{mode.inspect}"
        end

        mode.to_sym
      end

      def performing(execution)
        previous = current_execution
        ActiveSupport::IsolatedExecutionState[EXECUTION_KEY] = execution
        yield
      ensure
        ActiveSupport::IsolatedExecutionState[EXECUTION_KEY] = previous
      end

      def current_execution
        ActiveSupport::IsolatedExecutionState[EXECUTION_KEY]
      end

      def within_attempt(&block)
        current_execution ? current_execution.within_attempt(&block) : yield
      end
    end

    class_methods do
      def delivers(mode)
        self.delivery_mode = DeliveryModes.mode!(mode)
      end

      def retries_on_process_death(attempts:)
        raise ArgumentError, "attempts must be a positive integer, got #{attempts.inspect}" unless attempts.is_a?(Integer) && attempts.positive?

        self.process_death_attempts = attempts
      end
    end

    def delivery_mode
      self.class.delivery_mode || SolidQueue.default_delivery_mode
    end
  end
end
