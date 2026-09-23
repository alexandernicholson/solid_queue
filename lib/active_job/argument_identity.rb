# frozen_string_literal: true

module ActiveJob
  module ArgumentIdentity
    private
      def argument_identity(value)
        active_record_model?(value) ? [ value.class.name, value.id ] : [ value ]
      end

      def active_record_model?(value)
        defined?(::ActiveRecord::Base) && value.is_a?(::ActiveRecord::Base)
      end
  end
end
