# frozen_string_literal: true

module SolidQueue
  class Job
    module Deliverable
      extend ActiveSupport::Concern

      class_methods do
        def delivery_modes_migrated?
          column_names.include?("delivery_mode")
        end

        def delivery_mode_from(active_job)
          ActiveJob::DeliveryModes.mode!(active_job.try(:delivery_mode) || SolidQueue.default_delivery_mode).to_s
        end
      end

      def delivery_mode
        stored = self[:delivery_mode] if has_attribute?(:delivery_mode)
        (stored.presence || job_class.try(:delivery_mode) || SolidQueue.default_delivery_mode).to_sym
      end

      def exactly_once?
        delivery_mode == :exactly_once
      end

      def at_most_once?
        delivery_mode == :at_most_once
      end
    end
  end
end
