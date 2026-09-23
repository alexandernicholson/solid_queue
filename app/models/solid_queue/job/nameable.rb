# frozen_string_literal: true

module SolidQueue
  class Job
    module Nameable
      def display_name
        @display_name ||= custom_display_name || class_name
      end

      private
        def custom_display_name
          return unless job_class.is_a?(Class) && job_class.method_defined?(:display_name)

          active_job = ActiveJob::Base.deserialize(arguments)
          active_job.arguments = ActiveJob::Arguments.deserialize(arguments.fetch("arguments", []))
          active_job.display_name.presence&.to_s
        rescue StandardError
          nil
        end
    end
  end
end
