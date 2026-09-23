# frozen_string_literal: true

module SolidQueue
  module DeathRecovery
    extend AppExecutor

    ERROR_CLASSES = [ Processes::ProcessPrunedError, Processes::ProcessExitError, Processes::ProcessMissingError ].freeze

    class << self
      def recover(job_ids, error)
        return if job_ids.empty? || !enabled? || !recoverable?(error)

        SolidQueue.instrument(:death_recovery, job_ids: job_ids, error: error, retried: [], exhausted: []) do |payload|
          job_ids.each do |job_id|
            case recover_job(job_id)
            when :retried then payload[:retried] << job_id
            when :exhausted then payload[:exhausted] << job_id
            end
          end
        end
      end

      def settings_from(settings)
        attempts = settings.to_h.with_indifferent_access[:attempts] if settings.respond_to?(:to_h)
        raise ArgumentError, "retry_on_process_death must be nil or { attempts: N } with N a positive integer, got #{settings.inspect}" unless attempts.is_a?(Integer) && attempts.positive?

        { attempts: attempts }
      end

      def attempts
        SolidQueue.retry_on_process_death&.fetch(:attempts)
      end

      def enabled?
        attempts.present?
      end

      private
        def recoverable?(error)
          ERROR_CLASSES.any? { |error_class| error.is_a?(error_class) }
        end

        def recover_job(job_id)
          failed_execution = Job.find_by(id: job_id)&.failed_execution
          return unless failed_execution && ERROR_CLASSES.map(&:name).include?(failed_execution.exception_class)

          if executions_counting_interruption(failed_execution.job) < attempts
            :retried if failed_execution.retry(interrupted: true)
          else
            :exhausted
          end
        rescue StandardError => error
          handle_thread_error(error)
          nil
        end

        def executions_counting_interruption(job)
          job.arguments.fetch("executions", 0).to_i + 1
        end
    end
  end
end
