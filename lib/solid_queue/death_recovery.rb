# frozen_string_literal: true

module SolidQueue
  module DeathRecovery
    extend AppExecutor

    ERROR_CLASSES = [ Processes::ProcessPrunedError, Processes::ProcessExitError, Processes::ProcessMissingError ].freeze
    UNCOMMITTED_ATTEMPTS = 3

    class << self
      def recover(job_ids, error)
        return if job_ids.empty? || !recoverable?(error)

        capped = job_ids.filter_map { |job_id| capped_job(job_id) }
        return if capped.empty?

        SolidQueue.instrument(:death_recovery, job_ids: capped.map(&:first), error: error, retried: [], exhausted: []) do |payload|
          capped.each do |job_id, job, attempts|
            case recover_job(job, attempts)
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

      def attempts_for(job)
        job.job_class.try(:process_death_attempts) || attempts
      end

      def uncommitted_exhausted?(job)
        executions_counting_interruption(job) >= (attempts_for(job) || UNCOMMITTED_ATTEMPTS)
      end

      private
        def recoverable?(error)
          ERROR_CLASSES.any? { |error_class| error.is_a?(error_class) }
        end

        def capped_job(job_id)
          job = Job.find_by(id: job_id)
          attempts = job && attempts_for(job)
          [ job_id, job, attempts ] if attempts
        rescue StandardError => error
          handle_thread_error(error)
          nil
        end

        def recover_job(job, attempts)
          failed_execution = job.failed_execution
          return unless failed_execution && ERROR_CLASSES.map(&:name).include?(failed_execution.exception_class)

          if executions_counting_interruption(job) < attempts
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
