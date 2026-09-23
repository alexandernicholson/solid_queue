# frozen_string_literal: true

module SolidQueue
  class ClaimedExecution < Execution
    belongs_to :process

    scope :orphaned, -> { where.missing(:process) }
    scope :timed_out, -> { where(timeout_at: ..Time.current) }

    class Result < Struct.new(:success, :error)
      def success?
        success
      end
    end

    # Raised when a job has already run (or failed) but we couldn't update its
    # claim/finished state because of a transient error. The claim is still held
    # by a living worker, so it won't be recovered as orphaned unless the worker
    # is stopped and replaced.
    class FinalizationError < Processes::UnrecoverableError
      def initialize(claimed_execution, cause:)
        super("Failed to finalize claimed execution #{claimed_execution.id} (job #{claimed_execution.job_id}): #{cause.class}: #{cause.message}")
        set_backtrace(cause.backtrace) if cause.backtrace
      end
    end

    class << self
      def claiming(job_ids, process_id, &block)
        job_data = Array(job_ids).collect { |job_id| { job_id: job_id, process_id: process_id } }

        SolidQueue.instrument(:claim, process_id: process_id, job_ids: job_ids) do |payload|
          insert_all!(job_data)
          where(job_id: job_ids, process_id: process_id).load.tap do |claimed|
            block.call(claimed)

            payload[:size] = claimed.size
            payload[:claimed_job_ids] = claimed.map(&:job_id)
          end
        end
      end

      def release_for_process(process_id)
        where(process_id: process_id).release_all
      end

      def fail_for_process(process_id, error)
        where(process_id: process_id).fail_all_with(error)
      end

      def fail_orphaned(error)
        orphaned.fail_all_with(error)
      end

      def release_all
        SolidQueue.instrument(:release_many_claimed) do |payload|
          includes(:job).tap do |executions|
            executions.each(&:release)

            payload[:size] = executions.size
          end
        end
      end

      def fail_all_with(error)
        includes(:job).tap do |executions|
          return if executions.empty?

          failed_job_ids = []
          SolidQueue.instrument(:fail_many_claimed) do |payload|
            failed_job_ids = executions.select { |execution| execution.failed_with(error) }.map(&:job_id)

            payload[:process_ids] = executions.map(&:process_id).uniq
            payload[:job_ids] = executions.map(&:job_id).uniq
            payload[:display_names] = display_names_for(executions)
            payload[:size] = executions.size
            payload[:error] = error
          end
          DeathRecovery.recover(failed_job_ids, error)
        end
      end

      def fail_timed_out
        return 0 unless column_names.include?("timeout_at")

        timed_out.includes(:job).to_a.count { |execution| fail_timed_out_execution(execution) }
      end

      def discard_all_in_batches(*)
        raise UndiscardableError, "Can't discard jobs in progress"
      end

      def discard_all_from_jobs(*)
        raise UndiscardableError, "Can't discard jobs in progress"
      end

      def display_names_for(executions)
        executions.to_h { |execution| [ execution.job_id, execution.job.display_name ] }
      end

      private
        def fail_timed_out_execution(execution)
          max_run_time = execution.job.run_time_limit
          return false unless execution.failed_with(Processes::RunTimeExceededError.for(max_run_time))

          SolidQueue.instrument(:run_time_exceeded, job_id: execution.job_id, process_id: execution.process_id,
            max_run_time: max_run_time, started_at: execution.started_at, display_name: execution.job.display_name)
          true
        end
    end

    def perform
      performed = false
      ExecutionHooks.run(:around_perform, self) do
        performed = true
        perform_claimed
      end
      raise ExecutionHooks::NotPerformedError unless performed
    rescue Exception => error
      record_failure(error) unless performed
      raise
    end

    def release
      SolidQueue.instrument(:release_claimed, job_id: job.id, process_id: process_id, display_name: job.display_name) do
        unless_already_finalized do
          next false if started_at?

          job.dispatch_bypassing_concurrency_limits
          destroy!
        end
      end
    end

    def discard
      raise UndiscardableError, "Can't discard a job in progress"
    end

    def failed_with(error)
      finalize { job.failed_with(error) }
    end

    private
      def perform_claimed
        run_time_limit = job.run_time_limit
        return if (job.deduplicated? || run_time_limit) && !start(run_time_limit)

        result = execute(run_time_limit)

        if result.success?
          finalizing { finished }
        else
          record_failure(result.error)
          raise result.error
        end
      end

      def record_failure(error)
        ExecutionHooks.notify(:on_failure, self, error) if finalizing { failed_with(error) }
      end

      # A failure here means the job already ran but we couldn't record the
      # outcome, and the claim is still held by this living worker, where no
      # recovery can reach it: it's a process problem, not a job problem
      def finalizing
        yield
      rescue => error
        raise FinalizationError.new(self, cause: error) if still_claimed?

        raise
      end

      def start(run_time_limit)
        now = Time.current
        attributes = { started_at: now }
        attributes[:timeout_at] = now + run_time_limit + SolidQueue.run_time_grace if run_time_limit && has_attribute?(:timeout_at)

        self.class.where(id: id, started_at: nil).update_all(attributes) == 1
      end

      def execute(run_time_limit)
        raise Job::ClassMissingError.for(job) if job.job_class.nil?

        within_run_time_limit(run_time_limit) do
          ActiveJob::Base.execute(job.arguments.merge("provider_job_id" => job.id))
        end
        Result.new(true, nil)
      rescue Exception => e
        Result.new(false, e)
      end

      def within_run_time_limit(run_time_limit, &block)
        if run_time_limit
          Timeout.timeout(run_time_limit.to_f, Processes::RunTimeExceededError, Processes::RunTimeExceededError.for(run_time_limit).message, &block)
        else
          yield
        end
      end

      def finished
        finalize { job.finished! }
      end

      def finalize
        finalized = unless_already_finalized do
          yield
          destroy!
          true
        end

        # Unblock the next job outside the finalize transaction so a failure while
        # releasing the concurrency lock or dispatching the next job can't roll back
        # a job that already finished or failed. Only the actor that owned and
        # finalized the claim gets here, so the lock is released exactly once.
        job.unblock_next_blocked_job if finalized
        finalized
      end

      def unless_already_finalized
        transaction do
          return false unless self.class.unscoped.lock.find_by(id: id)

          yield
        end
      end

      def still_claimed?
        self.class.exists?(id)
      rescue
        # If we can't check because the DB is unavailable, assume the claim is
        # still held so the worker can be stopped and replaced.
        true
      end
  end
end
