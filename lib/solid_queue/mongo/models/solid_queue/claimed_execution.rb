# frozen_string_literal: true

module SolidQueue
  class ClaimedExecution < Execution
    class Result < Struct.new(:success, :error)
      def success? = success
    end

    class FinalizationError < Processes::UnrecoverableError
      def initialize(claimed_execution, cause:)
        super("Failed to finalize claimed execution #{claimed_execution.id} (job #{claimed_execution.job_id}): #{cause.class}: #{cause.message}")
        set_backtrace(cause.backtrace) if cause.backtrace
      end
    end

    class Uncommitted < StandardError
      attr_reader :error

      def initialize(error)
        @error = error
        super(error.message)
      end
    end

    class Conflict < StandardError; end

    RELEASE_UNCOMMITTED_MAX_TIME_MS = 250
    LOCK_CONFLICT_CODES = [ 50, 112 ].freeze

    class << self
      def claiming(job_ids, process_id, &block)
        SolidQueue.instrument(:claim, process_id: process_id.to_s, job_ids: Array(job_ids).map(&:to_s)) do |payload|
          ReadyExecution.claiming(job_ids, process_id).tap do |claimed|
            block.call(claimed) if block
            payload[:candidates] = Array(job_ids).size
            payload[:candidates_seen] = Array(job_ids).size
            payload[:size] = claimed.size
            payload[:claimed] = claimed.size
            payload[:claimed_job_ids] = claimed.map(&:job_id)
          end
        end
      end

      def release_for_process(process_id)
        release_all(claimed_for(process_id: SolidQueue::Mongo.id!(process_id)))
      end

      def fail_for_process(process_id, error)
        fail_all_with(error, claimed_for(process_id: SolidQueue::Mongo.id!(process_id)))
      end

      def fail_orphaned(error)
        fail_all_with(error, orphaned)
      end

      # Without relations to scope, callers pass the executions to act on;
      # the defaults cover every claimed execution, like the unscoped Active Record calls.
      def release_all(executions = claimed_for)
        SolidQueue.instrument(:release_many_claimed) do |payload|
          payload[:size] = executions.count(&:release)
        end
      end

      def fail_all_with(error, executions = claimed_for)
        return 0 if executions.empty?

        uncommitted, executions = executions.partition(&:uncommitted_exactly_once?)
        SolidQueue::Mongo.after_commit { release_all_uncommitted(uncommitted, error) } if uncommitted.any?
        return 0 if executions.empty?

        failed = []
        SolidQueue.instrument(:fail_many_claimed) do |payload|
          failed = executions.select { |execution| execution.failed_with(error) }
          payload[:process_ids] = executions.map(&:process_id).uniq
          payload[:job_ids] = executions.map(&:job_id).uniq
          payload[:display_names] = display_names_for(executions)
          payload[:size] = failed.size
          payload[:error] = error
        end
        failed_job_ids = failed.select(&:rerunnable?).map(&:job_id)
        SolidQueue::Mongo.after_commit { DeathRecovery.recover(failed_job_ids, error) }
        failed.size
      end

      def fail_timed_out
        collection.find(
          { state: "claimed", timeout_at: { "$lte" => Time.current } },
          hint: "claimed_timeout",
          **SolidQueue::Mongo.session_options
        ).map { |document| from_document(document) }.count { |execution| fail_timed_out_execution(execution) }
      end

      def display_names_for(executions)
        executions.to_h { |execution| [ execution.job_id, execution.display_name ] }
      end

      def discard_all_in_batches(*)
        raise UndiscardableError, "Can't discard jobs in progress"
      end

      def discard_all_from_jobs(*)
        raise UndiscardableError, "Can't discard jobs in progress"
      end

      private
        def claimed_for(extra = {})
          collection.find({ state: "claimed" }.merge(extra), **SolidQueue::Mongo.session_options).map { |doc| from_document(doc) }
        end

        def orphaned
          collection.aggregate([
            { "$match" => { state: "claimed" } },
            { "$lookup" => {
              from: SolidQueue::Mongo.collection(:processes).name,
              localField: "process_id", foreignField: "_id", as: "owner"
            } },
            { "$match" => { "owner.0" => { "$exists" => false } } },
            { "$project" => { owner: 0 } }
          ], read_concern: { level: :snapshot }, **SolidQueue::Mongo.session_options).map { |document| from_document(document) }
        end

        def release_all_uncommitted(executions, error)
          SolidQueue.instrument(:release_uncommitted, job_ids: executions.map(&:job_id), process_ids: executions.map(&:process_id).uniq,
            display_names: display_names_for(executions), error: error) do |payload|
            outcomes = executions.group_by { |execution| execution.release_uncommitted(error) }

            %i[ released exhausted locked ].each { |outcome| payload[outcome] = outcomes.fetch(outcome, []).map(&:job_id) }
            payload[:size] = payload[:released].size
          end
        end

        def fail_timed_out_execution(execution)
          max_run_time = execution.run_time_limit
          return false unless execution.failed_with(Processes::RunTimeExceededError.for(max_run_time))

          SolidQueue.instrument(:run_time_exceeded, job_id: execution.job_id, process_id: execution.process_id,
            max_run_time: max_run_time, started_at: execution.started_at, display_name: execution.display_name)
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
      released = false
      SolidQueue.instrument(:release_claimed, job_id: job_id, process_id: process_id, display_name: display_name) do
        if exactly_once?
          released = release_uncommitted == :released
        else
          transaction(operation: "release_claimed_job") do
            released = false
            result = self.class.collection.update_one(
              ownership_filter.merge(started_at: nil),
              { "$set" => { state: "ready" }, "$unset" => claim_unsets },
              **SolidQueue::Mongo.session_options
            )
            released = result.modified_count == 1
          end
        end
      end
      released
    end

    def release_uncommitted(error = nil)
      exhausted = error && DeathRecovery.uncommitted_exhausted?(job)
      updated = self.class.collection.find_one_and_update(
        ownership_filter.merge(started_at: nil),
        exhausted ? uncommitted_failure(error) : uncommitted_release(counting: error.present?),
        projection: { _id: 1 },
        hint: { _id: 1 },
        max_time_ms: RELEASE_UNCOMMITTED_MAX_TIME_MS
      )
      return false unless updated
      return :released unless exhausted

      BatchExecution.complete(job) if batched?
      job.unblock_next_blocked_job
      :exhausted
    rescue ::Mongo::Error::OperationFailure => failure
      raise unless LOCK_CONFLICT_CODES.include?(failure.code)

      :locked
    end

    def uncommitted_exactly_once?
      started_at.nil? && exactly_once?
    end

    def rerunnable?
      !(started_at.present? && at_most_once?)
    end

    def within_attempt
      yield
    rescue Exception
      SolidQueue::Mongo.restart_transaction
      @conflicted = true unless start(run_time_limit)
      raise
    end

    def discard
      raise UndiscardableError, "Can't discard a job in progress"
    end

    def failed_with(error)
      finalizing do
        finalize("failed", error: FailedExecution.error_from(error), finished_at: Time.current) do
          BatchExecution.complete(job) if batched?
        end
      end
    end

    private
      def perform_claimed
        return perform_exactly_once if exactly_once?

        run_time_limit = self.run_time_limit
        return if (at_most_once? || run_time_limit) && !start(run_time_limit)

        result = execute(run_time_limit)
        if result.success?
          finalizing { finished }
        else
          record_failure(result.error)
          raise result.error
        end
      end

      def perform_exactly_once
        run_time_limit = self.run_time_limit
        error = nil

        SolidQueue.instrument(:perform_exactly_once, job_id: job_id, process_id: process_id, display_name: display_name, run_time_limit: run_time_limit) do |payload|
          payload[:outcome] = begin
            transaction(operation: "perform_exactly_once") do
              error = nil
              @conflicted = false
              next :conflict unless start(run_time_limit)

              result = ActiveJob::DeliveryModes.performing(self) { execute(run_time_limit) }
              raise Uncommitted.new(result.error), cause: result.error unless result.success?
              raise Conflict if @conflicted || !finished

              :committed
            end
          rescue Conflict
            :conflict
          rescue Uncommitted => uncommitted
            error = uncommitted.error
            :rolled_back
          rescue => failure
            error = failure
            :rolled_back
          end
        end

        if error
          record_failure(error)
          raise error
        end
      end

      def record_failure(error)
        ExecutionHooks.notify(:on_failure, self, error) if failed_with(error)
      end

      def execute(run_time_limit)
        raise Job::ClassMissingError.for(job) if job.job_class.nil?

        within_run_time_limit(run_time_limit) do
          ActiveJob::Base.execute(job.arguments.merge("provider_job_id" => job.id))
        end
        Result.new(true, nil)
      rescue Exception => error
        Result.new(false, error)
      end

      def within_run_time_limit(run_time_limit, &block)
        if run_time_limit
          Timeout.timeout(run_time_limit.to_f, Processes::RunTimeExceededError, Processes::RunTimeExceededError.for(run_time_limit).message, &block)
        else
          yield
        end
      end

      def finalizing
        yield
      rescue => error
        raise FinalizationError.new(self, cause: error) if still_claimed?
        raise
      end

      def finished
        finalize("finished", finished_at: Time.current) do
          BatchExecution.complete(job) if batched?
          Job.delete_recurring_markers([ bson_id ]) unless SolidQueue.preserve_finished_jobs?
          unless SolidQueue.preserve_finished_jobs?
            self.class.collection.delete_one(
              { _id: bson_id, state: "finished", claim_generation: claim_generation },
              **SolidQueue::Mongo.session_options
            )
          end
        end
      end

      def finalize(target_state, values)
        finalized = false
        transaction(operation: "finalize_claimed_job") do
          finalized = false
          result = self.class.collection.update_one(
            ownership_filter,
            { "$set" => values.merge(state: target_state), "$unset" => claim_unsets },
            **SolidQueue::Mongo.session_options
          )
          if result.modified_count == 1
            yield
            SolidQueue::Mongo.after_commit { job.unblock_next_blocked_job }
            finalized = true
          end
        end
        finalized
      end

      def ownership_filter
        {
          _id: bson_id,
          state: "claimed",
          process_id: SolidQueue::Mongo.id!(process_id),
          claim_token: claim_token,
          claim_generation: claim_generation
        }
      end

      def claim_unsets
        { process_id: true, claim_token: true, claimed_at: true, started_at: true, timeout_at: true }
      end

      def uncommitted_release(counting:)
        values = { state: "ready" }
        values[:arguments] = ActiveSupport::JSON.encode(arguments.merge("executions" => arguments.fetch("executions", 0).to_i + 1)) if counting
        { "$set" => values, "$unset" => claim_unsets }
      end

      def uncommitted_failure(error)
        { "$set" => { state: "failed", error: FailedExecution.error_from(error), finished_at: Time.current }, "$unset" => claim_unsets }
      end

      def start(run_time_limit)
        now = Time.current
        values = { started_at: now }
        values[:timeout_at] = now + run_time_limit + SolidQueue.run_time_grace if run_time_limit

        self.class.collection.update_one(
          ownership_filter.merge(started_at: nil),
          { "$set" => values },
          **SolidQueue::Mongo.session_options
        ).modified_count == 1
      end

      def still_claimed?
        self.class.collection.find(ownership_filter, **SolidQueue::Mongo.session_options).limit(1).first.present?
      rescue
        true
      end
  end
end
