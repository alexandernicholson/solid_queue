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
        process_bson_id = SolidQueue::Mongo.id!(process_id)
        instrument_release_many(claimed_for(process_id: process_bson_id))
      end

      def fail_for_process(process_id, error)
        process_bson_id = SolidQueue::Mongo.id!(process_id)
        fail_many(claimed_for(process_id: process_bson_id), error)
      end

      def fail_orphaned(error)
        executions = collection.aggregate([
          { "$match" => { state: "claimed" } },
          { "$lookup" => {
            from: SolidQueue::Mongo.collection(:processes).name,
            localField: "process_id", foreignField: "_id", as: "owner"
          } },
          { "$match" => { "owner.0" => { "$exists" => false } } },
          { "$project" => { owner: 0 } }
        ], read_concern: { level: :snapshot }, **SolidQueue::Mongo.session_options).map { |document| from_document(document) }
        fail_many(executions, error)
      end

      def release_all
        instrument_release_many(claimed_for)
      end

      def fail_all_with(error)
        fail_many(claimed_for, error)
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

        def instrument_release_many(executions)
          SolidQueue.instrument(:release_many_claimed) do |payload|
            payload[:size] = release_many(executions)
          end
        end

        def release_many(executions)
          executions.count(&:release)
        end

        def fail_many(executions, error)
          return 0 if executions.empty?

          SolidQueue.instrument(:fail_many_claimed) do |payload|
            failed = executions.count { |execution| execution.failed_with(error) }
            payload[:process_ids] = executions.map(&:process_id).uniq
            payload[:job_ids] = executions.map(&:job_id).uniq
            payload[:size] = failed
            payload[:error] = error
            failed
          end
        end
    end

    def perform
      result = execute
      if result.success?
        finalizing { finalize_success }
      else
        finalizing { finalize_failure(result.error) }
        raise result.error
      end
    end

    def release
      released = false
      SolidQueue.instrument(:release_claimed, job_id: job_id, process_id: process_id) do
        transaction(operation: "release_claimed_job") do
          released = false
          result = self.class.collection.update_one(
            ownership_filter,
            { "$set" => { state: "ready" }, "$unset" => claim_unsets },
            **SolidQueue::Mongo.session_options
          )
          released = result.modified_count == 1
        end
      end
      released
    end

    def discard
      raise UndiscardableError, "Can't discard a job in progress"
    end

    def failed_with(error)
      finalizing { finalize_failure(error) }
    end

    private
      def execute
        raise Job::ClassMissingError.for(job) if job.job_class.nil?

        ActiveJob::Base.execute(job.arguments.merge("provider_job_id" => job.id))
        Result.new(true, nil)
      rescue Exception => error
        Result.new(false, error)
      end

      def finalizing
        yield
      rescue => error
        raise FinalizationError.new(self, cause: error) if still_claimed?
        raise
      end

      def finalize_success
        finalize("finished", finished_at: Time.current) do
          BatchExecution.complete(job) if batch_tracking?
          Job.delete_recurring_markers([ bson_id ]) unless SolidQueue.preserve_finished_jobs?
          unless SolidQueue.preserve_finished_jobs?
            self.class.collection.delete_one(
              { _id: bson_id, state: "finished", claim_generation: claim_generation },
              **SolidQueue::Mongo.session_options
            )
          end
        end
      end

      def finalize_failure(error)
        finalize("failed", error: FailedExecution.error_from(error), finished_at: Time.current) do
          BatchExecution.complete(job) if batch_tracking?
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
        { process_id: true, claim_token: true, claimed_at: true }
      end

      def batch_tracking?
        Batch.migrated? && batch_id.present?
      end

      def still_claimed?
        self.class.collection.find(ownership_filter, **SolidQueue::Mongo.session_options).limit(1).first.present?
      rescue
        true
      end
  end
end
