# frozen_string_literal: true

module SolidQueue
  class RecurringExecution < Record
    collection_name :recurring_executions

    class AlreadyRecorded < StandardError; end

    field :job_id
    field :task_key
    field :run_at

    class << self
      def record(task_key, run_at)
        transaction(operation: "record_recurring_execution") do
          active_job = yield
          if active_job && active_job.successfully_enqueued?
            collection.insert_one(
              {
                _id: BSON::ObjectId.new,
                job_id: SolidQueue::Mongo.id(active_job.provider_job_id),
                task_key: task_key.to_s,
                run_at: run_at,
                created_at: Time.current
              },
              **SolidQueue::Mongo.session_options
            )
          end
          active_job
        end
      rescue ::Mongo::Error::OperationFailure => error
        if duplicate_recurring_run?(error)
          raise AlreadyRecorded
        else
          raise_persistence_error(error)
        end
      rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
        raise_persistence_error(error)
      end

      def clear_in_batches(batch_size: 500)
        loop do
          orphan_ids = collection.aggregate(
            [
              {
                "$lookup" => {
                  from: Job.collection.name,
                  localField: "job_id",
                  foreignField: "_id",
                  as: "job"
                }
              },
              { "$match" => { job: { "$size" => 0 } } },
              { "$project" => { _id: 1 } },
              { "$limit" => batch_size }
            ],
            **SolidQueue::Mongo.session_options
          ).map { |marker| marker["_id"] || marker[:_id] }
          break if orphan_ids.empty?

          collection.delete_many({ _id: { "$in" => orphan_ids } }, **SolidQueue::Mongo.session_options)
        end
      rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
        raise_persistence_error(error)
      end

      def last_enqueued_at_by_task(keys)
        requested_keys = Array(keys).map(&:to_s)
        return {} if requested_keys.empty?

        pipeline = [
          { "$match" => { task_key: { "$in" => requested_keys } } },
          { "$group" => { _id: "$task_key", run_at: { "$max" => "$run_at" } } }
        ]
        collection.aggregate(pipeline, **SolidQueue::Mongo.session_options).each_with_object({}) do |row, times|
          times[row["_id"] || row[:_id]] = row["run_at"] || row[:run_at]
        end
      end

      private
        def duplicate_recurring_run?(error)
          return false unless error.respond_to?(:code) && error.code == 11_000

          details = if error.respond_to?(:result) && error.result.respond_to?(:documents)
            error.result.documents.first
          elsif error.respond_to?(:result)
            error.result
          end
          pattern = details && (details["keyPattern"] || details[:keyPattern])
          return pattern.keys.map(&:to_s).sort == %w[run_at task_key] if pattern.respond_to?(:keys)
          error.message.include?("task_key_1_run_at_1") ||
            error.message.include?("recurring_executions_task_key_run_at")
        end
    end
  end
end
