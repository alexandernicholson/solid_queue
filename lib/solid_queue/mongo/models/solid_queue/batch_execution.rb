# frozen_string_literal: true

module SolidQueue
  class BatchExecution < Record
    collection_name :batch_executions

    field :job_id
    field :batch_id
    field :active_job_id
    field :kind, default: "attempt"
    field :created_at

    class << self
      def create_all_from_jobs(jobs)
        jobs.select(&:batched?).group_by(&:batch_id).each_value do |jobs_in_batch|
          SolidQueue::Mongo.transaction(operation: "add jobs to batch") do
            batch_id = SolidQueue::Mongo.id!(jobs_in_batch.first.batch_id)
            logical_ids = jobs_in_batch.map(&:active_job_id).uniq
            now = Time.current
            logical_operations = logical_ids.map do |active_job_id|
              logical_job_id = "logical:#{batch_id}:#{active_job_id}"
              {
                update_one: {
                  filter: { job_id: logical_job_id },
                  update: { "$setOnInsert" => {
                    job_id: logical_job_id,
                    batch_id: batch_id,
                    active_job_id: active_job_id,
                    kind: "logical",
                    created_at: now
                  } },
                  upsert: true
                }
              }
            end
            logical_result = collection.bulk_write(logical_operations, ordered: false, **SolidQueue::Mongo.session_options)

            # Both additions and completion checks write the parent in their
            # transactions, converting a would-be write skew into a conflict.
            result = SolidQueue::Mongo.collection(:batches).update_one(
              { _id: batch_id, finished_at: nil },
              {
                "$inc" => { total_jobs: logical_result.upserted_count, version: 1 },
                "$set" => { updated_at: now }
              },
              **SolidQueue::Mongo.session_options
            )
            raise Batch::AlreadyFinished, "Can't add jobs into an already finished batch" unless result.matched_count == 1

            operations = jobs_in_batch.map do |job|
              {
                update_one: {
                  filter: { job_id: job.bson_id },
                  update: { "$setOnInsert" => {
                    job_id: job.bson_id,
                    batch_id: batch_id,
                    active_job_id: job.active_job_id,
                    kind: "attempt",
                    created_at: now
                  } },
                  upsert: true
                }
              }
            end
            collection.bulk_write(operations, ordered: false, **SolidQueue::Mongo.session_options)
          end
        end
      end

      def complete(job)
        complete_job_id(job.bson_id)
      rescue StandardError => error
        SolidQueue.instrument(:batch_progress_error, batch_id: job.batch_id, job_id: job.id, error: error)
        raise
      end

      def count
        collection.count_documents({ kind: { "$ne" => "logical" } }, **SolidQueue::Mongo.session_options)
      end

      def count_for_batch(batch_id)
        collection.count_documents(
          { batch_id: SolidQueue::Mongo.id!(batch_id), kind: "attempt" },
          **SolidQueue::Mongo.session_options
        )
      end

      def outstanding_for_batch?(batch_id)
        outstanding_query(batch_id).first.present?
      end

      def outstanding_query(batch_id)
        collection.find(
          { batch_id: SolidQueue::Mongo.id!(batch_id), kind: "attempt" },
          hint: "batch_execution_attempts",
          **SolidQueue::Mongo.session_options
        ).projection(_id: 1).limit(1)
      end

      def for_batch(batch_id)
        collection.find(
          { batch_id: SolidQueue::Mongo.id!(batch_id), kind: { "$ne" => "logical" } },
          **SolidQueue::Mongo.session_options
        ).map do |document|
          from_document(document)
        end
      end

      def sweep_stale_executions(batch_size: 500)
        stale = collection.aggregate(
          [
            { "$match" => { kind: { "$ne" => "logical" } } },
            { "$lookup" => {
              from: SolidQueue::Mongo.collection(:jobs).name,
              localField: "job_id",
              foreignField: "_id",
              as: "job"
            } },
            { "$match" => {
              "$or" => [
                { "job.0" => { "$exists" => false } },
                { "job.state" => { "$in" => %w[finished failed] } }
              ]
            } },
            { "$project" => { job_id: 1 } },
            { "$limit" => batch_size }
          ],
          **SolidQueue::Mongo.session_options
        )
        stale.count { |marker| complete_job_id(marker["job_id"] || marker[:job_id]) }
      end

      private
        def complete_job_id(job_id)
          removed = false
          SolidQueue::Mongo.transaction(operation: "complete batch execution") do
            marker = collection.find({ job_id: job_id }, **SolidQueue::Mongo.session_options).first
            next unless marker

            batch_id = marker["batch_id"] || marker[:batch_id]
            deletion = collection.delete_one({ _id: marker["_id"] || marker[:_id] }, **SolidQueue::Mongo.session_options)
            next unless deletion.deleted_count == 1

            SolidQueue::Mongo.collection(:batches).update_one(
              { _id: batch_id },
              { "$inc" => { version: 1 }, "$set" => { updated_at: Time.current } },
              **SolidQueue::Mongo.session_options
            )
            removed = true
            SolidQueue::Mongo.after_commit { finish_batch(batch_id, job_id) }
          end
          removed
        end

        def finish_batch(batch_id, job_id)
          Batch.find(batch_id).finish
        rescue SolidQueue::RecordNotFound
          # Retention can remove the batch before delayed cleanup runs.
        rescue StandardError => error
          SolidQueue.instrument(
            :batch_progress_error,
            batch_id: batch_id.to_s,
            job_id: job_id.to_s,
            error: error
          )
        end
    end

    def batch
      Batch.find(batch_id)
    end

    def destroy!
      self.class.send(:complete_job_id, job_id)
      self
    end

    alias_method :destroy, :destroy!
  end
end
