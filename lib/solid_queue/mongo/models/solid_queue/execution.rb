# frozen_string_literal: true

module SolidQueue
  class Execution < Job
    class UndiscardableError < StandardError; end

    class << self
      def type
        name.demodulize.sub(/Execution\z/, "").underscore.to_sym
      end

      def find(id)
        find_by(id: SolidQueue::Mongo.id!(id)) || raise_record_not_found(id)
      end

      def find_by(filter = {})
        super(scoped(filter))
      end

      def count(filter = {})
        super(scoped(filter))
      end

      def delete_all(filter = {})
        super(scoped(filter))
      end

      def distinct_values_of(field, filter = {})
        super(field, scoped(filter))
      end

      def create_all_from_jobs(jobs)
        ids = Array(jobs).map(&:bson_id)
        return [] if ids.empty?

        collection.update_many(
          { _id: { "$in" => ids }, state: { "$in" => [ nil, "scheduled", "failed" ] } },
          { "$set" => { state: type.to_s }, "$unset" => { process_id: true, claim_token: true, claimed_at: true, started_at: true, error: true, finished_at: true } },
          **SolidQueue::Mongo.session_options
        )
        Job.find_many(ids).select { |job| job.state == type.to_s }.map { |job| from_document(job.attributes) }
      end

      def discard_all_in_batches(batch_size: 500, **filter)
        discard_matching(scoped(filter), batch_size: batch_size)
      end

      def discard_all_from_jobs(jobs)
        ids = Array(jobs).map(&:bson_id)
        SolidQueue.instrument(:discard_all, jobs_size: ids.size, status: type) do |payload|
          payload[:size] = discard_jobs(ids, expected_state: type.to_s)
        end
      end

      private
        def discard_matching(filter, batch_size: 500)
          discarded = 0
          batches = 0
          SolidQueue.instrument(:discard_all, batch_size: batch_size, status: filter[:state]&.to_sym || type, batches: 0, size: 0) do |payload|
            loop do
              ids = collection.find(filter, **SolidQueue::Mongo.session_options).projection(_id: 1).sort(_id: 1).limit(batch_size).map { |row| row["_id"] }
              break if ids.empty?
              count = discard_jobs(ids, expected_state: filter[:state])
              break if count.zero?
              discarded += count
              batches += 1
            end
            payload[:size] = discarded
            payload[:batches] = batches
          end
          discarded
        end

        def scoped(filter)
          filter.merge(state: type.to_s)
        end

        def discard_jobs(ids, expected_state:)
          discarded_jobs = []
          transaction(operation: "discard_jobs") do
            filter = { _id: { "$in" => ids }, state: expected_state }
            discarded_jobs = collection.find(filter, **SolidQueue::Mongo.session_options).map { |doc| Job.from_document(doc) }
            discarded_jobs.each do |job|
              BatchExecution.complete(job) if defined?(BatchExecution) && defined?(Batch) && Batch.migrated? && job.batch_id.present?
            end
            Job.delete_recurring_markers(discarded_jobs.map(&:bson_id))
            collection.delete_many(filter, **SolidQueue::Mongo.session_options)
            Deduplication.release(discarded_jobs.select(&:deduplicated?), windowed: true)
          end
          discarded_jobs.each do |job|
            SolidQueue::Mongo.after_commit { job.unblock_next_blocked_job } if job.state == "ready" && job.concurrency_limited?
          end
          discarded_jobs.size
        end
    end

    def job_id
      id
    end

    def job
      Job.from_document(attributes)
    end

    def type
      self.class.type
    end

    def discard
      raise UndiscardableError, "Can't discard a job in progress" if state == "claimed"

      SolidQueue.instrument(:discard, job_id: id, status: type) do
        discarded = self.class.send(:discard_jobs, [ bson_id ], expected_state: state)
        raise SolidQueue::RecordNotFound, "Execution #{id} no longer exists" if discarded.zero?
      end
      true
    end
  end
end
