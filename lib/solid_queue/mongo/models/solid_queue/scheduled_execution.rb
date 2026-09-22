# frozen_string_literal: true

module SolidQueue
  class ScheduledExecution < Execution
    class << self
      def schedule(job)
        document = collection.find_one_and_update(
          { _id: job.bson_id, state: { "$in" => [ nil, "scheduled" ] } },
          { "$set" => { state: "scheduled", scheduled_at: job.scheduled_at, updated_at: Time.current } },
          return_document: :after,
          **SolidQueue::Mongo.session_options
        )
        document && from_document(document)
      end

      def create_all_from_jobs(jobs)
        jobs.filter_map { |job| schedule(job) }
      end

      def dispatch_next_batch(batch_size)
        candidate_count, dispatched_count = transaction(operation: "dispatch_scheduled") do
          now = Time.current
          documents = collection.find(
            { state: "scheduled", scheduled_at: { "$lte" => now } },
            **SolidQueue::Mongo.session_options
          ).sort(scheduled_at: 1, priority: 1, _id: 1).limit(batch_size).to_a

          jobs = documents.map { |document| Job.from_document(document) }
          [ documents.size, Job.dispatch_all(jobs).size ]
        end
        return 0 if candidate_count.zero?

        SolidQueue.instrument(:dispatch_scheduled, batch_size: batch_size) do |payload|
          payload[:size] = dispatched_count
        end
      end

      def count
        collection.count_documents({ state: "scheduled" }, **SolidQueue::Mongo.session_options)
      end

      def any?
        count.positive?
      end

      def none?
        count.zero?
      end
    end
  end
end
