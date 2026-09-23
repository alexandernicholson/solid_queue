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
        jobs = collection.find(
          { state: "scheduled", scheduled_at: { "$lte" => Time.current } },
          hint: "scheduled_dispatch_v2",
          **SolidQueue::Mongo.session_options
        ).sort(scheduled_at: 1, priority: 1, _id: 1).limit(batch_size).map { |document| Job.from_document(document) }
        return 0 if jobs.empty?

        SolidQueue.instrument(:dispatch_scheduled, batch_size: batch_size) do |payload|
          without_limit, with_limit = jobs.partition { |job| !job.concurrency_limited? }
          dispatched = transaction(operation: "dispatch_scheduled") { ReadyExecution.create_all_from_jobs(without_limit).size }
          payload[:size] = dispatched + with_limit.count { |job| %w[ready blocked].include?(job.dispatch) }
        end
      end

      def due_count_across(queue_list, priority: nil)
        QueueSelector.new(queue_list, self).filters.sum do |queue_name|
          filter = { state: "scheduled", scheduled_at: { "$lte" => Time.current } }
          filter[:queue_name] = queue_name if queue_name
          collection.count_documents(prioritized_within(filter, priority), **SolidQueue::Mongo.session_options)
        end
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
