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
        jobs = next_batch(batch_size)
        return 0 if jobs.empty?

        SolidQueue.instrument(:dispatch_scheduled, batch_size: batch_size) do |payload|
          payload[:size] = dispatch_jobs(jobs)
        end
      end

      def any?
        count.positive?
      end

      def none?
        count.zero?
      end

      private
        def next_batch(batch_size)
          collection.find(
            { state: "scheduled", scheduled_at: { "$lte" => Time.current } },
            hint: "scheduled_dispatch_v2",
            **SolidQueue::Mongo.session_options
          ).sort(scheduled_at: 1, priority: 1, _id: 1).limit(batch_size).map { |document| Job.from_document(document) }
        end

        def dispatch_jobs(jobs)
          without_limit, with_limit = jobs.partition { |job| !job.concurrency_limited? }
          dispatched = transaction(operation: "dispatch_scheduled") { ReadyExecution.create_all_from_jobs(without_limit).size }
          dispatched + with_limit.count { |job| %w[ready blocked].include?(job.dispatch) }
        end
    end
  end
end
