# frozen_string_literal: true

module SolidQueue
  class ReadyExecution < Execution
    scope :queued_as, ->(queue_name) { where(queue_name: queue_name) }
    scope :prioritized_within, ->(range) { where(priority: range) if range }

    assumes_attributes_from_job

    class << self
      def claim(queue_list, limit, process_id, priority: nil)
        QueueSelector.new(queue_list, self).scoped_relations.flat_map do |queue_relation|
          select_and_lock(queue_relation.prioritized_within(priority), process_id, limit).tap do |locked|
            limit -= locked.size
          end
        end
      end

      def aggregated_count_across(queue_list, priority: nil)
        QueueSelector.new(queue_list, self).scoped_relations.sum { |queue_relation| queue_relation.prioritized_within(priority).count }
      end

      def latency
        oldest_due_at = Job.where(id: select(:job_id)).minimum(:scheduled_at)
        oldest_due_at ? [ (Time.current - oldest_due_at).to_i, 0 ].max : 0
      end

      def count_waiting_longer_than(age)
        where(job_id: Job.where(scheduled_at: ...age.seconds.ago, finished_at: nil).select(:id)).count
      end

      private
        def select_and_lock(queue_relation, process_id, limit)
          return [] if limit <= 0

          transaction do
            candidates = select_candidates(queue_relation, limit)
            lock_candidates(candidates, process_id)
          end
        end

        def select_candidates(queue_relation, limit)
          # Force query execution here with #to_a to avoid unintended FOR UPDATE query executions
          queue_relation.ordered.limit(limit).non_blocking_lock.select(:id, :job_id).to_a
        end

        def lock_candidates(executions, process_id)
          return [] if executions.none?

          SolidQueue::ClaimedExecution.claiming(executions.map(&:job_id), process_id) do |claimed|
            ids_to_delete = executions.index_by(&:job_id).values_at(*claimed.map(&:job_id)).map(&:id)
            where(id: ids_to_delete).delete_all
          end
        end


        def discard_jobs(job_ids)
          Job.release_all_concurrency_locks Job.where(id: job_ids)
          super
        end
    end
  end
end
