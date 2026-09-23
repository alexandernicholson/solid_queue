# frozen_string_literal: true

module SolidQueue
  class ScheduledExecution < Execution
    include Dispatching

    scope :due, -> { where(scheduled_at: ..Time.current) }
    scope :ordered, -> { order(scheduled_at: :asc, priority: :asc, job_id: :asc) }
    scope :next_batch, ->(batch_size) { due.ordered.limit(batch_size) }
    scope :queued_as, ->(queue_name) { where(queue_name: queue_name) }
    scope :prioritized_within, ->(range) { where(priority: range) if range }

    assumes_attributes_from_job :scheduled_at

    class << self
      def dispatch_next_batch(batch_size)
        transaction do
          job_ids = next_batch(batch_size).non_blocking_lock.pluck(:job_id)
          if job_ids.empty? then 0
          else
            SolidQueue.instrument(:dispatch_scheduled, batch_size: batch_size) do |payload|
              payload[:size] = dispatch_jobs(job_ids)
            end
          end
        end
      end

      def due_count_across(queue_list, priority: nil)
        QueueSelector.new(queue_list, self).scoped_relations.sum { |queue_relation| queue_relation.due.prioritized_within(priority).count }
      end
    end
  end
end
