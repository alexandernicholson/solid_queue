# frozen_string_literal: true

module SolidQueue
  module Mongo
    module Indexes
      MANIFEST = {
        jobs: [
          { key: { priority: 1, _id: 1 }, name: "ready_poll_all_v2", partial_filter_expression: { state: "ready" } },
          { key: { queue_name: 1, priority: 1, _id: 1 }, name: "ready_poll_by_queue_v2", partial_filter_expression: { state: "ready" } },
          { key: { queue_name: 1, created_at: 1 }, name: "ready_latency_by_queue_v2", partial_filter_expression: { state: "ready" } },
          { key: { created_at: 1 }, name: "ready_latency_all_v2", partial_filter_expression: { state: "ready" } },
          { key: { scheduled_at: 1, priority: 1, _id: 1 }, name: "scheduled_dispatch_v2", partial_filter_expression: { state: "scheduled" } },
          { key: { concurrency_key: 1, priority: 1, _id: 1 }, name: "blocked_release_v2", partial_filter_expression: { state: "blocked" } },
          { key: { expires_at: 1, concurrency_key: 1 }, name: "blocked_maintenance_v2", partial_filter_expression: { state: "blocked" } },
          { key: { process_id: 1, _id: 1 }, name: "claimed_by_process_v2", partial_filter_expression: { state: "claimed" } },
          { key: { claim_token: 1, process_id: 1 }, name: "claimed_by_token_v2", partial_filter_expression: { state: "claimed" } },
          { key: { created_at: 1, _id: 1 }, name: "failed_jobs_v2", partial_filter_expression: { state: "failed" } },
          { key: { finished_at: 1, _id: 1 }, name: "finished_retention_v2", partial_filter_expression: { state: "finished" } },
          { key: { active_job_id: 1, _id: 1 }, name: "active_job_attempts" },
          { key: { batch_id: 1, _id: 1 }, name: "batch_jobs_v2", partial_filter_expression: { batch_id: { "$exists" => true } } },
          { key: { class_name: 1, _id: 1 }, name: "jobs_by_class_v2" }
        ],
        processes: [
          { key: { last_heartbeat_at: 1 }, name: "process_heartbeat" },
          { key: { name: 1, supervisor_id: 1 }, name: "process_identity", unique: true },
          { key: { supervisor_id: 1 }, name: "process_supervisees" }
        ],
        semaphores: [
          { key: { key: 1 }, name: "semaphore_key", unique: true },
          { key: { expires_at: 1, key: 1 }, name: "semaphore_expiration" },
          { key: { key: 1, value: 1 }, name: "semaphore_availability" }
        ],
        pauses: [
          { key: { queue_name: 1 }, name: "pause_queue", unique: true }
        ],
        recurring_tasks: [
          { key: { key: 1 }, name: "recurring_task_key", unique: true },
          { key: { static: 1, key: 1 }, name: "recurring_task_source" }
        ],
        recurring_executions: [
          { key: { task_key: 1, run_at: 1 }, name: "recurring_executions_task_key_run_at", unique: true },
          { key: { job_id: 1 }, name: "recurring_execution_job", unique: true },
          { key: { task_key: 1, run_at: -1 }, name: "recurring_execution_latest" }
        ],
        batches: [
          { key: { active_job_batch_id: 1 }, name: "active_job_batch", unique: true },
          { key: { finished_at: 1, updated_at: 1 }, name: "batch_completion" },
          { key: { failed_at: 1, finished_at: 1 }, name: "batch_retention" },
          { key: { finished_at: 1, enqueued_at: 1 }, name: "batch_stalled" },
          { key: { finished_at: 1, enqueued_at: 1, created_at: 1 }, name: "batch_unstarted" }
        ],
        batch_executions: [
          { key: { job_id: 1 }, name: "batch_execution_job", unique: true },
          { key: { batch_id: 1, created_at: 1 }, name: "batch_execution_members" },
          { key: { batch_id: 1, active_job_id: 1 }, name: "batch_logical_jobs" }
        ]
      }.freeze

      OBSOLETE_MANAGED_INDEXES = {
        jobs: [
          { key: { state: 1, priority: 1, _id: 1 }, name: "ready_poll_all", partial_filter_expression: { state: "ready" } },
          { key: { state: 1, queue_name: 1, priority: 1, _id: 1 }, name: "ready_poll_by_queue", partial_filter_expression: { state: "ready" } },
          { key: { state: 1, queue_name: 1, created_at: 1 }, name: "ready_latency_by_queue", partial_filter_expression: { state: "ready" } },
          { key: { state: 1, created_at: 1 }, name: "ready_latency_all", partial_filter_expression: { state: "ready" } },
          { key: { state: 1, scheduled_at: 1, priority: 1, _id: 1 }, name: "scheduled_dispatch", partial_filter_expression: { state: "scheduled" } },
          { key: { state: 1, concurrency_key: 1, priority: 1, _id: 1 }, name: "blocked_release", partial_filter_expression: { state: "blocked" } },
          { key: { state: 1, expires_at: 1, concurrency_key: 1 }, name: "blocked_maintenance", partial_filter_expression: { state: "blocked" } },
          { key: { state: 1, process_id: 1, _id: 1 }, name: "claimed_by_process", partial_filter_expression: { state: "claimed" } },
          { key: { state: 1, claim_token: 1 }, name: "claimed_by_token", partial_filter_expression: { state: "claimed" } },
          { key: { state: 1, created_at: 1, _id: 1 }, name: "failed_jobs", partial_filter_expression: { state: "failed" } },
          { key: { state: 1, finished_at: 1, _id: 1 }, name: "finished_retention", partial_filter_expression: { state: "finished" } },
          { key: { batch_id: 1, state: 1, _id: 1 }, name: "batch_jobs" },
          { key: { class_name: 1, state: 1 }, name: "jobs_by_class" }
        ]
      }.freeze

      class << self
        def prepare!(client)
          MANIFEST.each do |collection_name, specs|
            indexes = client["solid_queue_#{collection_name}"].indexes
            specs.each do |spec|
              options = spec.reject { |key, _| key == :key }
              indexes.create_one(spec.fetch(:key), options)
            end
            remove_obsolete_managed_indexes(indexes, collection_name)
          end
          @prepared_client = client
          @prepared_pid = ::Process.pid
          true
        end

        def prepared?(client = SolidQueue::Mongo.client)
          return true if @prepared_client.equal?(client) && @prepared_pid == ::Process.pid

          prepared = MANIFEST.all? do |collection_name, specs|
            existing = client["solid_queue_#{collection_name}"].indexes.to_a
            specs.all? { |spec| existing.any? { |index| index_matches?(index, spec) } }
          end
          if prepared
            @prepared_client = client
            @prepared_pid = ::Process.pid
          end
          prepared
        end

        def reset!
          @prepared_client = @prepared_pid = nil
        end

        private
          def remove_obsolete_managed_indexes(indexes, collection_name)
            obsolete_specs = OBSOLETE_MANAGED_INDEXES.fetch(collection_name, [])
            return if obsolete_specs.empty?

            existing = indexes.to_a
            obsolete_specs.each do |spec|
              index = existing.find { |candidate| index_matches?(candidate, spec) }
              indexes.drop_one(index.fetch("name")) if index
            end
          end

          def index_matches?(index, spec)
            return false unless index["name"] == spec.fetch(:name)
            return false unless normalized(index["key"]) == normalized(spec.fetch(:key))

            spec.all? do |option, value|
              option == :key || option == :name || normalized(index[driver_option_name(option)]) == normalized(value)
            end
          end

          def driver_option_name(option)
            {
              partial_filter_expression: "partialFilterExpression",
              expire_after: "expireAfterSeconds"
            }.fetch(option, option.to_s)
          end

          def normalized(value)
            if value.respond_to?(:each_pair)
              value.each_pair.with_object({}) { |(key, item), hash| hash[key.to_s] = normalized(item) }
            elsif value.is_a?(Array)
              value.map { |item| normalized(item) }
            else
              value
            end
          end
      end
    end
  end
end
