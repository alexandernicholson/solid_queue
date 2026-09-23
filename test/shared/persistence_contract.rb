# frozen_string_literal: true

module PersistenceContract
  OPERATIONS = {
    "SolidQueue::Job" => {
      class: %i[ enqueue enqueue_all find find_by clear_finished_in_batches ],
      instance: %i[ id active_job_id class_name queue_name priority arguments status finished? ready? claimed? failed?
        scheduled? blocked? finished! failed_with retry discard dispatch prepare_for_execution due?
        concurrency_limited? deduplicated? unblock_next_blocked_job batch ]
    },
    "SolidQueue::ReadyExecution" => {
      class: %i[ claim aggregated_count_across create_all_from_jobs discard_all_in_batches discard_all_from_jobs
        latency count_waiting_longer_than discard_all_in_queue ],
      instance: %i[ job job_id discard ]
    },
    "SolidQueue::ClaimedExecution" => {
      class: %i[ claiming release_for_process fail_for_process fail_orphaned release_all fail_all_with ],
      instance: %i[ job job_id process_id perform release failed_with discard ]
    },
    "SolidQueue::FailedExecution" => {
      class: %i[ retry_all discard_all_in_batches discard_all_from_jobs
        discard_all_in_queue ],
      instance: %i[ job job_id retry discard exception_class message backtrace ]
    },
    "SolidQueue::ScheduledExecution" => {
      class: %i[ dispatch_next_batch discard_all_in_batches discard_all_from_jobs none?
        due_count_across discard_all_in_queue ],
      instance: %i[ job job_id discard ]
    },
    "SolidQueue::BlockedExecution" => {
      class: %i[ unblock release_many release_one discard_all_in_batches discard_all_from_jobs
        discard_all_in_queue ],
      instance: %i[ job job_id release discard ]
    },
    "SolidQueue::Semaphore" => {
      class: %i[ wait signal signal_all expire ],
      instance: %i[ key value expires_at ]
    },
    "SolidQueue::Process" => {
      class: %i[ register prune find_by ],
      instance: %i[ id kind name pid hostname metadata last_heartbeat_at supervisor_id heartbeat deregister
        update_metadata! prune ]
    },
    "SolidQueue::Deduplication" => {
      class: %i[ reserve release ],
      instance: %i[ key active_job_id expires_at ]
    },
    "SolidQueue::Batch" => {
      class: %i[ enqueue current_batch_id wrap_in_batch_context find_by migrated? warn_about_pending_migrations
        sweep_stalled clear_finished_in_batches ],
      instance: %i[ id enqueue total_jobs completed_jobs failed_jobs pending_jobs finished? succeeded? failed? status ]
    },
    "SolidQueue::RecurringTask" => {
      class: %i[ wrap from_configuration create_or_update_all dynamic_tasks static_tasks task_keys delete_static_except ],
      instance: %i[ key next_time enqueue valid? errors ]
    },
    "SolidQueue::Queue" => {
      class: %i[ all find_by_name ],
      instance: %i[ name paused? pause resume size latency clear ]
    }
  }.freeze

  def test_models_implement_the_persistence_contract
    missing = OPERATIONS.flat_map do |model_name, operations|
      model = model_name.safe_constantize
      next [ model_name ] unless model

      model.define_attribute_methods if model.respond_to?(:define_attribute_methods)

      operations.fetch(:class).reject { |name| model.respond_to?(name) }.map { |name| "#{model_name}.#{name}" } +
        operations.fetch(:instance).reject { |name| model.method_defined?(name) }.map { |name| "#{model_name}##{name}" }
    end

    assert_empty missing, "Missing persistence operations: #{missing.join(", ")}"
  end

  def test_claims_and_counts_accept_a_priority_range
    [ [ "SolidQueue::ReadyExecution", :claim ], [ "SolidQueue::ReadyExecution", :aggregated_count_across ], [ "SolidQueue::ScheduledExecution", :due_count_across ] ].each do |model_name, operation|
      assert_includes model_name.constantize.method(operation).parameters, [ :key, :priority ], "#{model_name}.#{operation} takes no priority: range"
    end
  end
end
