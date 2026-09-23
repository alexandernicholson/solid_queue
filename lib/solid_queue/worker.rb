# frozen_string_literal: true

module SolidQueue
  class Worker < Processes::Poller
    include LifecycleHooks

    after_boot :run_start_hooks
    before_shutdown :run_stop_hooks
    after_shutdown :run_exit_hooks

    attr_reader :queues, :pool, :priority_range

    def initialize(**options)
      execution_pool_type = options.key?(:fibers) ? :fiber : :thread

      options = options.dup.with_defaults(SolidQueue::Configuration::WORKER_DEFAULTS)
      execution_pool_size = execution_pool_type == :fiber ? options[:fibers] : options[:threads]

      # Ensure that the queues array is deep frozen to prevent accidental modification
      @queues = Array(options[:queues]).map(&:freeze).freeze
      @priority_range = Range.new(options[:min_priority], options[:max_priority]) if options[:min_priority] || options[:max_priority]
      @exit_on_complete = options[:exit_on_complete] == true
      @drained = false

      @pool = Pool.build \
        type: execution_pool_type,
        size: execution_pool_size,
        on_idle: -> { wake_up },
        on_unrecoverable_error: -> { request_termination }

      super(**options)
    end

    def metadata
      super.merge(queues: queues.join(","), pool_type: pool.type, pool_size: pool.size, priority_range: priority_range&.to_s, exit_on_complete: exit_on_complete? || nil)
    end

    def drained?
      @drained
    end

    private
      def poll
        with_execution_hooks(:around_poll) do
          claim_executions.then do |executions|
            executions.each do |execution|
              pool.post(execution)
            end

            drain if exit_on_complete? && !drained? && all_claims_finished? && nothing_left_to_run?

            pool.idle? ? polling_interval : 10.minutes
          end
        end
      end

      def claim_executions
        with_polling_volume do
          with_execution_hooks(:around_claim) do
            SolidQueue::ReadyExecution.claim(queues, pool.available_capacity, process_id, priority: priority_range)
          end
        end
      end

      def with_execution_hooks(name, &block)
        if defined?(SolidQueue::ExecutionHooks)
          SolidQueue::ExecutionHooks.run(name, self, &block)
        else
          yield
        end
      end

      def exit_on_complete?
        @exit_on_complete
      end

      def all_claims_finished?
        pool.available_capacity == pool.size
      end

      def nothing_left_to_run?
        SolidQueue::ScheduledExecution.due_count_across(queues, priority: priority_range).zero? && all_work_completed?
      end

      def drain
        SolidQueue.instrument(:drained, process_id: process_id, name: name, queues: queues, priority_range: priority_range) do
          @drained = true

          if running_as_fork? && supervised?
            stop_supervisor
          elsif !supervised?
            request_termination
          end
        end
      end

      def stop_supervisor
        ::Process.kill(:TERM, supervisor.pid)
      rescue Errno::ESRCH
        request_termination
      end

      def request_termination
        # Signal the poller to shut down without joining from the pool thread.
        # Runnable#stop joins when unsupervised, which would deadlock once
        # shutdown waits for this pool thread to finish.
        @stopped = true
        wake_up
      end

      def shutdown
        pool.shutdown
        pool.wait_for_termination(SolidQueue.shutdown_timeout)

        super
      end

      def all_work_completed?
        SolidQueue::ReadyExecution.aggregated_count_across(queues, priority: priority_range).zero?
      end

      def set_procline
        procline [ "waiting for jobs in #{queues.join(",")}", priority_range && "with priorities #{priority_range}" ].compact.join(" ")
      end
  end
end
