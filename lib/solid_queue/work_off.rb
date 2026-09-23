# frozen_string_literal: true

module SolidQueue
  class WorkOff < Processes::Base
    Result = Struct.new(:successes, :failures)

    attr_reader :queues, :limit, :priority

    def initialize(queues: "*", limit: 100, priority: nil)
      @queues = Array(queues).map(&:to_s)
      @limit = limit
      @priority = priority

      super
    end

    def kind
      "Worker"
    end

    def metadata
      { queues: queues.join(","), limit: limit, priority_range: priority&.to_s }
    end

    def run
      result = Result.new(0, 0)

      SolidQueue.instrument(:work_off, queues: queues, limit: limit, priority: priority) do |payload|
        run_callbacks(:boot)
        dispatch_due_jobs
        perform_jobs(result)
      ensure
        run_callbacks(:shutdown)
        self_pipe.each_value(&:close)
        payload[:successes], payload[:failures] = result.to_a
      end

      result
    end

    private
      def generate_name
        "work_off-#{::Process.pid}-#{SecureRandom.hex(4)}"
      end

      def perform_jobs(result)
        while registered? && result.successes + result.failures < limit
          execution = claim_next || (claim_next if dispatch_due_jobs.positive?)
          break unless execution

          perform(execution, result)
        end
      end

      def claim_next
        wrap_in_app_executor { SolidQueue::ReadyExecution.claim(queues, 1, process_id, priority: priority).first }
      end

      def dispatch_due_jobs
        dispatched = 0

        loop do
          batch = wrap_in_app_executor { SolidQueue::ScheduledExecution.dispatch_next_batch(limit) }
          break if batch.zero?

          dispatched += batch
        end

        dispatched
      end

      def perform(execution, result)
        wrap_in_app_executor { execution.perform }
        result.successes += 1
      rescue Processes::UnrecoverableError
        raise
      rescue Exception => error
        result.failures += 1
        raise unless error.is_a?(StandardError)

        handle_thread_error(error)
      end
  end
end
