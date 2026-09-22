# frozen_string_literal: true

module SolidQueue
  class FailedExecution < Execution
    MAX_ERROR_BYTES = 64 * 1024
    MAX_EXCEPTION_CLASS_BYTES = 1024
    MAX_MESSAGE_BYTES = 16 * 1024
    MAX_BACKTRACE_LINES = 64
    MAX_BACKTRACE_LINE_BYTES = 512

    attr_accessor :exception

    class << self
      def error_from(exception)
        {
          "exception_class" => truncate_utf8(exception.class.name, MAX_EXCEPTION_CLASS_BYTES),
          "message" => truncate_utf8(exception.message, MAX_MESSAGE_BYTES),
          "backtrace" => Array(exception.backtrace).first(MAX_BACKTRACE_LINES).map do |line|
            truncate_utf8(line, MAX_BACKTRACE_LINE_BYTES)
          end
        }
      end

      def retry_all(jobs)
        jobs = Array(jobs)
        SolidQueue.instrument(:retry_all, jobs_size: jobs.size) do |payload|
          payload[:size] = jobs.count do |item|
            execution = item.is_a?(FailedExecution) ? item : find_failed(item.respond_to?(:id) ? item.id : item)
            execution&.retry
          end
        end
      end

      def find_failed(id)
        document = collection.find({ _id: SolidQueue::Mongo.id!(id), state: "failed" }, **SolidQueue::Mongo.session_options).limit(1).first
        from_document(document) if document
      end

      private
        def truncate_utf8(value, max_bytes)
          string = value.to_s
          return string if string.encoding == Encoding::UTF_8 && string.valid_encoding? && string.bytesize <= max_bytes

          candidate = string.bytesize > max_bytes ? string.byteslice(0, max_bytes) : string
          candidate = candidate.encode(Encoding::UTF_8, invalid: :replace, undef: :replace)
          candidate = candidate.byteslice(0, max_bytes) if candidate.bytesize > max_bytes
          candidate = candidate.byteslice(0, candidate.bytesize - 1) until candidate.valid_encoding?
          candidate
        end
    end

    def retry
      retried = false
      SolidQueue.instrument(:retry, job_id: job_id) do
        transaction(operation: "retry_failed_job") do
          retried = false
          payload = arguments.deep_dup
          payload["executions"] = 0
          payload["exception_executions"] = {}
          result = self.class.collection.update_one(
            { _id: bson_id, state: "failed" },
            {
              "$set" => { arguments: ActiveSupport::JSON.encode(payload) },
              "$unset" => { state: true, error: true, finished_at: true, process_id: true, claim_token: true, claimed_at: true }
            },
            **SolidQueue::Mongo.session_options
          )
          if result.modified_count == 1
            reload
            job.prepare_for_execution
            retried = true
          end
        end
      end
      retried
    end

    def exception_class
      error_hash["exception_class"]
    end

    def message
      error_hash["message"]
    end

    def backtrace
      error_hash["backtrace"]
    end

    private
      def error_hash
        (error || {}).with_indifferent_access
      end
  end
end
