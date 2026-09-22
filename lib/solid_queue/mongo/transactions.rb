# frozen_string_literal: true

require "timeout"

module SolidQueue
  module Mongo
    TRANSIENT_TRANSACTION_ERROR = "TransientTransactionError"
    UNKNOWN_COMMIT_RESULT = "UnknownTransactionCommitResult"

    class TransactionDeadlineExceeded < StandardError
      attr_reader :operation, :attempt, :cause

      def initialize(operation:, attempt:, cause:)
        @operation = operation
        @attempt = attempt
        @cause = cause
        super("MongoDB transaction #{operation.inspect} exceeded its retry deadline after #{attempt} attempts: #{cause.message}")
        set_backtrace(cause.backtrace)
      end
    end

    class << self
      def transaction(operation:)
        raise ArgumentError, "operation is required" if operation.nil? || operation.to_s.empty?

        if current_session && transaction_in_progress?(current_session)
          return yield current_session
        end

        if current_session
          run_owned_transaction(current_session, operation.to_s) { |session| yield session }
        else
          client.start_session do |session|
            previous = context.dup
            context[:session] = session
            context[:client] = client
            run_owned_transaction(session, operation.to_s) { |owned_session| yield owned_session }
          ensure
            self.context = previous if previous
          end
        end
      end

      def track_transaction_record(record)
        records = context[:transaction_records]
        return unless records

        records[record.__id__] ||= [ record, record.mongo_transaction_snapshot ]
      end


      private
        def run_owned_transaction(session, operation)
          deadline = monotonic_now + transaction_timeout
          attempt = 0
          committed_result = nil
          committed_callbacks = nil

          loop do
            attempt += 1
            callbacks = []
            records = {}
            previous_owned = context[:owned_transaction]
            previous_callbacks = context[:after_commit]
            previous_records = context[:transaction_records]
            previous_deadline = context[:deadline]
            begin
              timeout_ms = remaining_timeout_ms(deadline, operation, attempt)
              session.start_transaction(
                read_concern: { level: :snapshot },
                write_concern: { w: :majority },
                timeout_ms: timeout_ms,
                max_commit_time_ms: timeout_ms
              )
              context[:owned_transaction] = true
              context[:after_commit] = callbacks
              context[:transaction_records] = records
              context[:deadline] = deadline
              result = yield session
              ensure_before_deadline!(deadline, operation, attempt)
              commit_with_retry(session, operation, attempt, deadline)
              committed_result = result
              committed_callbacks = callbacks
              break
            rescue Exception => error
              abort_transaction(session)
              restore_transaction_records(records)
              raise if error.is_a?(TransactionDeadlineExceeded)
              if retryable_label?(error, TRANSIENT_TRANSACTION_ERROR)
                record_retry(operation, TRANSIENT_TRANSACTION_ERROR, attempt, error, deadline)
                next
              elsif monotonic_now >= deadline && (driver_error?(error) || error.is_a?(::Timeout::Error))
                raise_deadline(operation, attempt, cause: error)
              end
              raise
            ensure
              context[:owned_transaction] = previous_owned
              context[:after_commit] = previous_callbacks
              context[:transaction_records] = previous_records
              context[:deadline] = previous_deadline
            end
          end

          # Callback failures happen after a known commit and must never retry
          # the already-committed transaction.
          committed_callbacks.each(&:call)
          committed_result
        end

        def commit_with_retry(session, operation, transaction_attempt, deadline)
          commit_attempt = 0
          loop do
            commit_attempt += 1
            ensure_before_deadline!(deadline, operation, commit_attempt, transaction_attempt: transaction_attempt, phase: :commit)
            session.commit_transaction(timeout_ms: remaining_timeout_ms(deadline, operation, commit_attempt))
            return
          rescue Exception => error
            raise if error.is_a?(TransactionDeadlineExceeded)
            if retryable_label?(error, UNKNOWN_COMMIT_RESULT) || error.is_a?(::Mongo::Error::TimeoutError)
              record_retry(operation, UNKNOWN_COMMIT_RESULT, commit_attempt, error, deadline,
                transaction_attempt: transaction_attempt, phase: :commit)
              next
            elsif monotonic_now >= deadline
              raise_deadline(operation, commit_attempt, transaction_attempt: transaction_attempt,
                phase: :commit, cause: error)
            end
            raise
          end
        end

        def record_retry(operation, label, attempt, error, deadline, transaction_attempt: attempt, phase: :transaction)
          if monotonic_now >= deadline
            SolidQueue.instrument(:transaction_retry,
              operation: operation, error_label: label, attempt: attempt,
              transaction_attempt: transaction_attempt, phase: phase, deadline_exceeded: true, error: error)
            raise TransactionDeadlineExceeded.new(operation: operation, attempt: attempt, cause: error)
          end

          SolidQueue.instrument(:transaction_retry,
            operation: operation, error_label: label, attempt: attempt,
            transaction_attempt: transaction_attempt, phase: phase, deadline_exceeded: false, error: error)
        end

        def remaining_timeout_ms(deadline, operation, attempt)
          remaining = ((deadline - monotonic_now) * 1000).floor
          return remaining if remaining.positive?

          raise_deadline(operation, attempt)
        end

        def ensure_before_deadline!(deadline, operation, attempt, transaction_attempt: attempt, phase: :transaction)
          return if monotonic_now < deadline

          raise_deadline(operation, attempt, transaction_attempt: transaction_attempt, phase: phase)
        end

        def raise_deadline(operation, attempt, transaction_attempt: attempt, phase: :transaction, cause: nil)
          error = cause || Timeout::Error.new("MongoDB transaction retry deadline exceeded")
          SolidQueue.instrument(:transaction_retry,
            operation: operation, error_label: "DeadlineExceeded", attempt: attempt,
            transaction_attempt: transaction_attempt, phase: phase, deadline_exceeded: true, error: error)
          raise TransactionDeadlineExceeded.new(operation: operation, attempt: attempt, cause: error)
        end

        def retryable_label?(error, label)
          while error
            return true if error.respond_to?(:label?) && error.label?(label)
            return true if error.respond_to?(:has_error_label?) && error.has_error_label?(label)
            return true if error.respond_to?(:labels) && error.labels.include?(label)

            error = error.cause
          end
          false
        end

        def restore_transaction_records(records)
          records.each_value { |record, snapshot| record.restore_mongo_transaction_snapshot(snapshot) }
        end

        def abort_transaction(session)
          session.abort_transaction(timeout_ms: 100) if transaction_in_progress?(session)
        rescue *DRIVER_ERRORS
          nil
        end

        def driver_error?(error)
          DRIVER_ERRORS.any? { |type| error.is_a?(type) }
        end

        def transaction_timeout
          timeout = SolidQueue.mongo_transaction_timeout if SolidQueue.respond_to?(:mongo_transaction_timeout)
          Float(timeout || 5)
        end

        def monotonic_now
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end
    end
  end
end
