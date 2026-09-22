# frozen_string_literal: true

module SolidQueue
  class ReadyExecution < Execution
    class AmbiguousClaimError < Processes::UnrecoverableError
      def initialize(process_id:, token:, cause:)
        super("Claim outcome for process #{process_id} and token #{token} is unresolved: #{cause.class}: #{cause.message}")
        set_backtrace(cause.backtrace) if cause.backtrace
      end
    end

    class << self
      def claim(queue_list, limit, process_id)
        process_bson_id = SolidQueue::Mongo.id!(process_id)
        claimed = []
        candidates_seen = 0

        SolidQueue.instrument(:claim, process_id: process_id.to_s, job_ids: []) do |payload|
          selectors_for(queue_list).each do |queue_name|
            break if limit <= 0

            executions, seen = claim_from_queue(queue_name, limit, process_bson_id)
            candidates_seen += seen
            claimed.concat(executions)
            limit -= executions.size
          end

          payload[:candidates] = candidates_seen
          payload[:candidates_seen] = candidates_seen
          payload[:size] = claimed.size
          payload[:claimed] = claimed.size
          payload[:job_ids] = claimed.map(&:job_id)
          payload[:claimed_job_ids] = payload[:job_ids]
        end
        claimed
      end

      def claiming(job_ids, process_id)
        ids = Array(job_ids).map { |id| SolidQueue::Mongo.id!(id) }
        token = BSON::ObjectId.new
        process_bson_id = SolidQueue::Mongo.id!(process_id)
        error = nil

        begin
          collection.update_many(
            { _id: { "$in" => ids }, state: "ready" },
            claim_update(process_bson_id, token),
            **SolidQueue::Mongo.session_options
          )
        rescue *SolidQueue::Mongo::DRIVER_ERRORS => caught
          error = caught
        end

        claimed = resolve_claim_outcome(token, process_bson_id, error, ids.size)
        yield claimed if block_given?
        claimed
      end

      def aggregated_count_across(queue_list)
        selectors_for(queue_list).sum do |queue_name|
          filter = { state: "ready" }
          filter[:queue_name] = queue_name if queue_name
          collection.count_documents(filter, **SolidQueue::Mongo.session_options)
        end
      end

      def queued_as(queue_name)
        collection.find({ state: "ready", queue_name: queue_name.to_s }, **SolidQueue::Mongo.session_options).map { |doc| from_document(doc) }
      end

      private
        def selectors_for(queue_list)
          selector = QueueSelector.new(queue_list, self)
          selector.filters
        end

        MAX_IN_FLIGHT_CANDIDATES = 10_000

        def claim_from_queue(queue_name, limit, process_id)
          candidate_ids = reserve_candidates(queue_name, limit)
          return [ [], 0 ] if candidate_ids.empty?

          token = BSON::ObjectId.new
          error = nil
          begin
            begin
              collection.update_many(
                { _id: { "$in" => candidate_ids }, state: "ready" },
                claim_update(process_id, token),
                **SolidQueue::Mongo.session_options
              )
            rescue *SolidQueue::Mongo::DRIVER_ERRORS => caught
              # Acknowledgement loss must never replay a non-retryable update.
              error = caught
            end

            claimed = resolve_claim_outcome(token, process_id, error, candidate_ids.size)
            [ claimed, candidate_ids.size ]
          ensure
            release_candidates(candidate_ids)
          end
        end

        def reserve_candidates(queue_name, limit)
          claim_lock.synchronize do
            available = MAX_IN_FLIGHT_CANDIDATES - in_flight_candidates.size
            next [] unless available.positive?

            candidate_limit = [ limit, available ].min
            candidate_filter = { state: "ready" }
            candidate_filter[:queue_name] = queue_name if queue_name
            candidate_filter[:_id] = { "$nin" => in_flight_candidates.keys } if in_flight_candidates.any?
            ids = collection.find(
              candidate_filter,
              **SolidQueue::Mongo.session_options
            ).sort(priority: 1, _id: 1).limit(candidate_limit).batch_size(candidate_limit)
              .projection(_id: 1).map { |row| row["_id"] }
            ids.each { |id| in_flight_candidates[id] = true }
            ids
          end
        end

        def release_candidates(ids)
          claim_lock.synchronize { ids.each { |id| in_flight_candidates.delete(id) } }
        end

        def claim_lock
          if @claim_lock_pid != ::Process.pid
            @claim_lock = Mutex.new
            @in_flight_candidates = {}
            @claim_lock_pid = ::Process.pid
          end
          @claim_lock
        end

        def in_flight_candidates
          @in_flight_candidates
        end

        def resolve_claim_outcome(token, process_id, write_error, limit)
          claimed = claimed_by_token(token, process_id, limit)
          return claimed unless write_error

          # A server response (including a write-concern failure) has a stable
          # result that token readback can resolve. A transport failure does
          # not: the update may still be running after this read. Stop this
          # worker so process removal and orphan recovery fence late claims.
          return claimed if write_error.respond_to?(:result) && write_error.result

          raise AmbiguousClaimError.new(process_id: process_id, token: token, cause: write_error)
        rescue *SolidQueue::Mongo::DRIVER_ERRORS => read_error
          cause = write_error || read_error
          raise AmbiguousClaimError.new(process_id: process_id, token: token, cause: cause)
        end

        def claim_update(process_id, token)
          {
            "$set" => {
              state: "claimed",
              process_id: process_id,
              claim_token: token,
              claimed_at: Time.current
            },
            "$inc" => { claim_generation: 1 }
          }
        end

        def claimed_by_token(token, process_id, limit)
          collection.find(
            { state: "claimed", process_id: process_id, claim_token: token },
            **SolidQueue::Mongo.session_options
          ).sort(priority: 1, _id: 1).limit(limit).batch_size(limit).map { |doc| ClaimedExecution.from_document(doc) }
        end
    end
  end
end
