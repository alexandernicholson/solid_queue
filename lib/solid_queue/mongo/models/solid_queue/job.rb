# frozen_string_literal: true

require "active_job/enqueuing"

module SolidQueue
  class Job < Record
    collection_name :jobs

    class EnqueueError < StandardError; end
    class DuplicateError < ActiveJob::EnqueueError; end

    class ClassMissingError < NameError
      def self.for(job)
        new("Job class #{job.class_name.inspect} could not be resolved")
      end
    end


    class DispatchConflict < StandardError; end
    DEFAULT_PRIORITY = 0
    DEFAULT_QUEUE_NAME = "default"
    STATES = %w[ready scheduled blocked claimed failed finished].freeze
    BSON_MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
    LIFECYCLE_RESERVE_BYTES = 128 * 1024
    MAX_ENQUEUE_DOCUMENT_BYTES = BSON_MAX_DOCUMENT_BYTES - LIFECYCLE_RESERVE_BYTES

    field :queue_name, default: DEFAULT_QUEUE_NAME
    field :priority, default: DEFAULT_PRIORITY
    field :active_job_id
    field :class_name
    field :arguments, default: "{}"
    field :scheduled_at
    field :created_at
    field :finished_at
    field :concurrency_key
    field :deduplication_key
    field :batch_id
    field :state
    field :process_id
    field :claim_token
    field :claim_generation, default: 0
    field :claimed_at
    field :started_at
    field :expires_at
    field :error

    class << self
      def enqueue(active_job, scheduled_at: Time.current)
        active_job.scheduled_at = scheduled_at

        attributes = attributes_from_active_job(active_job).merge(_id: BSON::ObjectId.new, created_at: Time.current)
        job = if attributes[:concurrency_key].blank? && attributes[:batch_id].nil? && attributes[:deduplication_key].nil?
          attributes[:state] = scheduled_at > Time.current ? "scheduled" : "ready"
          ensure_enqueue_document_size!(attributes)
          create!(attributes)
        else
          transaction(operation: "enqueue_job") do
            created = new(attributes)
            next created.tap(&:duplicate!) unless Deduplication.reserve(created, active_job)

            BatchExecution.create_all_from_jobs([ created ]) if batch_tracking?
            if !created.due?
              created.state = "scheduled"
            elsif !created.concurrency_limited? || Semaphore.wait(created)
              created.state = "ready"
            elsif created.concurrency_on_conflict.to_s == "discard"
              BatchExecution.complete(created) if created.batched?
              Deduplication.release([ created ], windowed: true) if created.deduplicated?
              next created
            else
              created.state = "blocked"
              created.expires_at = created.concurrency_duration.from_now
            end
            pending_document = attributes.merge(state: created.state)
            pending_document[:expires_at] = created.expires_at if created.expires_at
            ensure_enqueue_document_size!(pending_document)
            created.save!
            created
          end
        end

        active_job.provider_job_id = job.id if job.persisted?
        active_job.successfully_enqueued = job.persisted?
        raise DuplicateError, "#{active_job.class.name} is already enqueued as #{active_job.deduplication_key}" if job.duplicate?
        job
      rescue EnqueueError, SolidQueue::PersistenceError, SolidQueue::Mongo::TransactionDeadlineExceeded, *SolidQueue::Mongo::DRIVER_ERRORS => error
        active_job.successfully_enqueued = false
        raise_enqueue_error(error)
      end

      def enqueue_all(active_jobs)
        current_batch_id = Batch.current_batch_id
        now = Time.current
        jobs = []
        duplicates = []

        transaction(operation: "enqueue_jobs") do
          duplicates = []
          documents = active_jobs.filter_map do |active_job|
            active_job.scheduled_at ||= now
            active_job.batch_id = current_batch_id || active_job.batch_id
            document = attributes_from_active_job(active_job).merge(_id: BSON::ObjectId.new, created_at: now)
            unless Deduplication.reserve(from_document(document), active_job)
              duplicates << active_job
              next
            end

            if active_job.scheduled_at > now
              document[:state] = "scheduled"
            elsif document[:concurrency_key].blank?
              document[:state] = "ready"
            end
            ensure_enqueue_document_size!(document)
            document
          end

          unless documents.empty?
            collection.insert_many(documents, **SolidQueue::Mongo.session_options)
            jobs = documents.map { |document| from_document(document) }
            BatchExecution.create_all_from_jobs(jobs) if batch_tracking?
            pending = jobs.select { |job| job.state.nil? }
            pending.each(&:dispatch)
            refresh_existing(pending)
          end
        end

        jobs_by_active_id = jobs.select(&:persisted?).index_by(&:active_job_id)
        active_jobs.each do |active_job|
          if (job = jobs_by_active_id[active_job.job_id])
            active_job.provider_job_id = job.id
            active_job.successfully_enqueued = true
          else
            active_job.successfully_enqueued = false
            active_job.enqueue_error = DuplicateError.new("#{active_job.class.name} is already enqueued as #{active_job.deduplication_key}") if duplicates.include?(active_job)
          end
        end
        active_jobs.count(&:successfully_enqueued?)
      rescue EnqueueError, SolidQueue::PersistenceError, SolidQueue::Mongo::TransactionDeadlineExceeded, *SolidQueue::Mongo::DRIVER_ERRORS => error
        active_jobs.each { |job| job.successfully_enqueued = false }
        raise_enqueue_error(error)
      end

      def prepare_all_for_execution(jobs)
        due, future = jobs.partition(&:due?)
        dispatch_all(due)
        future.each(&:schedule)
        refresh_existing(jobs).select { |job| %w[ready blocked scheduled].include?(job.state) }
      end

      def dispatch_all(jobs)
        without_limit, with_limit = jobs.partition { |job| !job.concurrency_limited? }
        ReadyExecution.create_all_from_jobs(without_limit)
        with_limit.each(&:dispatch)
        refresh_existing(jobs).select { |job| %w[ready blocked].include?(job.state) }
      end

      def schedule_all(jobs)
        Array(jobs).each(&:schedule)
        refresh_existing(jobs).select(&:scheduled?)
      end

      def release_all_concurrency_locks(jobs)
        Semaphore.signal_all(Array(jobs).select(&:concurrency_limited?))
      end

      def find(id)
        super(SolidQueue::Mongo.id!(id))
      end

      def find_by_active_job_id(active_job_id)
        find_by(active_job_id: active_job_id)
      end

      def find_many(ids)
        bson_ids = Array(ids).map { |id| SolidQueue::Mongo.id!(id) }
        collection.find({ _id: { "$in" => bson_ids } }, **SolidQueue::Mongo.session_options).map { |document| from_document(document) }
      end

      def to_bson_id(id)
        SolidQueue::Mongo.id!(id)
      end

      def distinct_queue_names(state: nil)
        filter = state ? { state: state.to_s } : {}
        collection.find(filter, **SolidQueue::Mongo.session_options).distinct(:queue_name)
      end

      def ready_metrics(queue_name)
        filter = { state: "ready", queue_name: queue_name.to_s }
        first = collection.find(filter, **SolidQueue::Mongo.session_options).sort(created_at: 1).limit(1).first
        { size: collection.count_documents(filter, **SolidQueue::Mongo.session_options), oldest_created_at: first && first["created_at"] }
      end

      def discard_ready_in_queue(queue_name, batch_size: 500)
        Execution.discard_matching({ state: "ready", queue_name: queue_name.to_s }, batch_size: batch_size)
      end


      def refresh_existing(jobs)
        jobs = Array(jobs)
        return jobs if jobs.empty?

        documents = collection.find(
          { _id: { "$in" => jobs.map(&:bson_id) } }, **SolidQueue::Mongo.session_options
        ).to_a.index_by { |document| document.fetch("_id") }
        jobs.each do |job|
          if (document = documents[job.bson_id])
            job.assign_attributes(document)
          else
            job.instance_variable_set(:@persisted, false)
          end
        end
      end
      def clear_finished_in_batches(batch_size: 500, finished_before: SolidQueue.clear_finished_jobs_after.ago, class_name: nil, sleep_between_batches: 0)
        filter = { state: "finished", finished_at: { "$lt" => finished_before } }
        filter[:class_name] = class_name if class_name.present?
        loop do
          ids = collection.find(filter, **SolidQueue::Mongo.session_options).projection(_id: 1).limit(batch_size).map { |row| row["_id"] }
          break if ids.empty?
          transaction(operation: "clear_finished_jobs") do
            delete_recurring_markers(ids)
            collection.delete_many({ _id: { "$in" => ids }, state: "finished" }, **SolidQueue::Mongo.session_options)
          end
          sleep(sleep_between_batches) if sleep_between_batches.positive?
        end
      end

      def delete_recurring_markers(ids)
        RecurringExecution.collection.delete_many(
          { job_id: { "$in" => Array(ids).map { |id| SolidQueue::Mongo.id!(id) } } },
          **SolidQueue::Mongo.session_options
        )
      end

      private
        def batch_tracking?
          Batch.migrated?
        end

        def attributes_from_active_job(active_job)
          {
            queue_name: active_job.queue_name.presence || DEFAULT_QUEUE_NAME,
            active_job_id: active_job.job_id,
            priority: active_job.priority || DEFAULT_PRIORITY,
            scheduled_at: active_job.scheduled_at,
            class_name: active_job.class.name,
            arguments: ActiveSupport::JSON.encode(active_job.serialize),
            concurrency_key: active_job.concurrency_key,
            deduplication_key: active_job.try(:deduplication_key),
            batch_id: active_job.batch_id.present? ? SolidQueue::Mongo.id!(active_job.batch_id) : nil
          }.compact
        end

        def ensure_enqueue_document_size!(document)
          estimated_size = bson_size_upper_bound(document)
          return if estimated_size && estimated_size <= MAX_ENQUEUE_DOCUMENT_BYTES

          bytesize = BSON::Document.new(document).to_bson.to_s.bytesize
          return if bytesize <= MAX_ENQUEUE_DOCUMENT_BYTES

          raise EnqueueError,
            "Serialized job is #{bytesize} bytes; MongoDB enqueue limit is #{MAX_ENQUEUE_DOCUMENT_BYTES} bytes"
        end

        def bson_size_upper_bound(value)
          case value
          when Hash
            value.sum(5) do |key, child|
              child_size = bson_size_upper_bound(child)
              return unless child_size
              2 + key.to_s.bytesize + child_size
            end
          when Array
            value.each_with_index.sum(5) do |child, index|
              child_size = bson_size_upper_bound(child)
              return unless child_size
              2 + index.to_s.bytesize + child_size
            end
          when String
            5 + value.bytesize
          when Integer, Float, Time
            8
          when BSON::ObjectId
            12
          when true, false
            1
          when nil
            0
          else
            value.respond_to?(:acts_like?) && value.acts_like?(:time) ? 8 : nil
          end
        end

        def raise_enqueue_error(error)
          raise error if error.is_a?(EnqueueError)
          if (transient = SolidQueue::Mongo.transient_error_for_caller_transaction(error))
            raise transient
          end

          raise enqueue_error(error)
        end

        def enqueue_error(error)
          EnqueueError.new("#{error.class.name}: #{error.message}").tap do |wrapped|
            wrapped.set_backtrace(error.backtrace)
          end
        end
    end

    def arguments
      ActiveSupport::JSON.decode(@attributes[:arguments])
    end

    def arguments=(payload)
      track_mongo_transaction_state
      @attributes[:arguments] = payload.is_a?(String) ? payload : ActiveSupport::JSON.encode(payload)
    end

    def batched?
      Batch.migrated? && batch_id.present?
    end

    def batch
      Batch.find(batch_id) if batched?
    end

    def to_bson_id
      bson_id
    end

    def batch_id
      value = attributes["batch_id"] || attributes[:batch_id]
      value&.to_s
    end

    def process_id
      value = attributes["process_id"] || attributes[:process_id]
      value&.to_s
    end

    def due?
      scheduled_at.nil? || scheduled_at <= Time.current
    end

    def concurrency_limited?
      concurrency_key.present? && job_class.present?
    end

    def deduplicated?
      deduplication_key.present?
    end

    def duplicate!
      @duplicate = true
    end

    def duplicate?
      @duplicate == true
    end

    def job_class
      @job_class ||= class_name.safe_constantize
    end

    def concurrency_limit
      job_class&.concurrency_limit
    end

    def concurrency_duration
      job_class&.concurrency_duration
    end

    def concurrency_on_conflict
      job_class&.concurrency_on_conflict
    end

    def prepare_for_execution
      due? ? dispatch : schedule
    end

    def dispatch
      transaction(operation: "dispatch_job") do
        reload
        next false unless [ nil, "scheduled", "failed" ].include?(state)

        if !concurrency_limited? || Semaphore.wait(self)
          raise DispatchConflict unless dispatch_bypassing_concurrency_limits
          "ready"
        elsif concurrency_on_conflict.to_s == "discard"
          destroy_pending!
          false
        else
          BlockedExecution.block(self)
          reload.state
        end
      end
    rescue DispatchConflict
      false
    end

    def dispatch_bypassing_concurrency_limits
      result = self.class.collection.update_one(
        { _id: bson_id, state: { "$in" => [ nil, "scheduled", "failed" ] } },
        { "$set" => { state: "ready" }, "$unset" => terminal_unsets },
        **SolidQueue::Mongo.session_options
      )
      reload
      result.modified_count == 1 || state == "ready"
    end

    def schedule
      ScheduledExecution.schedule(self)
      reload.state
    end

    def transition_to!(new_state, from:, set: {}, unset: {})
      update = { "$set" => { state: new_state.to_s }.merge(set) }
      update["$unset"] = unset.index_with { true } if unset.any?
      result = self.class.collection.update_one(
        { _id: bson_id, state: { "$in" => Array(from).map(&:to_s) + (Array(from).include?(nil) ? [ nil ] : []) } },
        update,
        **SolidQueue::Mongo.session_options
      )
      reload
      result.modified_count == 1
    end

    def finished!
      previous_state = nil
      transaction(operation: "finish_job") do
        previous = self.class.collection.find_one_and_update(
          { _id: bson_id, state: { "$ne" => "claimed" } },
          { "$set" => { state: "finished", finished_at: Time.current }, "$unset" => claim_unsets.merge(error: true) },
          return_document: :before,
          **SolidQueue::Mongo.session_options
        )
        raise SolidQueue::RecordNotFound, "Job #{id} cannot be finished" unless previous

        previous_state = previous["state"] || previous[:state]
        reload
        BatchExecution.complete(self) if batch_tracking?
        Deduplication.release([ self ]) if deduplicated?
        unless SolidQueue.preserve_finished_jobs?
          self.class.delete_recurring_markers([ bson_id ])
          self.class.collection.delete_one({ _id: bson_id, state: "finished" }, **SolidQueue::Mongo.session_options)
          SolidQueue::Mongo.after_commit { unblock_next_blocked_job } if previous_state == "ready" && concurrency_limited?
        end
      end
      @persisted = false unless SolidQueue.preserve_finished_jobs?
      self
    end

    def fail_with(exception)
      transaction(operation: "fail_job") do
        result = self.class.collection.update_one(
          { _id: bson_id, state: { "$ne" => "claimed" } },
          { "$set" => { state: "failed", error: FailedExecution.error_from(exception), finished_at: Time.current }, "$unset" => claim_unsets },
          **SolidQueue::Mongo.session_options
        )
        raise SolidQueue::RecordNotFound, "Job #{id} cannot be failed" unless result.modified_count == 1
        reload
        BatchExecution.complete(self) if batch_tracking?
      end
      FailedExecution.from_document(attributes)
    end
    alias_method :failed_with, :fail_with

    def reset_execution_counters
      payload = arguments.deep_dup
      payload["executions"] = 0
      payload["exception_executions"] = {}
      update!(arguments: payload)
    end

    def finished?
      state == "finished"
    end

    def ready? = state == "ready"
    def claimed? = state == "claimed"
    def failed? = state == "failed"
    def scheduled? = state == "scheduled"
    def blocked? = state == "blocked"
    def status = state&.to_sym

    def retry
      failed_execution&.retry
    end

    def recurring_execution
      RecurringExecution.find_by(job_id: bson_id)
    end

    def ready_execution
      ReadyExecution.from_document(attributes) if ready?
    end

    def claimed_execution
      ClaimedExecution.from_document(attributes) if claimed?
    end

    def failed_execution
      FailedExecution.from_document(attributes) if failed?
    end

    def scheduled_execution
      ScheduledExecution.from_document(attributes) if scheduled?
    end

    def blocked_execution
      BlockedExecution.from_document(attributes) if blocked?
    end


    def discard
      execution_class = {
        "ready" => ReadyExecution,
        "scheduled" => ScheduledExecution,
        "blocked" => BlockedExecution,
        "claimed" => ClaimedExecution,
        "failed" => FailedExecution
      }[state]
      discarded = execution_class&.from_document(attributes)&.discard
      @persisted = false if discarded
      discarded
    end

    def unblock_next_blocked_job
      return false unless concurrency_limited?
      released = Semaphore.signal(self)
      BlockedExecution.release_for(concurrency_key) if released
      released
    end

    private
      def batch_tracking?
        self.class.send(:batch_tracking?) && batch_id.present?
      end

      def destroy_pending!
        transaction(operation: "discard_conflicted_job") do
          BatchExecution.complete(self) if batch_tracking?
          self.class.collection.delete_one({ _id: bson_id, state: { "$ne" => "claimed" } }, **SolidQueue::Mongo.session_options)
          self.class.delete_recurring_markers([ bson_id ])
          Deduplication.release([ self ], windowed: true) if deduplicated?
        end
      end

      def claim_unsets
        { process_id: true, claim_token: true, claimed_at: true, started_at: true }
      end

      def terminal_unsets
        claim_unsets.merge(error: true, finished_at: true)
      end
  end
end
