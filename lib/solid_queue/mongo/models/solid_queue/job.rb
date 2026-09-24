# frozen_string_literal: true

module SolidQueue
  class Job < Record
    collection_name :jobs

    class EnqueueError < StandardError; end

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
    field :batch_id
    field :delivery_mode
    field :state
    field :process_id
    field :claim_token
    field :claim_generation, default: 0
    field :claimed_at
    field :started_at
    field :timeout_at
    field :expires_at
    field :error

    class << self
      def enqueue(active_job, scheduled_at: Time.current)
        active_job.scheduled_at = scheduled_at

        now = Time.current
        attributes = attributes_from_active_job(active_job).merge(_id: BSON::ObjectId.new, created_at: now)
        job = if attributes[:concurrency_key].blank? && attributes[:batch_id].nil?
          attributes[:state] = scheduled_at > now ? "scheduled" : "ready"
          ensure_enqueue_document_size!(attributes)
          create!(attributes)
        else
          transaction(operation: "enqueue_job") do
            created = new(attributes)
            batch_all([ created ])
            if !created.due?
              created.state = "scheduled"
            elsif !created.concurrency_limited? || Semaphore.wait(created)
              created.state = "ready"
            elsif created.concurrency_on_conflict.discard?
              BatchExecution.complete(created) if created.batched?
              next created
            else
              created.state = "blocked"
              created.expires_at = now + created.concurrency_duration
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

        job
      rescue EnqueueError, SolidQueue::PersistenceError, SolidQueue::Mongo::TransactionDeadlineExceeded, *SolidQueue::Mongo::DRIVER_ERRORS => error
        active_job.successfully_enqueued = false
        raise_enqueue_error(error)
      end

      def enqueue_all(active_jobs)
        current_batch_id = Batch.current_batch_id
        now = Time.current
        jobs = []

        transaction(operation: "enqueue_jobs") do
          documents = active_jobs.map do |active_job|
            active_job.scheduled_at ||= now
            active_job.batch_id = current_batch_id || active_job.batch_id
            attributes_from_active_job(active_job).merge(_id: BSON::ObjectId.new, created_at: now).tap do |document|
              if active_job.scheduled_at > now
                document[:state] = "scheduled"
              elsif document[:concurrency_key].blank?
                document[:state] = "ready"
              end
              ensure_enqueue_document_size!(document)
            end
          end

          unless documents.empty?
            collection.insert_many(documents, **SolidQueue::Mongo.session_options)
            jobs = documents.map { |document| from_document(document) }
            batch_all(jobs)
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

          end
        end
        active_jobs.count(&:successfully_enqueued?)
      rescue EnqueueError, SolidQueue::PersistenceError, SolidQueue::Mongo::TransactionDeadlineExceeded, *SolidQueue::Mongo::DRIVER_ERRORS => error
        active_jobs.each { |job| job.successfully_enqueued = false }
        raise_enqueue_error(error)
      end

      def prepare_all_for_execution(jobs)
        due, not_yet_due = jobs.partition(&:due?)
        dispatch_all(due) + schedule_all(not_yet_due)
      end

      def dispatch_all(jobs)
        with_concurrency_limits, without_concurrency_limits = jobs.partition(&:concurrency_limited?)

        dispatch_all_at_once(without_concurrency_limits)
        dispatch_all_one_by_one(with_concurrency_limits)

        successfully_dispatched(jobs)
      end

      def schedule_all(jobs)
        schedule_all_at_once(jobs)
        successfully_scheduled(jobs)
      end

      def batch_all(jobs)
        BatchExecution.create_all_from_jobs(jobs) if Batch.migrated?
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

      def ready_metrics(queue_name)
        filter = { state: "ready", queue_name: queue_name.to_s }
        first = collection.find(filter, **SolidQueue::Mongo.session_options).sort(created_at: 1).limit(1).first
        { size: collection.count_documents(filter, **SolidQueue::Mongo.session_options), oldest_created_at: first && first["created_at"] }
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
        def dispatch_all_at_once(jobs)
          ReadyExecution.create_all_from_jobs(jobs)
        end

        def dispatch_all_one_by_one(jobs)
          jobs.each(&:dispatch)
        end

        def successfully_dispatched(jobs)
          refresh_existing(jobs).select { |job| job.ready? || job.blocked? }
        end

        def schedule_all_at_once(jobs)
          ScheduledExecution.create_all_from_jobs(jobs)
        end

        def successfully_scheduled(jobs)
          refresh_existing(jobs).select(&:scheduled?)
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
            batch_id: active_job.batch_id.present? ? SolidQueue::Mongo.id!(active_job.batch_id) : nil,
            delivery_mode: ActiveJob::DeliveryModes.mode!(active_job.try(:delivery_mode) || SolidQueue.default_delivery_mode).to_s
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

    def job_class
      @job_class ||= class_name.safe_constantize
    end

    def concurrency_limit
      job_class&.concurrency_limit
    end

    def run_time_limit
      limits = [ job_class.try(:run_time_limit), SolidQueue.max_run_time ]
      limits << SolidQueue.exactly_once_timeout if exactly_once?
      limits.compact.min
    end

    def delivery_mode
      (@attributes[:delivery_mode].presence || job_class.try(:delivery_mode) || SolidQueue.default_delivery_mode).to_sym
    end

    def exactly_once?
      delivery_mode == :exactly_once
    end

    def at_most_once?
      delivery_mode == :at_most_once
    end

    def display_name
      @display_name ||= custom_display_name || class_name
    end

    def concurrency_duration
      job_class&.concurrency_duration
    end

    def concurrency_on_conflict
      (job_class&.concurrency_on_conflict).to_s.inquiry
    end

    def prepare_for_execution
      due? ? dispatch : schedule
    end

    def dispatch
      transaction(operation: "dispatch_job") do
        reload
        next false unless [ nil, "scheduled", "failed" ].include?(state)

        if acquire_concurrency_lock
          raise DispatchConflict unless ready
          "ready"
        else
          handle_concurrency_conflict
        end
      end
    rescue DispatchConflict
      false
    end

    def dispatch_bypassing_concurrency_limits
      ready
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
        BatchExecution.complete(self) if batched?
        unless SolidQueue.preserve_finished_jobs?
          self.class.delete_recurring_markers([ bson_id ])
          self.class.collection.delete_one({ _id: bson_id, state: "finished" }, **SolidQueue::Mongo.session_options)
          SolidQueue::Mongo.after_commit { unblock_next_blocked_job } if previous_state == "ready" && concurrency_limited?
        end
      end
      @persisted = false unless SolidQueue.preserve_finished_jobs?
      self
    end

    def failed_with(exception)
      transaction(operation: "fail_job") do
        result = self.class.collection.update_one(
          { _id: bson_id, state: { "$ne" => "claimed" } },
          { "$set" => { state: "failed", error: FailedExecution.error_from(exception), finished_at: Time.current }, "$unset" => claim_unsets },
          **SolidQueue::Mongo.session_options
        )
        raise SolidQueue::RecordNotFound, "Job #{id} cannot be failed" unless result.modified_count == 1
        reload
        BatchExecution.complete(self) if batched?
      end
      FailedExecution.from_document(attributes)
    end

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
      release_concurrency_lock.tap do |released|
        release_next_blocked_job if released
      end
    end

    private
      def acquire_concurrency_lock
        return true unless concurrency_limited?

        Semaphore.wait(self)
      end

      def release_concurrency_lock
        return false unless concurrency_limited?

        Semaphore.signal(self)
      end

      def handle_concurrency_conflict
        if concurrency_on_conflict.discard?
          destroy_pending!
          false
        else
          block
        end
      end

      def block
        BlockedExecution.block(self)
        reload.state
      end

      def release_next_blocked_job
        BlockedExecution.release_one(concurrency_key)
      end

      def ready
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

      def destroy_pending!
        transaction(operation: "discard_conflicted_job") do
          BatchExecution.complete(self) if batched?
          self.class.collection.delete_one({ _id: bson_id, state: { "$ne" => "claimed" } }, **SolidQueue::Mongo.session_options)
          self.class.delete_recurring_markers([ bson_id ])
        end
      end

      def claim_unsets
        { process_id: true, claim_token: true, claimed_at: true, started_at: true, timeout_at: true }
      end

      def custom_display_name
        return unless job_class.is_a?(Class) && job_class.method_defined?(:display_name)

        active_job = ActiveJob::Base.deserialize(arguments)
        active_job.arguments = ActiveJob::Arguments.deserialize(arguments.fetch("arguments", []))
        active_job.display_name.presence&.to_s
      rescue StandardError
        nil
      end

      def terminal_unsets
        claim_unsets.merge(error: true, finished_at: true)
      end
  end
end
