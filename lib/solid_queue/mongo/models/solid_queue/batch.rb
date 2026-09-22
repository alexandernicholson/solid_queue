# frozen_string_literal: true

module SolidQueue
  class Batch < Record
    class AlreadyFinished < StandardError; end

    class PendingMigrations < StandardError
      def initialize(message = "The MongoDB collections and indexes haven't been prepared. Run `bin/rails solid_queue:prepare`")
        super
      end
    end

    collection_name :batches

    field :active_job_batch_id
    field :description
    field :on_finish
    field :on_success
    field :on_failure
    field :metadata, default: {}
    field :total_jobs, default: 0
    field :completed_jobs, default: 0
    field :failed_jobs, default: 0
    field :enqueued_at
    field :finished_at
    field :failed_at
    field :created_at
    field :updated_at
    field :version, default: 0

    class << self
      def migrated?
        @migrated ||= begin
          names = SolidQueue::Mongo.client.database.collection_names
          names.include?(SolidQueue::Mongo.collection(:batches).name) &&
            names.include?(SolidQueue::Mongo.collection(:batch_executions).name)
        end
      rescue StandardError
        false
      end

      def warn_about_pending_migrations
        SolidQueue.deprecator.warn("Solid Queue MongoDB collections are not prepared. Run `bin/rails solid_queue:prepare`.")
      end

      def create!(attributes = {})
        raise PendingMigrations unless migrated?
        super
      end

      def enqueue(description: nil, on_success: nil, on_failure: nil, on_finish: nil, metadata: nil, **extra_metadata, &block)
        raise PendingMigrations unless migrated?

        new(
          description: description,
          on_success: on_success,
          on_failure: on_failure,
          on_finish: on_finish,
          metadata: (metadata || {}).merge(extra_metadata)
        ).tap { |batch| batch.enqueue(&block) }
      end

      def current_batch_id
        ActiveSupport::IsolatedExecutionState[:current_batch_id]
      end

      def wrap_in_batch_context(batch_id)
        previous_batch_id = current_batch_id.presence
        ActiveSupport::IsolatedExecutionState[:current_batch_id] = batch_id
        yield
      ensure
        ActiveSupport::IsolatedExecutionState[:current_batch_id] = previous_batch_id
      end

      def all
        collection.find({}, **SolidQueue::Mongo.session_options).sort(created_at: 1).map { |document| from_document(document) }
      end

      def finished
        matching(finished_at: { "$exists" => true, "$ne" => nil })
      end

      def succeeded
        matching(finished_at: { "$exists" => true, "$ne" => nil }, failed_at: nil)
      end

      def unfinished
        matching(finished_at: nil)
      end

      def failed
        matching(failed_at: { "$exists" => true, "$ne" => nil })
      end

      def enqueued
        matching(enqueued_at: { "$exists" => true, "$ne" => nil })
      end

      def count
        collection.count_documents({}, **SolidQueue::Mongo.session_options)
      end

      def last(count = nil)
        records = collection.find({}, **SolidQueue::Mongo.session_options).sort(created_at: -1).limit(count || 1).map { |document| from_document(document) }
        count ? records.reverse : records.first
      end

      def destroy_all
        deleted = 0
        SolidQueue::Mongo.transaction(operation: "destroy all batches") do
          SolidQueue::Mongo.collection(:batch_executions).delete_many({}, **SolidQueue::Mongo.session_options)
          deleted = collection.delete_many({}, **SolidQueue::Mongo.session_options).deleted_count
        end
        deleted
      end

      def clear_finished_in_batches(batch_size: 500, finished_before: SolidQueue.clear_finished_jobs_after.ago, sleep_between_batches: 0)
        loop do
          ids = collection.find(
            { finished_at: { "$lt" => finished_before }, failed_at: nil },
            projection: { _id: 1 },
            **SolidQueue::Mongo.session_options
          ).limit(batch_size).map { |document| document["_id"] || document[:_id] }
          break if ids.empty?

          SolidQueue::Mongo.transaction(operation: "clear finished batches") do
            collection.delete_many({ _id: { "$in" => ids } }, **SolidQueue::Mongo.session_options)
            SolidQueue::Mongo.collection(:batch_executions).delete_many({ batch_id: { "$in" => ids } }, **SolidQueue::Mongo.session_options)
          end
          sleep(sleep_between_batches) if sleep_between_batches > 0
        end
      end

      def sweep_stalled(stalled_for: 5.minutes, batch_size: 500)
        SolidQueue.instrument(:sweep_stalled_batches, stalled_for: stalled_for, stale_executions: 0, finished_batches: 0, started_batches: 0) do |payload|
          payload[:stale_executions] = BatchExecution.sweep_stale(batch_size: batch_size)
          payload[:finished_batches] = finish_stalled_batches(batch_size: batch_size)
          payload[:started_batches] = start_stalled_batches(stalled_for: stalled_for, batch_size: batch_size)
        end
      end

      private
        def matching(filter)
          collection.find(filter, **SolidQueue::Mongo.session_options).sort(created_at: 1).map { |document| from_document(document) }
        end

        def serialize_callback(value)
          return unless value.present?
          return value if value.is_a?(Hash)

          active_job = value.is_a?(ActiveJob::Base) ? value : value.new
          active_job.batch_id = nil
          active_job.serialize
        end

        def finish_stalled_batches(batch_size:)
          finished_count = 0
          last_id = nil
          loop do
            filter = { finished_at: nil, enqueued_at: { "$exists" => true, "$ne" => nil } }
            filter[:_id] = { "$gt" => last_id } if last_id
            documents = collection.find(filter, **SolidQueue::Mongo.session_options).sort(_id: 1).limit(batch_size).to_a
            break if documents.empty?

            last_id = documents.last.fetch("_id")
            documents.each do |document|
              batch = from_document(document)
              next if BatchExecution.outstanding_for_batch?(batch.bson_id)

              batch.finish
              finished_count += 1 if batch.reload.finished?
            end
          end
          finished_count
        end

        def start_stalled_batches(stalled_for:, batch_size:)
          started_count = 0
          collection.find(
            { finished_at: nil, enqueued_at: nil, created_at: { "$lt" => stalled_for.ago } },
            **SolidQueue::Mongo.session_options
          ).limit(batch_size).each do |document|
            from_document(document).start
            started_count += 1
          end
          started_count
        end
    end

    %i[on_finish on_success on_failure].each do |callback_name|
      define_method("#{callback_name}=") do |value|
        self[callback_name] = self.class.send(:serialize_callback, value)
      end
    end

    def save!
      creating = !persisted?
      if creating
        now = Time.current
        self.active_job_batch_id ||= SecureRandom.uuid
        self.created_at ||= now
        self.updated_at = now
      end

      super.tap do
        SolidQueue::Mongo.after_commit { start } if creating
      end
    end

    def enqueue(&block)
      raise PendingMigrations unless self.class.migrated?

      creating = !persisted?
      committed = false
      original_attributes = @attributes.deep_dup if creating
      original_bson_id = @bson_id if creating
      if !creating && self.class.collection.find({ _id: bson_id, finished_at: { "$ne" => nil } }, **SolidQueue::Mongo.session_options).first
        raise AlreadyFinished, "Can't enqueue an already finished batch"
      end

      SolidQueue::Mongo.transaction(operation: "enqueue batch") do
        SolidQueue::Mongo.after_commit { committed = true }
        if creating
          # save! mutates in-memory state before commit. Restore the original
          # snapshot before every retried attempt, then insert again.
          @attributes = original_attributes.deep_dup
          @bson_id = original_bson_id
          @persisted = false
          save!
        else
          result = self.class.collection.update_one(
            { _id: bson_id, finished_at: nil },
            { "$inc" => { version: 1 }, "$set" => { updated_at: Time.current } },
            **SolidQueue::Mongo.session_options
          )
          raise AlreadyFinished, "Can't enqueue an already finished batch" unless result.matched_count == 1
        end

        self.class.wrap_in_batch_context(id) { block&.call(self) }
      end
      self
    rescue
      if creating && !committed
        @attributes = original_attributes
        @bson_id = original_bson_id
        @persisted = false
      end
      raise
    end

    def start
      SolidQueue::Mongo.transaction(operation: "start batch") do
        self.class.collection.update_one(
          { _id: bson_id, enqueued_at: nil, finished_at: nil },
          { "$set" => { enqueued_at: Time.current, updated_at: Time.current }, "$inc" => { version: 1 } },
          **SolidQueue::Mongo.session_options
        )
        reload
        finish
      end
      self
    end

    def finish
      return self if finished? || !enqueued?

      SolidQueue::Mongo.transaction(operation: "finish batch") do
        # This marker read and the parent write share the transaction. Adders also
        # mutate the parent, so MongoDB's write-conflict retry prevents write skew.
        next if BatchExecution.outstanding_for_batch?(bson_id)

        now = Time.current
        result = self.class.collection.update_one(
          { _id: bson_id, finished_at: nil, enqueued_at: { "$exists" => true, "$ne" => nil } },
          { "$set" => { finished_at: now, updated_at: now }, "$inc" => { version: 1 } },
          **SolidQueue::Mongo.session_options
        )
        finalize if result.modified_count == 1
      end
      reload
    rescue
      reload if persisted?
      raise
    end

    def jobs
      SolidQueue::Mongo.collection(:jobs).find({ batch_id: bson_id }, **SolidQueue::Mongo.session_options).sort(created_at: 1).map do |document|
        Job.from_document(document)
      end
    end

    def batch_executions
      BatchExecution.for_batch(bson_id)
    end

    def status
      return failed? ? :failed : :completed if finished?
      enqueued? ? :enqueued : :pending
    end

    def failed?
      failed_at.present?
    end

    def succeeded?
      finished? && !failed?
    end

    def finished?
      finished_at.present?
    end

    def enqueued?
      enqueued_at.present?
    end

    def pending_jobs
      finished? ? 0 : BatchExecution.count_for_batch(bson_id)
    end

    def completed_jobs
      finished? ? self[:completed_jobs].to_i : [ total_jobs.to_i - pending_jobs - failed_jobs, 0 ].max
    end

    def failed_jobs
      return self[:failed_jobs].to_i if finished?

      SolidQueue::Mongo.collection(:jobs).distinct(
        :active_job_id,
        { batch_id: bson_id, state: "failed" },
        **SolidQueue::Mongo.session_options
      ).size
    end

    def progress_percentage
      return 0 if total_jobs.to_i.zero?
      ([ total_jobs.to_i - pending_jobs, 0 ].max * 100.0 / total_jobs).round(2)
    end

    def metadata
      (self[:metadata] || {}).with_indifferent_access
    end

    def destroy!
      SolidQueue::Mongo.transaction(operation: "destroy batch") do
        SolidQueue::Mongo.collection(:batch_executions).delete_many({ batch_id: bson_id }, **SolidQueue::Mongo.session_options)
        super
      end
    end

    private
      def finalize
        reload
        return if BatchExecution.outstanding_for_batch?(bson_id)

        SolidQueue.instrument(:finish_batch, batch_id: id) do |payload|
          failures = SolidQueue::Mongo.collection(:jobs).distinct(
            :active_job_id,
            { batch_id: bson_id, state: "failed" },
            **SolidQueue::Mongo.session_options
          ).size
          completed = total_jobs.to_i - failures
          fields = { failed_jobs: failures, completed_jobs: completed, updated_at: Time.current }
          fields[:failed_at] = Time.current if failures.positive?
          self.class.collection.update_one({ _id: bson_id }, { "$set" => fields }, **SolidQueue::Mongo.session_options)
          fields.each { |name, value| public_send("#{name}=", value) }
          enqueue_callback_jobs
          payload.merge!(total_jobs: total_jobs, failed_jobs: failures, completed_jobs: completed)
        end
      end

      def enqueue_callback_jobs
        enqueue_callback_job(:on_failure) if failed?
        enqueue_callback_job(:on_success) unless failed?
        enqueue_callback_job(:on_finish)
      end

      def enqueue_callback_job(callback_name)
        callback = public_send(callback_name)
        return unless callback

        active_job = ActiveJob::Base.deserialize(callback)
        active_job.callback_batch_id = id
        active_job.run_callbacks(:enqueue) do
          Job.enqueue(active_job, scheduled_at: active_job.scheduled_at || Time.current)
        end
      end
  end
end
