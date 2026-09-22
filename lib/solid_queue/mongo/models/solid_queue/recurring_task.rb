# frozen_string_literal: true

require "fugit"
require "active_job/arguments"

module SolidQueue
  class RecurringTask < Record
    collection_name :recurring_tasks

    field :key
    field :schedule
    field :command
    field :class_name
    field :arguments, default: -> { [] }
    field :queue_name
    field :priority, default: 0
    field :static, default: true
    field :description

    validate :ensure_schedule_supported
    validate :ensure_command_or_class_present
    validate :ensure_existing_job_class

    mattr_accessor :default_job_class
    self.default_job_class = RecurringJob

    set_callback :save, :around, :serialize_arguments_while_saving

    class << self
      def wrap(args)
        args.is_a?(self) ? args : from_configuration(args.first, **args.second)
      end

      def from_configuration(key, **options)
        new(
          key: key.to_s,
          class_name: options[:class],
          command: options[:command],
          arguments: options[:args],
          schedule: options[:schedule],
          queue_name: options[:queue].presence,
          priority: options[:priority].presence,
          description: options[:description],
          static: options.fetch(:static, true)
        )
      end

      def from_document(document)
        return unless document

        attributes = document.dup
        arguments_key = attributes.key?(:arguments) ? :arguments : "arguments"
        attributes[arguments_key] = deserialize_arguments(attributes[arguments_key])
        super(attributes)
      end

      def create_dynamic_task(key, **options)
        from_configuration(key, **options.merge(static: false)).save!
        true
      end

      def delete_dynamic_task(key)
        task = find_by(key: key.to_s, static: false)
        raise_record_not_found(key) unless task
        task.destroy!
      end

      def create_or_update_all(tasks)
        now = Time.current
        operations = tasks.map do |task|
          attributes = task.attributes_for_upsert.symbolize_keys.merge(updated_at: now)
          attributes[:arguments] = serialize_arguments(attributes[:arguments])
          {
            update_one: {
              filter: { key: task.key },
              update: {
                "$set" => attributes,
                "$setOnInsert" => { _id: BSON::ObjectId.new, created_at: now }
              },
              upsert: true
            }
          }
        end
        return if operations.empty?

        collection.bulk_write(operations, **SolidQueue::Mongo.session_options)
      rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
        raise_persistence_error(error)
      end

      def dynamic_tasks(excluding: [])
        filter = { static: false }
        filter[:key] = { "$nin" => Array(excluding).map(&:to_s) } if excluding.present?
        collection.find(filter, **SolidQueue::Mongo.session_options).map { |document| from_document(document) }
      end

      def task_keys
        collection.find({}, **SolidQueue::Mongo.session_options).projection(key: 1).map { |document| document["key"] || document[:key] }
      end

      def delete_static_except(keys)
        filter = { static: true }
        filter[:key] = { "$nin" => Array(keys).map(&:to_s) } if keys.present?
        collection.delete_many(filter, **SolidQueue::Mongo.session_options).deleted_count
      rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
        raise_persistence_error(error)
      end

      def static_tasks(keys)
        filter = { static: true, key: { "$in" => Array(keys).map(&:to_s) } }
        collection.find(filter, **SolidQueue::Mongo.session_options).map { |document| from_document(document) }
      end

      def find_by!(filter = {})
        find_by(filter) || raise_record_not_found(filter)
      end

      def exists?(filter = {})
        collection.count_documents(normalize_filter(filter), limit: 1, **SolidQueue::Mongo.session_options).positive?
      end

      def admin_all
        collection.find({}, **SolidQueue::Mongo.session_options).sort(key: 1).map { |document| from_document(document) }
      end

      def admin_find(key)
        find_by(key: key.to_s)
      end

      def serialize_arguments(arguments)
        ActiveJob::Arguments.serialize(Array(arguments))
      end

      def deserialize_arguments(arguments)
        ActiveJob::Arguments.deserialize(Array(arguments))
      end
    end

    def schedule=(value)
      @parsed_schedule = @parsed_schedule_with_time_zone = nil
      @attributes[:schedule] = value
    end

    def class_name=(value)
      @job_class = nil
      @attributes[:class_name] = value
    end

    def reload
      super.tap do
        self.arguments = self.class.deserialize_arguments(arguments)
        @parsed_schedule = @parsed_schedule_with_time_zone = @job_class = nil
      end
    end

    def destroy
      destroy!
    end

    def next_time_after(time)
      parsed_schedule_with_time_zone.next_time(time).utc
    end

    def next_time
      parsed_schedule_with_time_zone.next_time.utc
    end

    def previous_time
      parsed_schedule_with_time_zone.previous_time.utc
    end

    def last_enqueued_time
      RecurringExecution.last_enqueued_at_by_task([ key ])[key]
    end

    def enqueue(at:)
      SolidQueue.instrument(:enqueue_recurring_task, task: key, at: at) do |payload|
        active_job = if using_solid_queue_adapter?
          enqueue_and_record(run_at: at)
        else
          payload[:other_adapter] = true
          perform_later.tap do |job|
            unless job.successfully_enqueued?
              report_enqueue_error(job.enqueue_error, at: at)
              payload[:enqueue_error] = job.enqueue_error&.message
            end
          end
        end

        active_job.tap { |enqueued_job| payload[:active_job_id] = enqueued_job.job_id }
      rescue RecurringExecution::AlreadyRecorded
        payload[:skipped] = true
        false
      rescue Job::EnqueueError => error
        report_enqueue_error(error, at: at)
        payload[:enqueue_error] = error.message
        false
      end
    end

    def to_s
      "#{class_name}.perform_later(#{arguments.map(&:inspect).join(",")}) [ #{parsed_schedule.original} ]"
    end

    def attributes_for_upsert
      attributes.except("id", "_id", "created_at", "updated_at")
    end

    private
      def serialize_arguments_while_saving
        original_arguments = arguments
        self.arguments = self.class.serialize_arguments(original_arguments)
        yield
      ensure
        self.arguments = original_arguments
      end

      def ensure_schedule_supported
        unless parsed_schedule.instance_of?(Fugit::Cron)
          errors.add :schedule, :unsupported, message: "is not a supported recurring schedule"
        end
      rescue ArgumentError => error
        message = if error.message.include?("multiple crons")
          "generates multiple cron schedules. Please use separate recurring tasks for each schedule, " \
            "or use explicit cron syntax (e.g., '40 0,15 * * *' for multiple times with the same minutes)"
        else
          error.message
        end
        errors.add :schedule, :unsupported, message: message
      end

      def ensure_command_or_class_present
        unless command.present? || class_name.present?
          errors.add :base, :command_and_class_blank, message: "either command or class must be present"
        end
      end

      def ensure_existing_job_class
        if class_name.present? && job_class.nil?
          errors.add :class_name, :undefined, message: "doesn't correspond to an existing class"
        end
      end

      def using_solid_queue_adapter?
        job_class.queue_adapter_name.inquiry.solid_queue?
      end

      def enqueue_and_record(run_at:)
        RecurringExecution.record(key, run_at) do
          job_class.new(*arguments_with_kwargs).set(enqueue_options).tap do |active_job|
            active_job.run_callbacks(:enqueue) { Job.enqueue(active_job) }
          end
        end
      end

      def perform_later
        job_class.new(*arguments_with_kwargs).tap { |active_job| active_job.enqueue(enqueue_options) }
      end

      def arguments_with_kwargs
        if class_name.nil?
          command
        elsif arguments.last.is_a?(Hash)
          arguments[0...-1] + [ Hash.ruby2_keywords_hash(arguments.last) ]
        else
          arguments
        end
      end

      def parsed_schedule_with_time_zone
        @parsed_schedule_with_time_zone ||= apply_default_time_zone_to(parsed_schedule)
      end

      def parsed_schedule
        @parsed_schedule ||= Fugit.parse(schedule, multi: :fail)
      end

      def apply_default_time_zone_to(schedule)
        if schedule.respond_to?(:zone) && schedule.zone.nil? && default_time_zone.present?
          Fugit.parse("#{schedule.to_cron_s} #{default_time_zone}", multi: :fail)
        else
          schedule
        end
      rescue ArgumentError
        schedule
      end

      def job_class
        @job_class ||= class_name.present? ? class_name.safe_constantize : self.class.default_job_class
      end

      def enqueue_options
        { queue: queue_name, priority: priority }.compact
      end

      def default_time_zone
        SolidQueue.time_zone
      end

      def report_enqueue_error(error, at:)
        if error
          Rails.error.report(error, handled: true, source: "application.solid_queue", context: { task: key, at: at })
        end
      end
  end
end
