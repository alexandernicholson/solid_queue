# frozen_string_literal: true

require "active_job/enqueuing"

module SolidQueue
  class Job < Record
    class EnqueueError < StandardError; end
    class DuplicateError < ActiveJob::EnqueueError; end

    # Raised when a job's class can't be resolved anymore, typically because it
    # was renamed or removed in a deploy while jobs referencing it were in
    # flight. It subclasses NameError, which is what resolving the class raises.
    class ClassMissingError < NameError
      def self.for(job)
        new("Job class #{job.class_name.inspect} could not be resolved")
      end
    end

    include Executable, Clearable, Recurrable, Batchable, Deduplicatable, RunTimeLimited, Nameable, Deliverable

    serialize :arguments, coder: JSON

    class << self
      def enqueue_all(active_jobs)
        # Bulk enqueues bypass ActiveJob#enqueue, so batch membership is captured here
        current_batch_id = Batch.current_batch_id

        active_jobs.each do |job|
          job.scheduled_at ||= Time.current
          job.batch_id = current_batch_id || job.batch_id
        end
        active_jobs_by_job_id = active_jobs.index_by(&:job_id)

        duplicates = []
        transaction do
          reserved, duplicates = active_jobs.partition { |active_job| reserve_deduplication(new(active_job_id: active_job.job_id), active_job) }
          jobs = create_all_from_active_jobs(reserved)
          prepare_all_for_execution(jobs).tap do |enqueued_jobs|
            enqueued_jobs.each do |enqueued_job|
              active_jobs_by_job_id[enqueued_job.active_job_id].provider_job_id = enqueued_job.id
              active_jobs_by_job_id[enqueued_job.active_job_id].successfully_enqueued = true
            end
          end
        end

        duplicates.each do |active_job|
          active_job.successfully_enqueued = false
          active_job.enqueue_error = duplicate_error_for(active_job)
        end
        active_jobs.count(&:successfully_enqueued?)
      end

      def enqueue(active_job, scheduled_at: Time.current)
        active_job.scheduled_at = scheduled_at

        enqueued_job = transaction do
          reserve_deduplication(new(active_job_id: active_job.job_id), active_job) ? create_from_active_job(active_job) : nil
        end
        raise duplicate_error_for(active_job) unless enqueued_job

        active_job.provider_job_id = enqueued_job.id if enqueued_job.persisted?
        active_job.successfully_enqueued = enqueued_job.persisted?
        enqueued_job
      end

      private
        DEFAULT_PRIORITY = 0
        DEFAULT_QUEUE_NAME = "default"

        def create_from_active_job(active_job)
          create!(**attributes_from_active_job(active_job))
        rescue ActiveRecord::ActiveRecordError => e
          enqueue_error = EnqueueError.new("#{e.class.name}: #{e.message}").tap do |error|
            error.set_backtrace e.backtrace
          end
          raise enqueue_error
        end

        def create_all_from_active_jobs(active_jobs)
          return none if active_jobs.empty?

          job_rows = active_jobs.map { |job| attributes_from_active_job(job) }
          insert_all(job_rows)
          where(active_job_id: active_jobs.map(&:job_id)).order(id: :asc).tap do |jobs|
            jobs.select(&:deduplicated?).each do |job|
              Deduplication.where(active_job_id: job.active_job_id, job_id: nil).update_all(job_id: job.id)
            end
          end
        end

        def attributes_from_active_job(active_job)
          {
            queue_name: active_job.queue_name || DEFAULT_QUEUE_NAME,
            active_job_id: active_job.job_id,
            priority: active_job.priority || DEFAULT_PRIORITY,
            scheduled_at: active_job.scheduled_at,
            class_name: active_job.class.name,
            arguments: active_job.serialize,
            concurrency_key: active_job.concurrency_key,
            deduplication_key: active_job.try(:deduplication_key)
          }.tap do |attributes|
            attributes[:batch_id] = active_job.batch_id if Batch.migrated?
            delivery_mode = delivery_mode_from(active_job)
            attributes[:delivery_mode] = delivery_mode if delivery_modes_migrated?
          end
        end
    end
  end
end
