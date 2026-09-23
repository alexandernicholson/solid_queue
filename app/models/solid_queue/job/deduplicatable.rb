# frozen_string_literal: true

module SolidQueue
  class Job
    module Deduplicatable
      extend ActiveSupport::Concern

      included do
        after_create -> { Deduplication.where(active_job_id: active_job_id, job_id: nil).update_all(job_id: id) }, if: :deduplicated?
        after_update :release_deduplication, if: -> { deduplicated? && saved_change_to_finished_at? && finished_at.present? }
        after_destroy -> { Deduplication.release([ self ], windowed: true) }, if: :deduplicated?
      end

      class_methods do
        def reserve_deduplication(job, active_job)
          return true unless active_job.try(:deduplication_key)

          Deduplication.reserve(job, active_job)
        end

        def duplicate_error_for(active_job)
          Job::DuplicateError.new("#{active_job.class.name} is already enqueued as #{active_job.deduplication_key}")
        end
      end

      def deduplicated?
        deduplication_key.present?
      end

      def release_deduplication
        Deduplication.release([ self ]) if deduplicated?
      end
    end
  end
end
