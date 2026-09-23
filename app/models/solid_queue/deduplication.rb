# frozen_string_literal: true

module SolidQueue
  class Deduplication < Record
    scope :expired, -> { where(expires_at: ...Time.current) }

    class << self
      def reserve(job, active_job)
        key = active_job.deduplication_key
        return true unless key

        expired.where(key: key).delete_all
        return true if create_unique_by(key: key, active_job_id: job.active_job_id, job_id: job.id, expires_at: active_job.deduplication_duration&.from_now)

        holder = find_by(key: key)
        return true if holder&.active_job_id == job.active_job_id

        SolidQueue.instrument(:enqueue_duplicate, deduplication_key: key, active_job_id: job.active_job_id, job_id: holder&.job_id)
        false
      end

      def release(jobs, windowed: false)
        active_job_ids = Array(jobs).map(&:active_job_id).compact.uniq
        return if active_job_ids.empty?

        released = active_job_ids - Job.where(active_job_id: active_job_ids, finished_at: nil).pluck(:active_job_id)
        return if released.empty?

        scope = where(active_job_id: released)
        scope = scope.where(expires_at: nil) unless windowed
        scope.delete_all
      end

      private
        def create_unique_by(attributes)
          transaction(requires_new: true) { create!(**attributes) }
          true
        rescue ActiveRecord::RecordNotUnique
          false
        end
    end
  end
end
