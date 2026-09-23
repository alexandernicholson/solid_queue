# frozen_string_literal: true

module SolidQueue
  class Deduplication < Record
    collection_name :deduplications

    field :key
    field :active_job_id
    field :job_id
    field :expires_at
    field :created_at

    class << self
      def reserve(job, active_job)
        key = active_job.deduplication_key
        return true unless key

        now = Time.current
        collection.delete_one({ key: key, expires_at: { "$lte" => now } }, **SolidQueue::Mongo.session_options)
        result = collection.update_one(
          { key: key },
          { "$setOnInsert" => {
            key: key,
            active_job_id: job.active_job_id,
            job_id: job.bson_id,
            expires_at: active_job.deduplication_duration&.from_now,
            created_at: now
          } },
          upsert: true,
          **SolidQueue::Mongo.session_options
        )
        return true if result.upserted_id

        holder = collection.find({ key: key }, **SolidQueue::Mongo.session_options).projection(active_job_id: 1, job_id: 1).first
        return true if holder && holder["active_job_id"] == job.active_job_id

        holder_id = holder&.dig("job_id")&.to_s
        SolidQueue::Mongo.after_commit do
          SolidQueue.instrument(:enqueue_duplicate, deduplication_key: key, active_job_id: job.active_job_id, job_id: holder_id)
        end
        false
      end

      def release(jobs, windowed: false)
        active_job_ids = Array(jobs).map(&:active_job_id).compact.uniq
        return if active_job_ids.empty?

        remaining = Job.collection.distinct(
          :active_job_id,
          { active_job_id: { "$in" => active_job_ids }, state: { "$in" => %w[ ready scheduled blocked claimed failed ] } },
          **SolidQueue::Mongo.session_options
        )
        released = active_job_ids - remaining
        return if released.empty?

        filter = { active_job_id: { "$in" => released } }
        filter[:expires_at] = nil unless windowed
        collection.delete_many(filter, **SolidQueue::Mongo.session_options)
      end
    end
  end
end
