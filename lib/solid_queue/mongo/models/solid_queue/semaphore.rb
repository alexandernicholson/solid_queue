# frozen_string_literal: true

module SolidQueue
  class Semaphore < Record
    collection_name :semaphores

    field :key
    field :value, default: 1
    field :expires_at
    field :created_at
    field :updated_at

    class << self
      def wait(job)
        transaction(operation: "semaphore_wait") do
          now = Time.current
          expiry = job.concurrency_duration.from_now
          limit = job.concurrency_limit || 1

          collection.update_one(
            { key: job.concurrency_key },
            { "$setOnInsert" => { value: limit, created_at: now, updated_at: now, expires_at: expiry } },
            upsert: true,
            **SolidQueue::Mongo.session_options
          )

          collection.update_one(
            { key: job.concurrency_key, value: { "$gt" => 0 } },
            { "$inc" => { value: -1 }, "$set" => { expires_at: expiry, updated_at: now } },
            **SolidQueue::Mongo.session_options
          ).modified_count == 1
        end
      end

      def signal(job)
        transaction(operation: "semaphore_signal") do
          limit = job.concurrency_limit || 1
          result = collection.update_one(
            { key: job.concurrency_key, value: { "$lt" => limit } },
            { "$inc" => { value: 1 }, "$set" => { expires_at: job.concurrency_duration.from_now, updated_at: Time.current } },
            **SolidQueue::Mongo.session_options
          )
          result.modified_count == 1
        end
      end

      def signal_all(jobs)
        transaction(operation: "semaphore_signal_all") do
          jobs.select(&:concurrency_limited?).group_by { |job| job.concurrency_limit || 1 }.sum do |limit, grouped_jobs|
            keys = grouped_jobs.map(&:concurrency_key).uniq
            next 0 if keys.empty?

            collection.update_many(
              { key: { "$in" => keys }, value: { "$lt" => limit } },
              { "$inc" => { value: 1 }, "$set" => { updated_at: Time.current } },
              **SolidQueue::Mongo.session_options
            ).modified_count
          end
        end
      end

      def expire(batch_size:)
        now = Time.current
        deleted = 0
        loop do
          ids = collection.find(
            { expires_at: { "$lt" => now } },
            **SolidQueue::Mongo.session_options
          ).projection(_id: 1).sort(expires_at: 1, _id: 1).limit(batch_size)
            .map { |document| document.fetch("_id") }
          break if ids.empty?

          removed = collection.delete_many(
            { _id: { "$in" => ids }, expires_at: { "$lt" => now } },
            **SolidQueue::Mongo.session_options
          ).deleted_count
          deleted += removed
          break if removed.zero?
        end
        deleted
      end
    end
  end
end
