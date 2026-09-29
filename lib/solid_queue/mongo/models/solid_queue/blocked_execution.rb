# frozen_string_literal: true

module SolidQueue
  class BlockedExecution < Execution
    class PromotionLost < StandardError; end
    private_constant :PromotionLost

    class << self
      def block(job)
        duration = job.job_class ? job.concurrency_duration : SolidQueue.default_concurrency_control_period
        now = Time.current
        document = collection.find_one_and_update(
          { _id: job.bson_id, state: { "$in" => [ nil, "scheduled" ] } },
          { "$set" => { state: "blocked", expires_at: now + duration, updated_at: now },
            "$unset" => { process_id: "", claim_token: "" } },
          return_document: :after,
          **SolidQueue::Mongo.session_options
        )
        document && from_document(document)
      end

      def release_one(concurrency_key)
        SolidQueue.instrument(:release_blocked, concurrency_key: concurrency_key, released: false) do |payload|
          transaction(operation: "release_blocked") do
            document = collection.find(
              { state: "blocked", concurrency_key: concurrency_key },
              hint: "blocked_release_v2",
              **SolidQueue::Mongo.session_options
            ).sort(priority: 1, _id: 1).limit(1).first
            next false unless document

            job = Job.from_document(document)
            payload[:job_id] = job.id
            if job.job_class.nil?
              job.failed_with(Job::ClassMissingError.for(job))
              payload[:failed] = true
              true
            elsif Semaphore.wait(job)
              result = collection.update_one(
                { _id: job.bson_id, state: "blocked" },
                { "$set" => { state: "ready", updated_at: Time.current }, "$unset" => { expires_at: "" } },
                **SolidQueue::Mongo.session_options
              )
              raise PromotionLost unless result.modified_count == 1

              payload[:released] = true
              true
            else
              false
            end
          end
        end
      rescue PromotionLost
        false
      end

      def release_many(concurrency_keys)
        Array(concurrency_keys).count { |concurrency_key| release_one(concurrency_key) }
      end

      def unblock(limit)
        SolidQueue.instrument(:release_many_blocked, limit: limit) do |payload|
          keys = collection.aggregate(
            [
              { "$match" => { state: "blocked", expires_at: { "$lt" => Time.current } } },
              { "$sort" => { concurrency_key: 1, expires_at: 1 } },
              { "$group" => { _id: "$concurrency_key" } },
              { "$limit" => limit }
            ],
            hint: "blocked_maintenance_v2",
            **SolidQueue::Mongo.session_options
          ).map { |document| document["_id"] }.compact
          payload[:size] = release_many(keys)
        end
      end

      def any?
        count.positive?
      end

      def none?
        count.zero?
      end
    end

    def release
      self.class.release_one(concurrency_key)
    end
  end
end
