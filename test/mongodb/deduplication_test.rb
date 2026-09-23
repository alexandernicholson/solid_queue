# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/deduplication_jobs"
require_relative "../shared/deduplication_behaviour"

class MongoDeduplicationTest < MongoTestCase
  include DeduplicationBehaviour

  private
    def job_count(job_class)
      SolidQueue::Mongo.collection(:jobs).count_documents(class_name: job_class.name)
    end

    def deduplication_count(key)
      SolidQueue::Mongo.collection(:deduplications).count_documents(key: key)
    end

    def expire_deduplication_key(key)
      SolidQueue::Mongo.collection(:deduplications).update_one({ key: key }, { "$set" => { expires_at: 1.minute.ago } })
    end

    def other_process_id
      BSON::ObjectId.new
    end
end
