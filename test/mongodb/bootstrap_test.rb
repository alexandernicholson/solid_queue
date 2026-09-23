# frozen_string_literal: true

require_relative "test_helper"

class MongoBootstrapTest < MongoTestCase
  def test_boots_and_prepares_mongodb_without_active_record
    assert_not defined?(ActiveRecord::Base)
    assert SolidQueue.mongodb?

    collection_names = SolidQueue::Mongo.client.database.collection_names
    SolidQueue::Mongo::COLLECTIONS.each do |name|
      assert_includes collection_names, "solid_queue_#{name}"
    end
  end

  def test_nested_object_ids_survive_an_active_job_payload_round_trip
    object_id = BSON::ObjectId.new
    payload = JSON.parse(JSON.generate(ActiveJob::Arguments.serialize([ { "nested" => [ object_id ] } ])))

    restored = ActiveJob::Arguments.deserialize(payload).dig(0, "nested", 0)

    assert_equal object_id, restored
    assert_instance_of BSON::ObjectId, restored
  end

  def test_mongoid_is_not_loaded
    assert_not defined?(::Mongoid)
  end
end
