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
end
