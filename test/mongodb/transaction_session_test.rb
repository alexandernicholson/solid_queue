# frozen_string_literal: true

require_relative "test_helper"

class MongoTransactionSessionTest < MongoTestCase
  class AbortTransaction < StandardError; end

  def test_a_body_that_outlasts_the_retry_deadline_still_commits
    previous_timeout = SolidQueue.mongo_transaction_timeout
    SolidQueue.mongo_transaction_timeout = 0.2
    collection = SolidQueue::Mongo.collection(:jobs)

    SolidQueue::Mongo.transaction(operation: "slow_body") do
      collection.insert_one({ active_job_id: "slow-body" }, **SolidQueue::Mongo.session_options)
      sleep 0.4
      collection.insert_one({ active_job_id: "slow-body" }, **SolidQueue::Mongo.session_options)
    end

    assert_equal 2, collection.count_documents(active_job_id: "slow-body")
  ensure
    SolidQueue.mongo_transaction_timeout = previous_timeout
  end

  def test_mongo_transaction_rolls_back_writes_made_with_its_session
    collection = SolidQueue::Mongo.collection(:jobs)

    assert_raises(AbortTransaction) do
      SolidQueue::Mongo.transaction(operation: "rollback_test") do |session|
        collection.insert_one({ active_job_id: "rolled-back" }, session: session)
        raise AbortTransaction
      end
    end

    assert_equal 0, collection.count_documents(active_job_id: "rolled-back")
  end

  def test_with_mongo_session_makes_native_enqueue_part_of_the_caller_transaction
    active_job = MongoRaceJob.new("scoped")

    SolidQueue::Mongo.client.start_session do |session|
      session.start_transaction
      SolidQueue.with_mongo_session(session) do
        SolidQueue::Job.enqueue(active_job)
      end
      session.abort_transaction
    end

    assert_equal 0, SolidQueue::Mongo.collection(:jobs).count_documents(active_job_id: active_job.job_id)
  end

  def test_only_writes_carrying_the_explicit_session_roll_back
    collection = SolidQueue::Mongo.collection(:jobs)

    SolidQueue::Mongo.client.start_session do |session|
      session.start_transaction
      collection.insert_one({ active_job_id: "explicit" }, session: session)
      collection.insert_one({ active_job_id: "omitted" })
      session.abort_transaction
    end

    assert_equal 0, collection.count_documents(active_job_id: "explicit")
    assert_equal 1, collection.count_documents(active_job_id: "omitted")
  end

  def test_with_session_defaults_to_the_configured_queue_database
    configured_client = SolidQueue::Mongo.client

    configured_client.start_session do |session|
      SolidQueue.with_mongo_session(session) do
        assert_same configured_client, SolidQueue::Mongo.client
        assert_equal configured_client.database.name, SolidQueue::Mongo.collection(:jobs).database.name
      end
    end
  end

  def test_with_session_accepts_an_explicit_client_on_the_same_cluster
    configured_client = SolidQueue::Mongo.client
    explicit_client = configured_client.use(configured_client.database.name)

    configured_client.start_session do |session|
      SolidQueue.with_mongo_session(session, client: explicit_client) do
        assert_same explicit_client, SolidQueue::Mongo.client
        assert_equal configured_client.database.name, SolidQueue::Mongo.collection(:jobs).database.name
      end
    end
  end

  def test_with_session_rejects_a_client_from_a_different_cluster
    other_client = ::Mongo::Client.new(MONGODB_TEST_URI)

    SolidQueue::Mongo.client.start_session do |session|
      error = assert_raises(ArgumentError) do
        SolidQueue.with_mongo_session(session, client: other_client) { flunk "must not yield" }
      end
      assert_match(/different Mongo client cluster/, error.message)
    end
  ensure
    other_client&.close
  end

  def test_with_session_rejects_nested_different_session_without_corrupting_owned_rollback
    collection = SolidQueue::Mongo.collection(:jobs)
    callback_runs = 0

    SolidQueue::Mongo.client.start_session do |other_session|
      assert_raises(AbortTransaction) do
        SolidQueue::Mongo.transaction(operation: "nested_session_guard") do |owned_session|
          SolidQueue::Mongo.after_commit { callback_runs += 1 }
          error = assert_raises(ArgumentError) do
            SolidQueue.with_mongo_session(other_session) { flunk "must not yield" }
          end
          assert_match(/nested Mongo sessions are not supported/, error.message)

          SolidQueue.with_mongo_session(owned_session) do
            collection.insert_one(
              { active_job_id: "same-session-nesting" },
              **SolidQueue::Mongo.session_options
            )
          end
          raise AbortTransaction
        end
      end
    end

    assert_equal 0, callback_runs
    assert_equal 0, collection.count_documents(active_job_id: "same-session-nesting")
  end
end
