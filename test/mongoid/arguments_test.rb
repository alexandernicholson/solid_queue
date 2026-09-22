# frozen_string_literal: true

require "bundler/setup"
require "active_job/railtie"
require "mongoid"
require_relative "../mongodb/test_helper"

Mongoid.configure do |config|
  config.clients.default = { uri: MONGODB_TEST_URI }
end

SolidQueue::MongoidIntegration.install!

class MongoidArgumentDocument
  include Mongoid::Document

  store_in collection: "solid_queue_test_argument_documents"
  field :observed_id, type: BSON::ObjectId
end

class MongoidConcurrencyJob < ActiveJob::Base
  include ActiveJob::ConcurrencyControls unless ancestors.include?(ActiveJob::ConcurrencyControls)

  limits_concurrency key: ->(document) { document }
end

class MongoidExecutionJob < ActiveJob::Base
  def perform(document, arguments)
    document.set(observed_id: arguments.fetch("nested").first)
  end
end

class MongoidArgumentsTest < MongoTestCase
  setup do
    MongoidArgumentDocument.delete_all
  end

  teardown do
    MongoidArgumentDocument.delete_all
  end

  test "a persisted document has the same concurrency key after reload" do
    document = MongoidArgumentDocument.create!
    original_key = MongoidConcurrencyJob.new(document).concurrency_key
    reloaded_key = MongoidConcurrencyJob.new(MongoidArgumentDocument.find(document.id)).concurrency_key

    assert_equal "MongoidConcurrencyJob/MongoidArgumentDocument/#{document.id}", original_key
    assert_equal original_key, reloaded_key
    refute defined?(ActiveRecord::Base)
  end

  test "a nested BSON ObjectId survives an Active Job JSON payload round trip" do
    object_id = BSON::ObjectId.new
    serialized = ActiveJob::Arguments.serialize([ { "nested" => [ object_id ] } ])
    json_payload = JSON.parse(JSON.generate(serialized))

    restored = ActiveJob::Arguments.deserialize(json_payload)

    assert_equal object_id, restored.dig(0, "nested", 0)
    assert_instance_of BSON::ObjectId, restored.dig(0, "nested", 0)
  end

  test "a queued job resolves its document and nested ObjectId when performed" do
    document = MongoidArgumentDocument.create!
    job = MongoidExecutionJob.perform_later(document, { "nested" => [ document.id ] })
    worker = SolidQueue::Process.register(kind: "Worker", name: SecureRandom.uuid,
      pid: ::Process.pid, hostname: "test", metadata: {})

    claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, worker.id).fetch(0)
    claim.perform

    assert_equal document.id, document.reload.observed_id
    assert SolidQueue::Job.find(job.provider_job_id).finished?
  end

  test "fork replaces the shared queue and application client without ending parent sessions" do
    skip "fork is unavailable" unless ::Process.respond_to?(:fork)

    previous_mongoid_client = SolidQueue.mongoid_client
    SolidQueue::Mongo.reset!
    SolidQueue.mongoid_client = :default
    SolidQueue::Mongo.prepare!
    parent_client = Mongoid::Clients.default
    assert_same parent_client, SolidQueue::Mongo.client

    document = MongoidArgumentDocument.create!
    object_id = BSON::ObjectId.new
    job = MongoidExecutionJob.perform_later(document, { "nested" => [ object_id ] })
    session = parent_client.start_session
    session.start_transaction
    token = SecureRandom.uuid
    marker_collection = parent_client["solid_queue_mongoid_fork_health"]
    marker_collection.insert_one({ token: token }, session: session)
    reader, writer = IO.pipe
    reader.binmode
    writer.binmode

    pid = fork do
      reader.close
      begin
        SolidQueue::Mongo.after_fork!
        queue_client = SolidQueue::Mongo.client
        application_client = Mongoid::Clients.default
        claim = SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0)
        claim.perform
        reloaded = MongoidArgumentDocument.find(document.id)
        writer.write(Marshal.dump(
          ok: true,
          shared_client: queue_client.equal?(application_client),
          observed_id: reloaded.observed_id
        ))
      rescue Exception => error
        writer.write(Marshal.dump(ok: false, error: "#{error.class}: #{error.message}"))
      ensure
        writer.close
        exit! 0
      end
    end

    writer.close
    result = Marshal.load(reader.read)
    reader.close
    _, status = Process.waitpid2(pid)
    assert status.success?
    assert result.fetch(:ok), result[:error]
    assert result.fetch(:shared_client)
    assert_equal object_id, result.fetch(:observed_id)

    session.commit_transaction
    assert_equal 1, marker_collection.count_documents(token: token)
    assert SolidQueue::Job.find(job.provider_job_id).finished?
  ensure
    if session
      session.abort_transaction if SolidQueue::Mongo.transaction_in_progress?(session)
      session.end_session
    end
    marker_collection&.delete_many(token: token) if defined?(token) && token
    SolidQueue::Mongo.reset!
    SolidQueue.mongoid_client = previous_mongoid_client if defined?(previous_mongoid_client)
    SolidQueue::Mongo.prepare!
  end

  test "fork preserves identity when a direct queue client is a Mongoid registry entry" do
    skip "fork is unavailable" unless ::Process.respond_to?(:fork)

    previous_mongo_client = SolidQueue.mongo_client
    SolidQueue::Mongo.reset!
    registry_client = Mongoid::Clients.default
    SolidQueue.mongo_client = registry_client
    SolidQueue::Mongo.prepare!
    document = MongoidArgumentDocument.create!
    reader, writer = IO.pipe
    reader.binmode
    writer.binmode

    pid = fork do
      reader.close
      begin
        SolidQueue::Mongo.after_fork!
        found = MongoidArgumentDocument.find(document.id)
        writer.write(Marshal.dump(
          ok: true,
          shared_client: SolidQueue::Mongo.client.equal?(Mongoid::Clients.default),
          document_id: found.id
        ))
      rescue Exception => error
        writer.write(Marshal.dump(ok: false, error: "#{error.class}: #{error.message}"))
      ensure
        writer.close
        exit! 0
      end
    end

    writer.close
    result = Marshal.load(reader.read)
    reader.close
    _, status = Process.waitpid2(pid)
    assert status.success?
    assert result.fetch(:ok), result[:error]
    assert result.fetch(:shared_client)
    assert_equal document.id, result.fetch(:document_id)
  ensure
    SolidQueue::Mongo.reset!
    SolidQueue.mongo_client = previous_mongo_client if defined?(previous_mongo_client)
    SolidQueue::Mongo.prepare!
  end
end
