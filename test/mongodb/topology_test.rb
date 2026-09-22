# frozen_string_literal: true

require_relative "test_helper"

class MongoTopologyFailoverJob < ActiveJob::Base
  class_attribute :performed_tokens, default: []

  def perform(token)
    self.class.performed_tokens += [ token ]
  end
end


class MongoTopologyTest < MongoTestCase
  test "transaction topology validation rejects a real standalone server" do
    uri = ENV["MONGODB_STANDALONE_URI"]
    skip "set MONGODB_STANDALONE_URI to a standalone MongoDB server" if uri.nil? || uri.empty?

    client = ::Mongo::Client.new(uri, server_selection_timeout: 2)
    error = assert_raises(SolidQueue::Mongo::ConfigurationError) do
      SolidQueue::Mongo.validate_transaction_support!(client)
    end

    assert_match(/requires a replica set or sharded cluster/, error.message)
  ensure
    client&.close
  end

  test "a multi-node stepdown emits SDAM health events and selects the new primary" do
    uri = ENV["MONGODB_STEPDOWN_URI"]
    skip "set MONGODB_STEPDOWN_URI to a dedicated multi-node replica set" if uri.nil? || uri.empty?

    client = ::Mongo::Client.new(uri,
      heartbeat_frequency: 0.1,
      server_selection_timeout: 15,
      connect_timeout: 2)
    SolidQueue::Mongo::Monitoring.install!(client)
    hello = client.database.command(hello: 1).first
    hosts = hello["hosts"] || []
    assert_operator hosts.size, :>=, 2, "MONGODB_STEPDOWN_URI must identify a multi-node replica set"
    original_primary = hello.fetch("primary")
    events = capture_topology_events do
      begin
        client.use("admin").database.command(replSetStepDown: 5, force: true).first
      rescue *SolidQueue::Mongo::DRIVER_ERRORS
        # Primaries commonly close the connection while stepping down.
      end

      wait_for_topology(timeout: 20) do
        begin
          current = client.database.command(hello: 1).first
          current["isWritablePrimary"] && current["primary"] != original_primary
        rescue *SolidQueue::Mongo::DRIVER_ERRORS
          false
        end
      end
    end

    primary_change = events.find { |event| event.name == "mongo_primary_change.solid_queue" && event.payload[:new_type] == "primary" }
    assert primary_change, "expected a primary-change SDAM event after replSetStepDown"
    assert primary_change.payload.fetch(:address)
  ensure
    SolidQueue::Mongo::Monitoring.forget!(client) if client
    client&.close
  end

  test "queue claims recover ownership across a real primary stepdown" do
    uri = ENV["MONGODB_STEPDOWN_URI"]
    skip "set MONGODB_STEPDOWN_URI to a dedicated multi-node replica set" if uri.nil? || uri.empty?

    previous_client = SolidQueue.mongo_client
    previous_url = SolidQueue.mongo_url
    queue_client = ::Mongo::Client.new(uri,
      heartbeat_frequency: 0.1, server_selection_timeout: 15,
      connect_timeout: 2, timeout_ms: 10_000, retry_writes: false,
      write: { w: :majority }, read: { mode: :primary })
    admin_client = ::Mongo::Client.new(uri,
      heartbeat_frequency: 0.1, server_selection_timeout: 15,
      connect_timeout: 2, timeout_ms: 10_000)
    SolidQueue::Mongo.reset!
    SolidQueue.mongo_client = queue_client
    SolidQueue::Mongo.prepare!
    SolidQueue::Mongo::COLLECTIONS.each { |name| SolidQueue::Mongo.collection(name).delete_many({}) }
    MongoTopologyFailoverJob.performed_tokens = []

    tokens = 3.times.map { SecureRandom.uuid }
    tokens.each { |token| MongoTopologyFailoverJob.perform_later(token) }
    actor = SolidQueue::Process.register(
      kind: "Worker", name: "failover-old-primary", pid: Process.pid,
      hostname: "topology-test", metadata: {}
    )
    barrier = UpdateCommandBarrier.new(queue_client.database.name)
    queue_client.subscribe(::Mongo::Monitoring::COMMAND, barrier)
    claim_result = Queue.new
    claim_thread = Thread.new do
      claim_result << SolidQueue::ReadyExecution.claim([ "default" ], tokens.size, actor.id)
    rescue Exception => error
      claim_result << error
    end
    Timeout.timeout(10) { barrier.arrived.pop }
    original_primary = admin_client.database.command(hello: 1).first.fetch("primary")
    begin
      admin_client.use("admin").database.command(replSetStepDown: 5, force: true).first
    rescue *SolidQueue::Mongo::DRIVER_ERRORS
      # The old primary may close its command socket after accepting stepdown.
    ensure
      barrier.release << true
    end

    Timeout.timeout(20) { claim_thread.join }
    first_claim = claim_result.pop
    unless first_claim.is_a?(Array) || first_claim.is_a?(SolidQueue::ReadyExecution::AmbiguousClaimError)
      raise first_claim
    end
    wait_for_topology(timeout: 20) do
      begin
        hello = queue_client.database.command(hello: 1).first
        hello["isWritablePrimary"] && hello["primary"] != original_primary
      rescue *SolidQueue::Mongo::DRIVER_ERRORS
        false
      end
    end

    # Retire the uncertain actor, then fence any claim that reached the old
    # primary late before allowing a replacement actor to take ownership.
    actor.deregister
    SolidQueue::ClaimedExecution.release_for_process(actor.id)
    replacement = SolidQueue::Process.register(
      kind: "Worker", name: "failover-new-primary", pid: Process.pid,
      hostname: "topology-test", metadata: {}
    )
    recovered = SolidQueue::ReadyExecution.claim([ "default" ], tokens.size, replacement.id)
    recovered.each(&:perform)

    assert_equal tokens.sort, MongoTopologyFailoverJob.performed_tokens.sort
    assert_equal tokens.size, MongoTopologyFailoverJob.performed_tokens.uniq.size
    assert_equal tokens.size, SolidQueue::Mongo.collection(:jobs).count_documents(state: "finished")
    assert_equal 0, SolidQueue::Mongo.collection(:jobs).count_documents(state: "claimed")
    assert_equal 0, SolidQueue::Mongo.collection(:jobs).count_documents(state: "ready")
  ensure
    barrier&.release&.push(true) if defined?(barrier) && barrier
    claim_thread&.join(1)
    queue_client&.unsubscribe(::Mongo::Monitoring::COMMAND, barrier) if queue_client && defined?(barrier) && barrier
    if queue_client
      begin
        SolidQueue::Mongo::COLLECTIONS.each { |name| queue_client["solid_queue_#{name}"].delete_many({}) }
      rescue *SolidQueue::Mongo::DRIVER_ERRORS
        nil
      end
    end
    SolidQueue::Mongo.reset!
    SolidQueue.mongo_client = previous_client if defined?(previous_client)
    SolidQueue.mongo_url = previous_url if defined?(previous_url)
    SolidQueue::Mongo.prepare!
    queue_client&.close
    admin_client&.close
  end

  class UpdateCommandBarrier
    attr_reader :arrived, :release

    def initialize(database_name)
      @database_name = database_name
      @arrived = Queue.new
      @release = Queue.new
      @blocked = false
      @lock = Mutex.new
    end

    def started(event)
      return unless event.command_name == "update" && event.database_name == @database_name
      return unless event.command["update"] == "solid_queue_jobs"
      return unless @lock.synchronize { !@blocked && (@blocked = true) }

      @arrived << true
      @release.pop
    end

    def succeeded(*)
    end

    def failed(*)
    end
  end

  private
    def capture_topology_events
      events = []
      lock = Mutex.new
      subscriber = lambda do |*arguments|
        event = ActiveSupport::Notifications::Event.new(*arguments)
        lock.synchronize { events << event }
      end
      pattern = /\Amongo_(?:primary_change|server_unavailable)\.solid_queue\z/
      ActiveSupport::Notifications.subscribed(subscriber, pattern) { yield }
      events
    end

    def wait_for_topology(timeout:)
      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + timeout
      until yield
        if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) >= deadline
          flunk "replica set did not elect a different primary within #{timeout} seconds"
        end
        sleep 0.05
      end
    end
end
