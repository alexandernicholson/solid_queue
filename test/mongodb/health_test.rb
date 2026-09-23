# frozen_string_literal: true

require "resolv"
require "stringio"
require_relative "test_helper"
require "mocha/minitest"

class MongoHealthTest < MongoTestCase
  FORK_COLLECTION = "solid_queue_fork_health"

  def setup
    super
    @mongo_client = SolidQueue.mongo_client
    @mongo_url = SolidQueue.mongo_url
  end

  def teardown
    SolidQueue::Mongo.reset!
    SolidQueue.mongo_client = @mongo_client
    SolidQueue.mongo_url = @mongo_url
    SolidQueue::Mongo.prepare!
    super
  end

  test "owned clients are replaced after fork without ending parent sessions" do
    assert_fork_safe_client
  end

  test "direct clients are replaced after fork without ending parent sessions" do
    supplied_client = ::Mongo::Client.new(MONGODB_TEST_URI)
    assert_fork_safe_client(client: supplied_client)
  ensure
    supplied_client&.close
  end

  test "callable clients are replaced after fork without ending parent sessions" do
    supplied_client = ::Mongo::Client.new(MONGODB_TEST_URI)
    assert_fork_safe_client(client: -> { supplied_client })
  ensure
    supplied_client&.close
  end

  test "SRV clients are rebuilt from their seedlist hostname after fork" do
    skip "fork is unavailable" unless ::Process.respond_to?(:fork)

    stub_srv_seedlist("cluster0.example.test", "node1.example.test:27017", "node2.example.test:27017")
    previous_logger = ::Mongo::Logger.logger
    ::Mongo::Logger.logger = ::Logger.new(nil)
    srv_client = ::Mongo::Client.new(
      "mongodb+srv://queue:secret@cluster0.example.test/queue_srv?authSource=admin",
      server_selection_timeout: 1, connect_timeout: 1
    )
    SolidQueue::Mongo.reset!
    SolidQueue.mongo_client = srv_client
    SolidQueue::Mongo.client

    result = in_fork do
      SolidQueue::Mongo.after_fork!
      child_client = SolidQueue::Mongo.client
      {
        replaced: !child_client.equal?(srv_client),
        hostname: child_client.cluster.options[:srv_uri]&.query_hostname,
        addresses: child_client.cluster.addresses.map(&:to_s).sort,
        user: child_client.options[:user],
        database: child_client.database.name
      }
    end

    assert result.fetch(:replaced)
    assert_equal "cluster0.example.test", result.fetch(:hostname)
    assert_equal [ "node1.example.test:27017", "node2.example.test:27017" ], result.fetch(:addresses)
    assert_equal "queue", result.fetch(:user)
    assert_equal "queue_srv", result.fetch(:database)
  ensure
    srv_client&.close
    ::Mongo::Logger.logger = previous_logger if previous_logger
  end

  test "pool wait and exhaustion publish measured CMAP telemetry" do
    wait_events = capture_events("mongo_pool_checkout_wait.solid_queue") do
      with_exhausted_pool(wait_queue_timeout: 2) do |client, release|
        releaser = Thread.new do
          sleep 0.075
          release << true
        end
        client.database.command(ping: 1).first
        releaser.join
      end
    end

    assert_equal 1, wait_events.size
    assert_operator wait_events.first.payload.fetch(:duration), :>=, 0.05
    assert_match(/\A[^:]+:\d+\z/, wait_events.first.payload.fetch(:address))

    failure_events = capture_events("mongo_pool_checkout_failed.solid_queue") do
      with_exhausted_pool(wait_queue_timeout: 0.05) do |client, _release|
        assert_raises(::Mongo::Error::ConnectionCheckOutTimeout) { client.database.command(ping: 1).first }
      end
    end

    assert_equal 1, failure_events.size
    assert_equal :timeout, failure_events.first.payload.fetch(:reason)
    assert_operator failure_events.first.payload.fetch(:duration), :>=, 0.04
  end

  test "polling silence is local to its thread and restores the logger" do
    output = StringIO.new
    logger = ::Logger.new(output)
    logger.level = ::Logger::DEBUG
    previous_logger = ::Mongo::Logger.logger
    ::Mongo::Logger.logger = logger
    entered = Queue.new
    release = Queue.new

    silenced = Thread.new do
      SolidQueue::Mongo.silence_logging do
        entered << true
        release.pop
        logger.info("silenced polling log")
      end
    end

    entered.pop
    logger.info("unrelated thread log")
    release << true
    silenced.join

    assert_includes output.string, "unrelated thread log"
    assert_not_includes output.string, "silenced polling log"
    assert_equal ::Logger::DEBUG, logger.level
  ensure
    release << true if defined?(release) && release && defined?(silenced) && silenced&.alive?
    silenced&.join
    ::Mongo::Logger.logger = previous_logger if defined?(previous_logger) && previous_logger
  end

  private
    def assert_fork_safe_client(client: nil)
      skip "fork is unavailable" unless ::Process.respond_to?(:fork)

      SolidQueue::Mongo.reset!
      SolidQueue.mongo_url = MONGODB_TEST_URI
      SolidQueue.mongo_client = client
      parent_client = SolidQueue::Mongo.client
      parent_client[FORK_COLLECTION].delete_many({})
      session = parent_client.start_session
      session.start_transaction
      token = SecureRandom.uuid
      parent_client[FORK_COLLECTION].insert_one({ token: token }, session: session)
      reader, writer = IO.pipe
      reader.binmode
      writer.binmode

      pid = fork do
        reader.close
        driver_log = StringIO.new
        ::Mongo::Logger.logger = ::Logger.new(driver_log)
        begin
          inherited_id = parent_client.object_id
          SolidQueue::Mongo.after_fork!
          child_client = SolidQueue::Mongo.client
          child_client.database.command(ping: 1).first
          writer.write(Marshal.dump(
            ok: true, inherited_id: inherited_id, child_id: child_client.object_id,
            driver_log: driver_log.string
          ))
        rescue Exception => error
          writer.write(Marshal.dump(ok: false, error: "#{error.class}: #{error.message}", driver_log: driver_log.string))
        ensure
          writer.close
          exit! 0
        end
      end

      writer.close
      result = Marshal.load(reader.read)
      reader.close
      _, status = ::Process.waitpid2(pid)
      assert status.success?
      assert result.fetch(:ok), result[:error]
      assert_not_equal result.fetch(:inherited_id), result.fetch(:child_id)
      assert_no_match(/Detected PID change/, result.fetch(:driver_log))

      session.commit_transaction
      assert_equal 1, parent_client[FORK_COLLECTION].count_documents(token: token)
    ensure
      if session
        session.abort_transaction if SolidQueue::Mongo.transaction_in_progress?(session)
        session.end_session
      end
      parent_client&.[](FORK_COLLECTION)&.delete_many({ token: token }) if defined?(token) && token
    end

    def stub_srv_seedlist(hostname, *addresses)
      result = ::Mongo::Srv::Result.new(hostname)
      addresses.each do |address|
        host, port = address.split(":")
        result.add_record(Resolv::DNS::Resource::IN::SRV.new(0, 0, Integer(port), Resolv::DNS::Name.create(host)).tap { |record| record.instance_variable_set(:@ttl, 60) })
      end
      ::Mongo::Srv::Resolver.any_instance.stubs(:get_records).returns(result)
      ::Mongo::Srv::Resolver.any_instance.stubs(:get_txt_options_string).returns(nil)
    end

    def in_fork
      reader, writer = IO.pipe
      reader.binmode
      writer.binmode

      pid = fork do
        reader.close
        writer.write(Marshal.dump(ok: true, value: yield))
      rescue Exception => error
        writer.write(Marshal.dump(ok: false, error: "#{error.class}: #{error.message}"))
      ensure
        writer.close
        exit! 0
      end

      writer.close
      result = Marshal.load(reader.read)
      reader.close
      ::Process.waitpid(pid)
      assert result.fetch(:ok), result[:error]
      result.fetch(:value)
    end

    def with_exhausted_pool(wait_queue_timeout:)
      client = ::Mongo::Client.new(MONGODB_TEST_URI,
        max_pool_size: 1, wait_queue_timeout: wait_queue_timeout, retry_reads: false)
      client.database.command(ping: 1).first
      SolidQueue::Mongo::Monitoring.install!(client)
      pool = client.cluster.next_primary.pool
      checked_out = Queue.new
      release = Queue.new
      holder = Thread.new do
        pool.with_connection do
          checked_out << true
          release.pop
        end
      end
      checked_out.pop

      yield client, release
    ensure
      release << true if defined?(release) && release
      holder&.join
      SolidQueue::Mongo::Monitoring.forget!(client) if client
      client&.close
    end

    def capture_events(name)
      events = []
      lock = Mutex.new
      subscriber = lambda do |*arguments|
        event = ActiveSupport::Notifications::Event.new(*arguments)
        lock.synchronize { events << event }
      end
      ActiveSupport::Notifications.subscribed(subscriber, name) { yield }
      events
    end
end
