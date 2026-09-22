# frozen_string_literal: true

require_relative "test_helper"

class MongoClaimPerformanceTest < MongoTestCase
  class CommandRecorder
    attr_reader :command_names

    def initialize
      @command_names = []
    end

    def started(event)
      @command_names << event.command_name
    end

    def succeeded(*)
    end

    def failed(*)
    end
  end

  [ 1, 100, 500 ].each do |batch_size|
    test "claiming #{batch_size} jobs uses three jobs collection operations and returns full payloads" do
      active_jobs = batch_size.times.map { |number| MongoRaceJob.new("payload-#{number}") }
      assert_equal batch_size, SolidQueue::Job.enqueue_all(active_jobs)
      recorder = CommandRecorder.new

      claims = with_command_recorder(recorder) do
        SolidQueue::ReadyExecution.claim("*", batch_size, BSON::ObjectId.new)
      end

      assert_equal batch_size, claims.size
      assert_equal active_jobs.map(&:provider_job_id).sort, claims.map(&:job_id).sort
      assert_equal active_jobs.map(&:serialize).map { |payload| payload.fetch("arguments") }.sort_by(&:to_s),
        claims.map { |claim| claim.arguments.fetch("arguments") }.sort_by(&:to_s)
      assert_equal [ "find", "update", "find" ],
        recorder.command_names.select { |name| %w[find update getMore].include?(name) }
    end
  end

  test "checkout timeout makes the claim ambiguous and releases local candidate reservations" do
    active_job = MongoRaceJob.perform_later("released-after-timeout")
    jobs = SolidQueue::ReadyExecution.collection
    timeout = ::Mongo::Error::ConnectionCheckOutTimeout.new(
      "injected claim checkout timeout",
      address: ::Mongo::Address.new("127.0.0.1:27017")
    )

    error = jobs.stub(:update_many, ->(*, **) { raise timeout }) do
      assert_raises(SolidQueue::ReadyExecution::AmbiguousClaimError) do
        SolidQueue::ReadyExecution.claim("*", 1, BSON::ObjectId.new)
      end
    end

    assert_includes error.message, "ConnectionCheckOutTimeout"
    claim = SolidQueue::ReadyExecution.claim("*", 1, BSON::ObjectId.new).fetch(0)
    assert_equal active_job.job_id, claim.active_job_id
  end

  test "independent processes claim every job without loss or duplication" do
    skip "fork is unavailable" unless Process.respond_to?(:fork)

    job_count = 500
    assert_equal job_count, SolidQueue::Job.enqueue_all(job_count.times.map { |number| MongoRaceJob.new(number) })
    readers = []
    children = 2.times.map do
      reader, writer = IO.pipe
      readers << reader
      fork do
        reader.close
        SolidQueue::Mongo.after_fork!
        claims = SolidQueue::ReadyExecution.claim("*", job_count, BSON::ObjectId.new)
        Marshal.dump(claims.map(&:job_id), writer)
        writer.close
        exit!(0)
      end.tap { writer.close }
    end

    claimed_ids = readers.flat_map { |reader| Marshal.load(reader) }
    statuses = children.map { |pid| Process.waitpid2(pid).last }

    assert statuses.all?(&:success?)
    assert_equal job_count, claimed_ids.size
    assert_equal job_count, claimed_ids.uniq.size
    assert_equal 0, SolidQueue::Mongo.collection(:jobs).count_documents(state: "ready")
    assert_equal job_count, SolidQueue::Mongo.collection(:jobs).count_documents(state: "claimed")
  ensure
    readers&.each { |reader| reader.close unless reader.closed? }
    children&.each do |pid|
      Process.waitpid(pid, Process::WNOHANG)
    rescue Errno::ECHILD
    end
  end

  private
    def with_command_recorder(recorder)
      SolidQueue::Mongo.client.subscribe(::Mongo::Monitoring::COMMAND, recorder)
      yield
    ensure
      SolidQueue::Mongo.client.unsubscribe(::Mongo::Monitoring::COMMAND, recorder)
    end
end
