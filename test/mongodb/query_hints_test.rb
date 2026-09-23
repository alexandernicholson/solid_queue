# frozen_string_literal: true

require_relative "test_helper"

class MongoQueryHintLimitedJob < ActiveJob::Base
  limits_concurrency key: ->(key) { key }, to: 1, duration: 5.minutes

  def perform(*)
  end
end

class MongoQueryHintsTest < MongoTestCase
  class CommandRecorder
    attr_reader :commands

    def initialize
      @commands = []
      @lock = Mutex.new
    end

    def started(event)
      @lock.synchronize { @commands << event.command } if %w[find aggregate].include?(event.command_name.to_s)
    end

    def succeeded(*)
    end

    def failed(*)
    end
  end

  test "claiming from every queue hints the ready poll and claim readback indexes" do
    MongoRaceJob.perform_later

    hints = hints_on(:jobs) { SolidQueue::ReadyExecution.claim("*", 1, BSON::ObjectId.new) }

    assert_equal [ "ready_poll_all_v2", "claimed_by_token_v2" ], hints
  end

  test "claiming from a named queue hints the per-queue ready poll index" do
    MongoRaceJob.perform_later

    hints = hints_on(:jobs) { SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new) }

    assert_equal [ "ready_poll_by_queue_v2", "claimed_by_token_v2" ], hints
  end

  test "dispatching due scheduled jobs hints the scheduled dispatch index" do
    MongoRaceJob.set(wait: 1.hour).perform_later
    SolidQueue::Mongo.collection(:jobs).update_many({ state: "scheduled" }, { "$set" => { scheduled_at: 1.minute.ago } })

    hints = hints_on(:jobs) { SolidQueue::ScheduledExecution.dispatch_next_batch(10) }

    assert_equal "scheduled_dispatch_v2", hints.first
  end

  test "releasing and unblocking blocked jobs hint the blocked indexes" do
    2.times { MongoQueryHintLimitedJob.perform_later("hinted") }
    key = MongoQueryHintLimitedJob.new("hinted").concurrency_key

    assert_equal [ "blocked_release_v2" ], hints_on(:jobs) { SolidQueue::BlockedExecution.release_for(key) }
    assert_equal [ "blocked_maintenance_v2" ], hints_on(:jobs) { SolidQueue::BlockedExecution.unblock(10) }
  end

  test "maintenance scans hint their indexes" do
    batch = SolidQueue::Batch.enqueue { MongoRaceJob.perform_later }

    assert_equal [ "batch_execution_attempts" ], hints_on(:batch_executions) { SolidQueue::BatchExecution.outstanding_for_batch?(batch.id) }
    assert_equal [ "process_heartbeat" ], hints_on(:processes) { SolidQueue::Process.prune }
    assert_equal [ "semaphore_expiration" ], hints_on(:semaphores) { SolidQueue::Semaphore.expire(batch_size: 10) }
  end

  private
    def hints_on(collection_name)
      recorder = CommandRecorder.new
      client = SolidQueue::Mongo.client
      client.subscribe(::Mongo::Monitoring::COMMAND, recorder)
      yield
      name = "solid_queue_#{collection_name}"
      recorder.commands.select { |command| (command["find"] || command["aggregate"]) == name }.map { |command| command["hint"] }
    ensure
      client&.unsubscribe(::Mongo::Monitoring::COMMAND, recorder)
    end
end
