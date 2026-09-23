# frozen_string_literal: true

require "test_helper"
require "active_support/log_subscriber/test_helper"

class LogSubscriberTest < ActiveSupport::TestCase
  include ActiveSupport::LogSubscriber::TestHelper

  teardown { ActiveSupport::LogSubscriber.log_subscribers.clear }

  def set_logger(logger)
    SolidQueue.logger = logger
  end

  test "unblock one job" do
    attach_log_subscriber
    instrument "release_blocked.solid_queue", job_id: 42, concurrency_key: "foo/1", released: true

    assert_match_logged :debug, "Release blocked job", "job_id: 42, concurrency_key: \"foo/1\", released: true"
  end

  test "unblock many jobs" do
    attach_log_subscriber
    instrument "release_many_blocked.solid_queue", limit: 42, size: 10

    assert_match_logged :debug, "Unblock jobs", "limit: 42, size: 10"
  end

  test "recurring task enqueued succesfully" do
    attach_log_subscriber
    time = Time.now
    instrument "enqueue_recurring_task.solid_queue", task: :example_task, active_job_id: "b944ddbc-6a37-43c0-b661-4b56e57195f5", at: time

    assert_match_logged :debug, "Enqueued recurring task", "task: :example_task, active_job_id: \"b944ddbc-6a37-43c0-b661-4b56e57195f5\", at: \"#{time.iso8601}\""
  end

  test "recurring task skipped" do
    attach_log_subscriber
    time = Time.now
    instrument "enqueue_recurring_task.solid_queue", task: :example_task, skipped: true, at: time

    assert_match_logged :debug, "Skipped recurring task – already dispatched", "task: :example_task, at: \"#{time.iso8601}\""
  end

  test "error enqueuing recurring task" do
    attach_log_subscriber
    time = Time.now
    instrument "enqueue_recurring_task.solid_queue", task: :example_task, enqueue_error: "Everything is broken", at: time

    assert_match_logged :error, "Error enqueuing recurring task", "task: :example_task, enqueue_error: \"Everything is broken\", at: \"#{time.iso8601}\""
  end

  test "fork boot timeout" do
    worker = SolidQueue::Worker.new

    attach_log_subscriber
    instrument "fork_boot_timeout.solid_queue", process: worker, pid: 42

    assert_match_logged :warn, "Terminate Worker that failed to boot in time", "pid: 42, hostname: \"#{worker.hostname}\", name: \"#{worker.name}\""
  end

  test "deregister process" do
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, hostname: "localhost", name: "worker-123")
    last_heartbeat_at = process.last_heartbeat_at.iso8601

    attach_log_subscriber
    instrument "deregister_process.solid_queue", process: process, pruned: false, claimed_size: 0

    assert_match_logged :debug, "Deregister Worker", "process_id: #{process.id}, pid: 42, hostname: \"localhost\", name: \"worker-123\", last_heartbeat_at: \"#{last_heartbeat_at}\", claimed_size: 0, pruned: false"
  end

  test "MongoDB command" do
    attach_log_subscriber
    instrument "mongo_command.solid_queue", status: :succeeded, command_name: "find", database_name: "queue", duration: 0.002, address: "127.0.0.1:27017", error: nil

    assert_match_logged :debug, "MongoDB command", "status: :succeeded, command_name: \"find\", database_name: \"queue\", duration: 0.002, address: \"127.0.0.1:27017\""
  end

  test "run time exceeded" do
    started_at = Time.now
    attach_log_subscriber
    instrument "run_time_exceeded.solid_queue", job_id: 42, process_id: 7, max_run_time: 10.minutes, started_at: started_at, display_name: "User#welcome"

    assert_match_logged :warn, "Fail job that exceeded its run time", "job_id: 42, process_id: 7, display_name: \"User#welcome\", max_run_time: 10 minutes, started_at: \"#{started_at.iso8601}\""
  end

  test "death recovery" do
    attach_log_subscriber
    instrument "death_recovery.solid_queue", job_ids: [ 1, 2, 3 ], retried: [ 1, 2 ], exhausted: [ 3 ], error: SolidQueue::Processes::ProcessMissingError.new

    assert_match_logged :info, "Retry jobs failed by process death", "job_ids: [1, 2, 3], retried: [1, 2], exhausted: [3], error: \"SolidQueue::Processes::ProcessMissingError The process that was running this job no longer exists\""
  end

  test "work off" do
    attach_log_subscriber
    instrument "work_off.solid_queue", queues: [ "*" ], limit: 100, priority: 0..10, successes: 3, failures: 1

    assert_match_logged :info, "Work off jobs", "queues: [\"*\"], limit: 100, priority: \"0..10\", successes: 3, failures: 1"
  end

  test "drained" do
    attach_log_subscriber
    instrument "drained.solid_queue", process_id: 7, name: "worker-1", queues: [ "a", "b" ], priority_range: 1..5

    assert_match_logged :info, "Worker drained", "process_id: 7, name: \"worker-1\", queues: [\"a\", \"b\"], priority_range: \"1..5\""
  end

  test "check latency" do
    attach_log_subscriber
    instrument "check_latency.solid_queue", max_age: 300, count: 2, latency: 451

    assert_match_logged :info, "Check queue latency", "max_age: 300, count: 2, latency: 451"
  end

  test "fail claimed jobs includes display names" do
    attach_log_subscriber
    instrument "fail_many_claimed.solid_queue", job_ids: [ 42 ], process_ids: [ 7 ], display_names: { 42 => "User#welcome" }, error: RuntimeError.new("gone")

    assert_match_logged :warn, "Fail claimed jobs", "job_ids: [42], process_ids: [7], display_names: #{({ 42 => "User#welcome" }).inspect}, error: \"RuntimeError gone\""
  end

  test "release claimed job includes the display name" do
    attach_log_subscriber
    instrument "release_claimed.solid_queue", job_id: 42, process_id: 7, display_name: "User#welcome"

    assert_match_logged :info, "Release claimed job", "job_id: 42, process_id: 7, display_name: \"User#welcome\""
  end

  test "perform exactly once" do
    attach_log_subscriber
    instrument "perform_exactly_once.solid_queue", job_id: 42, process_id: 7, display_name: "User#welcome", run_time_limit: 60.seconds, outcome: :committed

    assert_match_logged :debug, "Perform exactly-once job", "job_id: 42, process_id: 7, display_name: \"User#welcome\", run_time_limit: 60 seconds, outcome: :committed"
  end

  test "perform exactly once rolled back" do
    attach_log_subscriber
    instrument "perform_exactly_once.solid_queue", job_id: 42, process_id: 7, display_name: "User#welcome", run_time_limit: 60.seconds, outcome: :rolled_back

    assert_match_logged :info, "Perform exactly-once job", "job_id: 42, process_id: 7, display_name: \"User#welcome\", run_time_limit: 60 seconds, outcome: :rolled_back"
  end

  test "perform exactly once conflict" do
    attach_log_subscriber
    instrument "perform_exactly_once.solid_queue", job_id: 42, process_id: 7, display_name: "User#welcome", run_time_limit: 60.seconds, outcome: :conflict

    assert_match_logged :warn, "Perform exactly-once job", "job_id: 42, process_id: 7, display_name: \"User#welcome\", run_time_limit: 60 seconds, outcome: :conflict"
  end

  test "release uncommitted exactly-once claims" do
    attach_log_subscriber
    instrument "release_uncommitted.solid_queue", job_ids: [ 42, 43 ], released: [ 42 ], exhausted: [], locked: [ 43 ], process_ids: [ 7 ], display_names: { 42 => "User#welcome" }, size: 1, error: SolidQueue::Processes::ProcessMissingError.new

    assert_match_logged :info, "Release uncommitted exactly-once claims", "job_ids: [42, 43], released: [42], exhausted: [], locked: [43], process_ids: [7], display_names: #{({ 42 => "User#welcome" }).inspect}, error: \"SolidQueue::Processes::ProcessMissingError The process that was running this job no longer exists\""
  end

  test "exhausted uncommitted exactly-once claims" do
    attach_log_subscriber
    instrument "release_uncommitted.solid_queue", job_ids: [ 42 ], released: [], exhausted: [ 42 ], locked: [], process_ids: [ 7 ], display_names: { 42 => "User#welcome" }, size: 0, error: SolidQueue::Processes::ProcessMissingError.new

    assert_match_logged :warn, "Release uncommitted exactly-once claims", "job_ids: [42], released: [], exhausted: [42], locked: [], process_ids: [7]"
  end

  private
    def attach_log_subscriber
      ActiveSupport::LogSubscriber.attach_to :solid_queue, SolidQueue::LogSubscriber.new
    end

    def instrument(...)
      ActiveSupport::Notifications.instrument(...)
      wait
    end

    def assert_match_logged(level, action, attributes)
      assert_equal 1, @logger.logged(level).size
      assert_match /SolidQueue-[\d.]+(\.beta)? #{action} \(\d+\.\d+ms\)  #{Regexp.escape(attributes)}/, @logger.logged(level).last
    end
end
