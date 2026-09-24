# frozen_string_literal: true

require_relative "test_helper"

class MongoNativeRecurringJob < ActiveJob::Base
  class_attribute :performed_values, default: []

  def perform(value)
    self.class.performed_values += [ value ]
  end
end

class MongoNativeRecurringTest < MongoTestCase
  def setup
    super
    MongoNativeRecurringJob.performed_values = []
  end

  test "static recurring definitions can be inserted updated and dispatched" do
    options = { class: "MongoNativeRecurringJob", schedule: "every minute", static: true }
    original = SolidQueue::RecurringTask.from_configuration("refreshable", **options, args: [ "original" ])
    SolidQueue::RecurringTask.create_or_update_all([ original ])
    updated = SolidQueue::RecurringTask.from_configuration("refreshable", **options, args: [ "updated" ])
    SolidQueue::RecurringTask.create_or_update_all([ updated ])

    task = SolidQueue::RecurringTask.static_tasks([ "refreshable" ]).fetch(0)
    assert_equal 1, SolidQueue::RecurringTask.count
    task.enqueue(at: Time.current)
    SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0).perform
    assert_equal [ "updated" ], MongoNativeRecurringJob.performed_values
  end

  test "concurrent schedulers create one marker and one job for a recurrence" do
    run_at = Time.at(Time.current.to_i).utc
    recorded = Queue.new
    duplicates = Queue.new

    concurrently(8) do
      begin
        active_job = SolidQueue::RecurringExecution.record("hourly-report", run_at) do
          MongoNativeRecurringJob.perform_later("once")
        end
        recorded << active_job.provider_job_id
      rescue SolidQueue::RecurringExecution::AlreadyRecorded
        duplicates << true
      end
    end

    assert_equal 1, recorded.size
    assert_equal 7, duplicates.size
    assert_equal 1, SolidQueue::RecurringExecution.count
    assert_equal 1, SolidQueue::Job.count

    SolidQueue::ReadyExecution.claim([ "default" ], 1, BSON::ObjectId.new).fetch(0).perform
    assert_equal [ "once" ], MongoNativeRecurringJob.performed_values
  end

  test "the recurring execution key includes both task key and run time" do
    first_run = Time.at(Time.current.to_i).utc
    second_run = first_run + 1.minute

    first = SolidQueue::RecurringExecution.record("same-task", first_run) do
      MongoNativeRecurringJob.perform_later("first")
    end
    second = SolidQueue::RecurringExecution.record("same-task", second_run) do
      MongoNativeRecurringJob.perform_later("second")
    end

    assert_not_equal first.provider_job_id, second.provider_job_id
    assert_equal 2, SolidQueue::RecurringExecution.count
    assert_equal({ "same-task" => second_run }, SolidQueue::RecurringExecution.last_enqueued_at_by_task([ "same-task" ]))
  end

  test "recurring retention removes only markers whose jobs are gone" do
    run_at = Time.at(Time.current.to_i).utc
    live = SolidQueue::RecurringExecution.record("live", run_at) do
      MongoNativeRecurringJob.perform_later("live")
    end
    orphan = SolidQueue::RecurringExecution.record("orphan", run_at) do
      MongoNativeRecurringJob.perform_later("orphan")
    end
    SolidQueue::Job.collection.delete_one(_id: SolidQueue::Mongo.id!(orphan.provider_job_id))

    SolidQueue::RecurringExecution.clear_in_batches(batch_size: 1)

    assert_equal 1, SolidQueue::RecurringExecution.count
    assert_equal({ "live" => run_at }, SolidQueue::RecurringExecution.last_enqueued_at_by_task(%w[live orphan]))
    assert SolidQueue::Job.find(live.provider_job_id).ready?
  end
end
