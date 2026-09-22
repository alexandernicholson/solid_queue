# frozen_string_literal: true

require_relative "test_helper"
require "solid_queue/admin"

class MongoNativeAdminFailureJob < ActiveJob::Base
  def perform(*)
    raise "admin failure"
  end
end

class MongoNativeAdminTest < MongoTestCase
  test "batch counts exclude logical markers from outstanding attempts" do
    batch = SolidQueue::Batch.enqueue do
      MongoRaceJob.perform_later("batch-member")
    end

    counts = SolidQueue::Admin.batch_job_counts([ batch ], statuses: [ :pending ])

    assert_equal 1, counts.fetch(batch.id).fetch(:outstanding)
    assert_equal 1, counts.fetch(batch.id).fetch(:pending)
  end

  test "unfinished batches do not appear in finished or failed tabs" do
    batch = SolidQueue::Batch.enqueue do
      MongoRaceJob.perform_later("unfinished")
    end

    assert_equal [ batch.id ], SolidQueue::Admin.batches(status: :unfinished).map(&:id)
    assert_empty SolidQueue::Admin.batches(status: :finished)
    assert_empty SolidQueue::Admin.batches(status: :failed)
  end

  test "bulk actions honor their pagination window" do
    3.times { |index| MongoNativeAdminFailureJob.perform_later(index) }
    claims = SolidQueue::ReadyExecution.claim([ "default" ], 3, BSON::ObjectId.new)
    claims.each { |claim| assert_raises(RuntimeError) { claim.perform } }

    SolidQueue::Admin.retry_jobs(status: :failed, offset: 1, limit: 1)

    assert_equal 2, SolidQueue::Admin.jobs_count(status: :failed)
    assert_equal 1, SolidQueue::Admin.jobs_count(status: :pending)
  end

  test "open date filter leaves queue and class filters effective" do
    matching = MongoRaceJob.set(queue: "filtered").perform_later("matching")
    MongoRaceJob.set(queue: "other").perform_later("unmatched")

    filters = { queue_name: "filtered", job_class_name: "MongoRaceJob", enqueued_at: nil..nil }
    assert_equal [ matching.job_id ], SolidQueue::Admin.jobs(status: :pending, **filters).map(&:active_job_id)
    assert_equal 1, SolidQueue::Admin.jobs_count(status: :pending, **filters)
  end

  test "dashboard raw data exposes decoded job arguments" do
    enqueued = MongoRaceJob.perform_later("visible")
    job = SolidQueue::Admin.find_job(enqueued.job_id)

    assert_equal [ "visible" ], SolidQueue::Admin.job_attributes(job).fetch(:raw_data).dig("arguments", "arguments")
  end
end
