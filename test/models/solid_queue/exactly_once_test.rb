# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/exactly_once_jobs"
require_relative "../../shared/exactly_once_behaviour"
require_relative "../../../lib/generators/solid_queue/update/templates/db/add_delivery_modes_to_solid_queue"

class SolidQueue::ExactlyOnceTest < ActiveSupport::TestCase
  include ExactlyOnceBehaviour

  self.use_transactional_tests = false

  teardown { SolidQueue::Batch.destroy_all }

  test "without the delivery_mode column a job takes its class's mode when performed" do
    migrate_delivery_modes(:down)
    assert_not SolidQueue::Job.delivery_modes_migrated?

    exactly_once = SharedExactlyOnceJob.perform_later("unmigrated", raising: true)
    at_least_once = SharedPlainEffectJob.perform_later("unmigrated-plain", raising: true)
    2.times { assert_raises(SharedExactlyOnceError) { claim_and_perform } }

    assert_equal [ 0, 1 ], %w[ unmigrated unmigrated-plain ].map { |name| SharedExactlyOnceEffects.count(name) }
    assert_equal [ :exactly_once, :at_least_once ], [ exactly_once, at_least_once ].map { |job| SolidQueue::Job.find(job.provider_job_id).delivery_mode }
  ensure
    migrate_delivery_modes(:up)
  end

  test "there is no MongoDB session on SQL" do
    sessions = []
    SharedExactlyOnceJob.observer = ->(_) { sessions << SolidQueue.exactly_once_session }
    SharedExactlyOnceJob.perform_later("sessionless")

    claim_and_perform

    assert_equal [ nil ], sessions
  end

  test "a sweep skips an exactly-once claim whose row an open transaction holds" do
    skip_on_sqlite

    active_job = SharedExactlyOnceJob.perform_later("locked")
    claimed = claim(unregistered_process_id).sole
    holding = Concurrent::Event.new
    finish = Concurrent::Event.new
    holder = Thread.new do
      SolidQueue.app_executor.wrap do
        SolidQueue::Record.transaction do
          SolidQueue::ClaimedExecution.where(id: claimed.id).lock.first
          holding.set
          finish.wait(10)
        end
      end
    end
    holding.wait(10)

    events = capture_events("release_uncommitted.solid_queue") do
      SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
    end
    finish.set
    holder.join

    assert_empty events.first.payload[:released]
    assert_equal [ active_job.provider_job_id ], events.first.payload[:locked]
    assert SolidQueue::Job.find(active_job.provider_job_id).claimed?

    SolidQueue::ClaimedExecution.fail_orphaned(SolidQueue::Processes::ProcessMissingError.new)
    assert SolidQueue::Job.find(active_job.provider_job_id).ready?
  ensure
    finish&.set
    holder&.join
  end

  private
    def unregistered_process_id
      SolidQueue::Process.maximum(:id).to_i + 1_000
    end

    def failed_nested_attempt_keeps_enclosing_writes?
      true
    end

    def jobs_count(class_name)
      SolidQueue::Job.where(class_name: class_name).count
    end

    def with_failing_completion
      SolidQueue::Job.any_instance.stubs(:finished!).raises(SharedExactlyOnceError, "completion")
      yield
    ensure
      SolidQueue::Job.any_instance.unstub(:finished!)
    end

    def migrate_delivery_modes(direction)
      ActiveRecord::Migration.suppress_messages do
        SolidQueue::Record.connection_pool.with_connection do |connection|
          AddDeliveryModesToSolidQueue.new.exec_migration(connection, direction)
        end
      end

      SolidQueue::Job.reset_column_information
      SolidQueue::Record.connection_pool.disconnect!
    end

    def skip_on_sqlite
      skip "Row-level locking not supported on SQLite" if SolidQueue::Record.connection.adapter_name.downcase.include?("sqlite")
    end
end
