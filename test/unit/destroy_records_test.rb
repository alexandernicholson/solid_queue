# frozen_string_literal: true

require "test_helper"

class DestroyRecordsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  test "destroy_records removes batches left by a test" do
    SolidQueue::Batch.enqueue { AddToBufferJob.perform_later("left over") }

    destroy_records

    assert_equal 0, SolidQueue::Batch.count
  end
end
