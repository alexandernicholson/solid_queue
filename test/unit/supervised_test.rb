# frozen_string_literal: true

require "test_helper"
require "minitest/mock"

class SupervisedTest < ActiveSupport::TestCase
  class FakeProcess
    include SolidQueue::Processes::Supervised

    def stop
    end
  end

  test "forked processes exit immediately, without running inherited at-exit hooks" do
    reader, writer = IO.pipe

    pid = FakeProcess.new.send(:create_fork) do
      at_exit { writer.write("at_exit ran") }
      writer.write("block ran")
    end

    writer.close
    _, status = Process.waitpid2(pid)

    assert_equal 0, status.exitstatus
    assert_equal "block ran", reader.read
  ensure
    reader.close
  end

  test "Active Record forks leave application Mongoid clients alone" do
    reader, writer = IO.pipe

    SolidQueue::MongoidIntegration.stub(:after_fork!, -> { writer.write("mongoid reset, ") }) do
      pid = FakeProcess.new.send(:create_fork) { writer.write("block ran") }
      Process.waitpid(pid)
    end
    writer.close

    assert_equal "block ran", reader.read
  ensure
    reader.close
  end
end
