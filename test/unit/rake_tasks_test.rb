# frozen_string_literal: true

require "test_helper"
require "rake"

class RakeTasksTest < ActiveSupport::TestCase
  setup do
    @previous_rake_application = Rake.application
    @rake = Rake::Application.new
    Rake.application = @rake
    Rake::Task.define_task(:environment)
    load File.expand_path("../../lib/solid_queue/tasks.rb", __dir__)
  end

  teardown do
    Rake.application = @previous_rake_application
  end

  test "solid_queue:check exits 0 and prints OK message for a valid configuration" do
    SolidQueue::Configuration.any_instance.stubs(:skip_recurring_tasks?).returns(true)

    out, err = capture_io do
      assert_nothing_raised { @rake["solid_queue:check"].invoke }
    end

    assert_match "Solid Queue configuration is valid.", out
    assert_empty err
  end

  test "solid_queue:check exits 1 and prints errors for an invalid configuration" do
    SolidQueue::Configuration.any_instance.stubs(:invalid_tasks).returns(
      [ stub(key: "broken", errors: stub(full_messages: [ "is invalid" ])) ]
    )
    SolidQueue::Configuration.any_instance.stubs(:skip_recurring_tasks?).returns(false)

    status = nil
    out, err = capture_io do
      begin
        @rake["solid_queue:check"].invoke
      rescue SystemExit => e
        status = e.status
      end
    end

    assert_equal 1, status
    assert_empty out
    assert_match "Solid Queue configuration is invalid:", err
    assert_match "broken", err
  end

  test "solid_queue:update generates for the backend selected in application configuration" do
    with_application_backend(:mongodb) do
      Rails::Command.expects(:invoke).with(:generate, [ "solid_queue:update", "--backend=mongodb" ])

      @rake["solid_queue:update"].invoke
    end
  end

  test "SOLID_QUEUE_BACKEND overrides the configured backend for solid_queue:install" do
    with_application_backend(:mongodb) do
      Rails::Command.expects(:invoke).with(:generate, [ "solid_queue:install", "--backend=active_record" ])

      with_env("SOLID_QUEUE_BACKEND" => "active_record") { @rake["solid_queue:install"].invoke }
    end
  end

  private
    def with_application_backend(backend)
      Rails.application.config.solid_queue.backend = backend
      yield
    ensure
      Rails.application.config.solid_queue.delete(:backend)
    end

    def with_env(values)
      previous = values.keys.index_with { |key| ENV[key] }
      values.each { |key, value| ENV[key] = value }
      yield
    ensure
      previous.each { |key, value| ENV[key] = value }
    end
end
