# frozen_string_literal: true

require "test_helper"
require "open3"

class PersistenceBootTest < ActiveSupport::TestCase
  test "eager loading an Active Record application does not load MongoDB support" do
    output = run_in_eager_loaded_application <<~RUBY
      puts "mongo_support=\#{SolidQueue.const_defined?(:Mongo, false)}"
      puts "mongo_driver=\#{$LOADED_FEATURES.any? { |feature| feature.end_with?("/lib/mongo.rb") }}"
    RUBY

    assert_includes output, "mongo_support=false"
    assert_includes output, "mongo_driver=false"
  end

  test "settings assigned in application initializers are applied" do
    output = run_in_eager_loaded_application <<~RUBY, initializer: "Rails.application.config.solid_queue.clear_finished_jobs_after = 42.minutes"
      puts "clear_finished_jobs_after=\#{SolidQueue.clear_finished_jobs_after.inspect}"
    RUBY

    assert_includes output, "clear_finished_jobs_after=42 minutes"
  end

  private
    def run_in_eager_loaded_application(script, initializer: nil)
      Dir.mktmpdir do |dir|
        env = { "CI" => "1", "RAILS_ENV" => "test" }
        if initializer
          path = File.join(dir, "initializer.rb")
          File.write(path, initializer)
          env["SOLID_QUEUE_TEST_INITIALIZER"] = path
        end

        output, status = Open3.capture2e(
          env, Gem.ruby, File.expand_path("../../bin/rails", __dir__), "runner", script,
          chdir: File.expand_path("../..", __dir__)
        )

        assert status.success?, "Application failed to boot:\n#{output}"
        output
      end
    end
end
