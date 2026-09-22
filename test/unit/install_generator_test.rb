# frozen_string_literal: true

require "test_helper"
require "rails/generators/test_case"
require "generators/solid_queue/install/install_generator"

class InstallGeneratorTest < Rails::Generators::TestCase
  tests SolidQueue::InstallGenerator
  destination Rails.root.join("tmp/install_generator_test")
  setup :prepare_destination

  test "MongoDB install configures a driver-only application and explains preparation" do
    prepare_application

    output, = capture_io { run_generator %w[ --backend mongodb ] }

    assert_file "Gemfile" do |contents|
      assert_match(/gem "mongo", ">= 2\.24", "< 3"/, contents)
    end
    assert_file "config/queue.yml"
    assert_file "config/recurring.yml"
    assert_file "bin/jobs"
    assert_no_file "db/queue_schema.rb"
    assert_file "config/environments/production.rb" do |contents|
      assert_match(/config\.active_job\.queue_adapter = :solid_queue/, contents)
      assert_match(/config\.solid_queue\.backend = :mongodb/, contents)
      assert_no_match(/connects_to/, contents)
    end
    assert_match(/MongoDB support is experimental/, output)
    assert_match(%r{bin/rails solid_queue:prepare}, output)
  end

  test "default install preserves the Active Record setup" do
    prepare_application

    run_generator

    assert_file "db/queue_schema.rb"
    assert_file "Gemfile" do |contents|
      assert_no_match(/gem "mongo"/, contents)
    end
    assert_file "config/environments/production.rb" do |contents|
      assert_match(/config\.active_job\.queue_adapter = :solid_queue/, contents)
      assert_match(/config\.solid_queue\.connects_to = \{ database: \{ writing: :queue \} \}/, contents)
      assert_no_match(/config\.solid_queue\.backend/, contents)
    end
  end

  private
    def prepare_application
      production_config = File.join(destination_root, "config/environments/production.rb")
      FileUtils.mkdir_p File.dirname(production_config)
      File.write File.join(destination_root, "Gemfile"), "source \"https://rubygems.org\"\n"
      File.write production_config, <<~RUBY
        Rails.application.configure do
          # config.active_job.queue_adapter = :async
        end
      RUBY
    end
end
