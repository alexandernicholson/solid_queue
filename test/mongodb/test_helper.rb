# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"

begin
  require "bundler/setup"
rescue Bundler::GemNotFound => error
  raise Bundler::GemNotFound,
    "#{error.message}\nInstall the MongoDB test bundle with BUNDLE_GEMFILE=gemfiles/mongodb.gemfile bundle install.",
    error.backtrace
end

begin
  require "mongo"
rescue LoadError => error
  raise LoadError,
    "MongoDB tests require the MongoDB Ruby driver. Run with BUNDLE_GEMFILE=gemfiles/mongodb.gemfile and bundle install.",
    error.backtrace
end
require "fileutils"
require "tmpdir"
require "minitest/autorun"
require "action_controller/railtie"
require "active_job/railtie"
require "solid_queue"
require "active_support/test_case"

MONGODB_TEST_URI = ENV.fetch("MONGODB_URI") do
  raise "Set MONGODB_URI to an isolated MongoDB database whose name ends in _test"
end

MONGODB_TEST_ROOT = Dir.mktmpdir("solid_queue-mongodb-test")
Minitest.after_run { FileUtils.remove_entry(MONGODB_TEST_ROOT) if File.exist?(MONGODB_TEST_ROOT) }

module SolidQueueMongoTestApplication
  class Application < Rails::Application
    config.eager_load = true
    config.logger = ActiveSupport::Logger.new(nil)
    config.root = MONGODB_TEST_ROOT
    config.active_job.queue_adapter = :solid_queue
    config.solid_queue.backend = :mongodb
    config.solid_queue.mongo_url = MONGODB_TEST_URI
  end
end

Rails.application.initialize!

unless SolidQueue::Mongo.client.database.name.end_with?("_test")
  raise "MONGODB_URI must select an isolated database whose name ends in _test"
end

SolidQueue::Mongo.prepare!

class MongoRaceJob < ActiveJob::Base
  def perform(*)
  end
end

class MongoTestCase < ActiveSupport::TestCase
  def setup
    SolidQueue::Mongo::COLLECTIONS.each do |name|
      SolidQueue::Mongo.collection(name).delete_many({})
    end
  end

  private
    def concurrently(count)
      ready = Queue.new
      start = Queue.new
      errors = Queue.new

      threads = count.times.map do
        Thread.new do
          ready << true
          start.pop
          yield
        rescue Exception => error
          errors << error
        end
      end

      count.times { ready.pop }
      count.times { start << true }
      threads.each(&:join)
      raise errors.pop unless errors.empty?
    end
end
