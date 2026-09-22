# frozen_string_literal: true

require "open3"
require "rbconfig"
require "timeout"

module MongoRuntimeSupport
  RESULT_COLLECTION = "solid_queue_runtime_results"

  class RuntimeJob < ActiveJob::Base
    queue_as :runtime

    def perform(token, gate: nil)
      collection = SolidQueue::Mongo.client[RESULT_COLLECTION]
      collection.insert_one(token: token, event: "started", pid: Process.pid, thread_id: Thread.current.object_id, fiber_id: Fiber.current.object_id, client_id: SolidQueue::Mongo.client.object_id)

      if gate
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
        until collection.find(gate: gate, released: true).limit(1).first
          raise Timeout::Error, "runtime gate #{gate} was not released" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          sleep 0.01
        end
      end

      collection.insert_one(token: token, event: "completed", pid: Process.pid, thread_id: Thread.current.object_id, fiber_id: Fiber.current.object_id)
    end
  end

  private
    def runtime_results
      SolidQueue::Mongo.client[RESULT_COLLECTION]
    end

    def enqueue_runtime_job(token, gate: nil)
      RuntimeJob.perform_later(token, gate: gate)
    end

    def wait_until(timeout: 10)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      until yield
        raise Timeout::Error, "condition was not met within #{timeout} seconds" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        sleep 0.01
      end
    end

    def wait_for_runtime_event(token, event, timeout: 10)
      wait_until(timeout: timeout) { runtime_results.find(token: token, event: event).limit(1).first }
      runtime_results.find(token: token, event: event).first
    end

    def wait_for_pid(pid, timeout: 10)
      status = nil
      wait_until(timeout: timeout) do
        waited = Process.waitpid2(pid, Process::WNOHANG)
        status = waited&.last
      rescue Errno::ECHILD
        true
      end
      status
    end

    def stop_pid(pid, signal: :TERM)
      Process.kill(signal, pid)
      wait_for_pid(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end

    def run_worker(mode:, fibers: nil, threads: nil)
      options = { queues: [ "runtime" ], polling_interval: 0.01 }
      options[:fibers] = fibers if fibers
      options[:threads] = threads if threads
      SolidQueue::Worker.new(**options).tap { |worker| worker.mode = mode }
    end

    def run_cli_check
      script = <<~'RUBY'
        require "bundler/setup"
        require "tmpdir"
        require "pathname"
        require "action_controller/railtie"
        require "active_job/railtie"
        require "solid_queue"

        class MongoCliRuntimeApplication < Rails::Application
          config.root = Pathname.new(Dir.mktmpdir("solid-queue-mongo-cli"))
          config.eager_load = true
          config.logger = ActiveSupport::Logger.new(nil)
          config.secret_key_base = "mongo-cli-runtime"
          config.active_job.queue_adapter = :solid_queue
          config.solid_queue.backend = :mongodb
          config.solid_queue.mongo_url = ENV.fetch("MONGODB_URI")
        end

        MongoCliRuntimeApplication.initialize!
        abort "Active Record was loaded" if defined?(ActiveRecord::Base)
        require "solid_queue/cli"
        SolidQueue::Cli.start(["check", "--skip-recurring"])
      RUBY

      Open3.capture3({ "MONGODB_URI" => MONGODB_TEST_URI }, RbConfig.ruby, "-Ilib", "-e", script, chdir: File.expand_path("../..", __dir__))
    end
end
