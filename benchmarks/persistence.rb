# frozen_string_literal: true

# Run each backend in a fresh process, against an isolated Docker database.
# SQL:   DATABASE_URL=postgresql://.../solid_queue_benchmark BACKEND=active_record bundle exec ruby benchmarks/persistence.rb
# Mongo: MONGODB_URI=mongodb://.../solid_queue_benchmark?replicaSet=sqMongo BACKEND=mongodb bundle exec ruby benchmarks/persistence.rb
require "bundler/setup"
require "json"
require "tmpdir"
require "fileutils"
require "rails"
require "active_job/railtie"

backend = ENV.fetch("BACKEND").to_sym
raise ArgumentError, "BACKEND must be active_record or mongodb" unless %i[ active_record mongodb ].include?(backend)

ENV["RAILS_ENV"] = "benchmark"
require "active_record/railtie" if backend == :active_record
require_relative "../lib/solid_queue"

root = Dir.mktmpdir("solid-queue-benchmark-")
application = Class.new(Rails::Application) do
  config.root = root
  config.eager_load = false
  config.secret_key_base = "benchmark-only-not-a-deployment-secret"
  config.logger = ActiveSupport::Logger.new(File::NULL)
  config.active_job.queue_adapter = :solid_queue
  config.solid_queue.backend = backend
  config.solid_queue.logger = config.logger
end
PersistenceBenchmarkApplication = application

if backend == :active_record
  url = ENV.fetch("DATABASE_URL")
  raise "Use an isolated solid_queue_benchmark database" unless URI(url).path.start_with?("/solid_queue_benchmark")

  application.config.define_singleton_method(:database_configuration) do
    { "benchmark" => { "primary" => { "adapter" => "postgresql", "url" => url, "pool" => 20 } } }
  end
else
  application.config.solid_queue.mongo_database = "solid_queue_benchmark"
end

application.initialize!
ActiveJob::Base.logger = ActiveSupport::Logger.new(File::NULL)

if backend == :active_record
  ActiveRecord::Base.establish_connection
  ActiveRecord::Schema.verbose = false
  load File.expand_path("../lib/generators/solid_queue/install/templates/db/queue_schema.rb", __dir__)
else
  SolidQueue::Mongo.prepare!
end

class PersistenceBenchmarkJob < ActiveJob::Base
  def perform(*)
  end
end

class ContendedPersistenceBenchmarkJob < PersistenceBenchmarkJob
  limits_concurrency to: 4, key: ->(key, *) { key }
end

class PersistenceBenchmark
  def initialize(backend)
    @backend = backend
    @jobs = Integer(ENV.fetch("JOBS", "2000"))
    @repetitions = Integer(ENV.fetch("REPETITIONS", "3"))
    @workers = Integer(ENV.fetch("WORKERS", "8"))
    @payload = "x" * Integer(ENV.fetch("PAYLOAD_BYTES", "256"))
    @retries = 0
    @retry_mutex = Mutex.new
    @subscription = ActiveSupport::Notifications.subscribe(/transaction_retry\.solid_queue\z/) do
      @retry_mutex.synchronize { @retries += 1 }
    end
  end

  def run
    # Warm connection pools, schema caches, serializers and job class resolution.
    clear
    SolidQueue::Job.enqueue_all(build_jobs(100))
    SolidQueue::ReadyExecution.claim([ "default" ], 100, register_process.id)

    samples = []
    @repetitions.times do |iteration|
      samples << enqueue_sample(iteration, bulk: false)
      samples << enqueue_sample(iteration, bulk: true)
      [ 1, 10, 100 ].each { |size| samples << claim_sample(iteration, batch_size: size) }
      samples << contention_sample(iteration)
    end
    {
      backend: @backend, ruby: RUBY_VERSION, active_job: ActiveJob.version.to_s,
      jobs: @jobs, workers: @workers, payload_bytes: @payload.bytesize,
      repetitions: @repetitions, samples: samples
    }
  ensure
    ActiveSupport::Notifications.unsubscribe(@subscription)
    clear
  end

  private
    def clock
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end

    def build_jobs(count, klass: PersistenceBenchmarkJob)
      Array.new(count) { |i| klass.new("key-#{i % 8}", @payload) }
    end

    def register_process
      SolidQueue::Process.register(kind: "Worker", name: SecureRandom.uuid,
        pid: ::Process.pid, hostname: "benchmark", metadata: {})
    end

    def clear
      if @backend == :mongodb
        SolidQueue::Mongo.client.database.collection_names.grep(/\Asolid_queue_/).each do |name|
          SolidQueue::Mongo.client[name].delete_many({})
        end
      else
        tables = ActiveRecord::Base.connection.tables.grep(/\Asolid_queue_/)
        unless tables.empty?
          quoted = tables.map { |table| ActiveRecord::Base.connection.quote_table_name(table) }
          ActiveRecord::Base.connection.execute("TRUNCATE #{quoted.join(', ')} RESTART IDENTITY CASCADE")
        end
      end
    end

    def enqueue_sample(iteration, bulk:)
      clear
      jobs = build_jobs(@jobs)
      before_retries = @retries
      started = clock
      if bulk
        jobs.each_slice(100) { |batch| SolidQueue::Job.enqueue_all(batch) }
      else
        jobs.each { |job| SolidQueue::Job.enqueue(job) }
      end
      duration = clock - started
      raise "Enqueue lost jobs" unless jobs.all?(&:successfully_enqueued?)

      sample(iteration, bulk ? "enqueue_bulk_100" : "enqueue_single", @jobs, duration,
        transaction_retries: @retries - before_retries)
    end

    def claim_sample(iteration, batch_size:)
      clear
      SolidQueue::Job.enqueue_all(build_jobs(@jobs))
      process_ids = Array.new(@workers) { register_process.id }
      before_retries = @retries
      started = clock
      workers = process_ids.map do |process_id|
        Thread.new do
          ids = []
          timings = []
          loop do
            before = clock
            claimed = SolidQueue::ReadyExecution.claim([ "default" ], batch_size, process_id)
            timings << (clock - before) * 1000
            break if claimed.empty? && SolidQueue::ReadyExecution.aggregated_count_across([ "default" ]).zero?

            ids.concat(claimed.map(&:job_id))
          end
          [ ids, timings ]
        ensure
          ActiveRecord::Base.connection_handler.clear_active_connections! if @backend == :active_record
        end
      end
      results = workers.map(&:value)
      duration = clock - started
      ids = results.flat_map(&:first)
      raise "Lost or duplicate claims" unless ids.length == @jobs && ids.uniq.length == @jobs

      timings = results.flat_map(&:last).sort
      sample(iteration, "claim_#{batch_size}", ids.length, duration,
        p99_claim_ms: timings.fetch([(timings.size * 0.99).ceil - 1, 0].max),
        claim_calls: timings.size, transaction_retries: @retries - before_retries)
    end

    def contention_sample(iteration)
      clear
      before_retries = @retries
      jobs = build_jobs(@jobs, klass: ContendedPersistenceBenchmarkJob)
      started = clock
      jobs.each_slice((@jobs.to_f / @workers).ceil).map do |slice|
        Thread.new { slice.each { |job| SolidQueue::Job.enqueue(job) } }
      end.each(&:value)
      duration = clock - started
      raise "Concurrency enqueue lost jobs" unless jobs.all?(&:successfully_enqueued?)

      sample(iteration, "enqueue_contended_8_keys", @jobs, duration,
        transaction_retries: @retries - before_retries,
        retries_per_job: (@retries - before_retries).to_f / @jobs)
    end

    def sample(iteration, scenario, count, duration, **extra)
      { iteration: iteration, scenario: scenario, jobs: count, seconds: duration,
        jobs_per_second: count / duration, **extra }
    end
end

begin
  puts JSON.pretty_generate(PersistenceBenchmark.new(backend).run)
ensure
  SolidQueue::Mongo.disconnect! if backend == :mongodb
  FileUtils.remove_entry(root)
end
