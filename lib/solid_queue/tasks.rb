namespace :solid_queue do
  desc "Install Solid Queue"
  task :install do
    Rails::Command.invoke :generate, [ "solid_queue:install", "--backend=#{solid_queue_backend}" ]
  end

  desc "Copy any new Solid Queue migrations to the application"
  task :update do
    Rails::Command.invoke :generate, [ "solid_queue:update", "--backend=#{solid_queue_backend}" ]
  end

  desc "start solid_queue supervisor to dispatch and process jobs"
  task start: :environment do
    SolidQueue::Supervisor.start
  end

  desc "validate the Solid Queue configuration for the current Rails env without starting any process"
  task check: :environment do
    configuration = SolidQueue::Configuration.new
    exit 1 unless configuration.check
  end

  desc "exit non-zero when ready jobs have waited longer than max_age seconds (default 300) in any queue"
  task :check_latency, [ :max_age ] => :environment do |_, args|
    max_age = Integer(args.with_defaults(max_age: 300)[:max_age])

    count, latency = SolidQueue.instrument(:check_latency, max_age: max_age) do |payload|
      payload[:count] = SolidQueue::ReadyExecution.count_waiting_longer_than(max_age)
      payload[:latency] = SolidQueue::ReadyExecution.latency
      payload.values_at(:count, :latency)
    end

    if count.zero?
      $stdout.puts "OK: no ready jobs have waited longer than #{max_age} seconds."
    else
      $stderr.puts "#{count} ready jobs have waited longer than #{max_age} seconds; the oldest has waited #{latency} seconds."
      exit 1
    end
  end

  desc "discard ready, scheduled, blocked and failed jobs in the given queue, or in every queue, leaving claimed jobs to finish"
  task :clear, [ :queue ] => :environment do |_, args|
    queue_name = args[:queue].presence

    discarded = [ SolidQueue::BlockedExecution, SolidQueue::ScheduledExecution, SolidQueue::ReadyExecution, SolidQueue::FailedExecution ].sum do |executions|
      queue_name ? executions.discard_all_in_queue(queue_name) : executions.discard_all_in_batches
    end

    $stdout.puts "Discarded #{discarded} jobs from #{queue_name ? "queue #{queue_name}" : "all queues"}."
  end

  desc "Prepare Solid Queue persistence"
  task prepare: :environment do
    if SolidQueue.mongodb?
      SolidQueue::Mongo.prepare!
    else
      Rake::Task["db:prepare"].invoke
    end
  end

  def solid_queue_backend
    ENV.fetch("SOLID_QUEUE_BACKEND") do
      (Rails.application.config.solid_queue.backend || SolidQueue.backend).to_s
    end
  end
end
