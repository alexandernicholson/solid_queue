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
