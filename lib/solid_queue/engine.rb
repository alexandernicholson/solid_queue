# frozen_string_literal: true

module SolidQueue
  class Engine < ::Rails::Engine
    isolate_namespace SolidQueue

    rake_tasks do
      load "solid_queue/tasks.rb"
    end

    config.solid_queue = ActiveSupport::OrderedOptions.new

    initializer "solid_queue.persistence", after: :load_environment_config, before: :set_load_path do
      SolidQueue.backend = config.solid_queue.backend if config.solid_queue.key?(:backend)
      SolidQueue.validate_backend!

      if SolidQueue.mongodb?
        model_root = root.join("lib/solid_queue/mongo/models").to_s
        Rails.autoloaders.main.ignore(root.join("app/models"))
        paths["app/models"] = model_root
        config.autoload_paths << model_root
        config.eager_load_paths << model_root
        require "solid_queue/mongo"
      end
    end

    initializer "solid_queue.config" do
      config.solid_queue.each do |name, value|
        SolidQueue.public_send("#{name}=", value)
      end
    end

    initializer "solid_queue.time_zone" do |app|
      unless config.solid_queue.key?(:time_zone)
        SolidQueue.time_zone = app.config.time_zone
      end
    end

    initializer "solid_queue.app_executor", before: :run_prepare_callbacks do |app|
      config.solid_queue.app_executor    ||= app.executor
      config.solid_queue.on_thread_error ||= ->(exception) { Rails.error.report(exception, handled: false) }

      SolidQueue.app_executor = config.solid_queue.app_executor
      SolidQueue.on_thread_error = config.solid_queue.on_thread_error
    end

    initializer "solid_queue.logger" do
      ActiveSupport.on_load(:solid_queue) do
        self.logger = ::Rails.logger if logger == SolidQueue::DEFAULT_LOGGER
      end

      SolidQueue::LogSubscriber.attach_to :solid_queue
    end

    initializer "solid_queue.active_job.extensions" do
      ActiveSupport.on_load :active_job do
        include ActiveJob::ConcurrencyControls
        include ActiveJob::Deduplication
        include ActiveJob::RunTimeLimit
        include ActiveJob::DeliveryModes

        if defined?(::ActiveRecord::Railtie)
          ActiveSupport.on_load :active_record do
            ActiveJob::Base.include ActiveJob::BatchId
          end
        else
          include ActiveJob::BatchId
        end
      end
    end

    initializer "solid_queue.bson_serializer", after: "solid_queue.active_job.extensions" do
      if SolidQueue.mongodb?
        require "active_job/serializers/bson_object_id_serializer"
        ActiveJob::Serializers.add_serializers(ActiveJob::Serializers::BsonObjectIdSerializer)
      end
    end

    initializer "solid_queue.deprecator" do |app|
      app.deprecators[:solid_queue] = SolidQueue.deprecator
    end
  end
end
