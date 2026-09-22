# frozen_string_literal: true

class SolidQueue::InstallGenerator < Rails::Generators::Base
  source_root File.expand_path("templates", __dir__)
  class_option :backend, type: :string, default: "active_record",
    enum: %w[ active_record mongodb ],
    desc: "Persistence backend. Defaults to `active_record`"

  MONGODB_NOTICE = <<~NOTICE.freeze
    MongoDB support is experimental. Review the generated configuration, then run
    bin/rails solid_queue:prepare before starting Solid Queue processes.
  NOTICE


  def copy_files
    say_status :experimental, MONGODB_NOTICE, :yellow if mongodb?
    gem "mongo", ">= 2.24", "< 3" if mongodb?
    template "config/queue.yml"
    template "config/recurring.yml"
    template "db/queue_schema.rb" unless mongodb?
    template "bin/jobs"
    chmod "bin/jobs", 0755 & ~File.umask, verbose: false
  end

  def configure_adapter_and_database
    pathname = Pathname(destination_root).join("config/environments/production.rb")

    gsub_file pathname, /\n\s*config\.solid_queue\.(?:connects_to|backend)\s+=.*\n/, "\n", verbose: false
    replacement = +"  config.active_job.queue_adapter = :solid_queue\n"
    replacement << if mongodb?
      "  config.solid_queue.backend = :mongodb\n"
    else
      "  config.solid_queue.connects_to = { database: { writing: :queue } }\n"
    end

    gsub_file pathname, /(# )?config\.active_job\.queue_adapter\s+=.*\n/, replacement
  end

  private
    def mongodb?
      options[:backend] == "mongodb"
    end
end
