# frozen_string_literal: true

require "rails/generators"

class SolidQueue::UpdateGenerator < Rails::Generators::Base
  source_root File.expand_path("templates", __dir__)

  class_option :database, type: :string, aliases: %i[ --db ], default: "queue",
    desc: "The database that Solid Queue uses. Defaults to `queue`"

  class_option :backend, type: :string, default: "active_record",
    enum: %w[ active_record mongodb ],
    desc: "Persistence backend. Defaults to `active_record`"

  def copy_new_migrations
    return if options[:backend] == "mongodb"
    require "rails/generators/active_record"
    self.class.include ActiveRecord::Generators::Migration
    Dir.glob(File.join(self.class.source_root, "db", "*.rb")).each do |migration_file|
      name = File.basename(migration_file)
      migration_template File.join("db", name), File.join(db_migrate_path, name), skip: true
    end
  end
end
