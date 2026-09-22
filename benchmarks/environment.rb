# frozen_string_literal: true

require "bundler/setup"
require "active_job"
require "json"
require "mongo"
require "pg"
require "rails"
require "solid_queue/version"

postgres = PG.connect(ENV.fetch("DATABASE_URL"))
mongodb = Mongo::Client.new(ENV.fetch("MONGODB_URI"))

begin
  postgres_version = postgres.exec("SHOW server_version").getvalue(0, 0)
  postgres_version_number = postgres.exec("SHOW server_version_num").getvalue(0, 0)
  mongodb_version = mongodb.database.command(buildInfo: 1).first.fetch("version")
  hello = mongodb.database.command(hello: 1).first
  replica_status = mongodb.use("admin").database.command(replSetGetStatus: 1).first

  puts JSON.pretty_generate(
    client: {
      image: "ruby:3.4-bookworm",
      ruby: RUBY_VERSION,
      rails: Rails.version,
      active_job: ActiveJob.version.to_s,
      solid_queue: SolidQueue::VERSION,
      pg_driver: PG::VERSION,
      mongo_driver: Mongo::VERSION
    },
    postgres: {
      image: "postgres:16",
      server: postgres_version,
      server_version_number: postgres_version_number,
      cpu_limit: 2,
      memory_limit: "2g"
    },
    mongodb: {
      image: "mongo:8.0",
      server: mongodb_version,
      topology: "single-node replica set",
      replica_set: hello.fetch("setName"),
      members: replica_status.fetch("members").size,
      write_durability: "majority with journal; no multi-node replication latency",
      cpu_limit: 2,
      memory_limit: "2g"
    }
  )
ensure
  postgres&.close
  mongodb&.close
end
