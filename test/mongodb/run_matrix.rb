#!/usr/bin/env ruby
# frozen_string_literal: true

require "pathname"

ROOT = Pathname(__dir__).join("../..").expand_path.freeze
LANES = {
  "pinned" => {
    ruby: "4.0.2", rails: "8.0.5.1", driver: "2.26.0", mongodb: "8.0"
  },
  "latest" => {
    ruby: "4.0.7", rails: "8.1.3.1", driver: "2.26.0", mongodb: "8.3"
  }
}.freeze
SELECTED_LANES = ENV.fetch("LANES", LANES.keys.join(",")).split(",").freeze
FAULT_APP_NAME = "solid-queue-fault-tests"
NATIVE_TEST_COMMAND = [
  "bundle", "exec", "ruby", "-Itest", "-e",
  'Dir["test/mongodb/**/*_test.rb"].sort.each { |file| require File.expand_path(file) }'
].freeze

def run!(*command, **options)
  puts "+ #{command.join(" ")}"
  return if system(*command, **options)

  abort "Command failed with status #{$?.exitstatus}: #{command.join(" ")}"
end

def wait_for_mongodb!(container, javascript, description, port: 27017)
  60.times do
    return if system(
      "docker", "exec", container, "mongosh", "--port", port.to_s, "--quiet", "--eval", javascript,
      out: File::NULL, err: File::NULL
    )

    sleep 1
  end

  system("docker", "logs", container)
  abort "MongoDB did not become #{description} within 60 seconds"
end

def start_mongodb(name, version)
  replica_set = "solid-queue-mongodb-#{name}-#{Process.pid}"
  standalone = "solid-queue-mongodb-standalone-#{name}-#{Process.pid}"
  at_exit { system("docker", "rm", "--force", replica_set, standalone, out: File::NULL, err: File::NULL) }

  run!(
    "docker", "run", "--detach", "--name", replica_set, "--ulimit", "nofile=65536:65536",
    "mongo:#{version}", "mongod", "--replSet", "rs0", "--bind_ip_all",
    "--setParameter", "enableTestCommands=1"
  )
  wait_for_mongodb!(replica_set, "quit(db.adminCommand({ ping: 1 }).ok ? 0 : 1)", "reachable")
  run!(
    "docker", "exec", replica_set, "mongosh", "--quiet", "--eval",
    'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})'
  )
  wait_for_mongodb!(replica_set, "quit(db.hello().isWritablePrimary ? 0 : 1)", "writable")
  run!(
    "docker", "run", "--detach", "--name", standalone, "--network", "container:#{replica_set}",
    "mongo:#{version}", "mongod", "--port", "27018", "--bind_ip_all"
  )
  wait_for_mongodb!(standalone, "quit(db.adminCommand({ ping: 1 }).ok ? 0 : 1)", "reachable", port: 27018)
  replica_set
end

def run_suite(container, image, database, command)
  arguments = [
    "docker", "run", "--rm",
    "--network", "container:#{container}",
    "--mount", "type=bind,source=#{ROOT.join("app")},target=/work/app,readonly",
    "--mount", "type=bind,source=#{ROOT.join("config")},target=/work/config,readonly",
    "--mount", "type=bind,source=#{ROOT.join("lib")},target=/work/lib,readonly",
    "--mount", "type=bind,source=#{ROOT.join("test")},target=/work/test,readonly",
    "--mount", "type=bind,source=#{ROOT.join("solid_queue.gemspec")},target=/work/solid_queue.gemspec,readonly",
    "--env", "MONGODB_URI=mongodb://127.0.0.1:27017/#{database}?replicaSet=rs0&appName=#{FAULT_APP_NAME}",
    "--env", "MONGODB_FAULT_TESTS=1",
    "--env", "MONGODB_STANDALONE_URI=mongodb://127.0.0.1:27018/#{database}_standalone?directConnection=true",
    image,
    *command
  ]
  puts "+ #{arguments.join(" ")}"
  system(*arguments)
end

failures = []
SELECTED_LANES.each do |name|
  lane = LANES.fetch(name) { abort "Unknown lane #{name.inspect}; choose from #{LANES.keys.join(", ")}" }
  image = "solid-queue-mongodb:#{name}"
  build = [
    "docker", "build", "--file", ROOT.join("test/mongodb/Dockerfile").to_s,
    "--build-arg", "RUBY_VERSION=#{lane[:ruby]}",
    "--build-arg", "RAILS_VERSION=#{lane[:rails]}",
    "--build-arg", "MONGO_DRIVER_VERSION=#{lane[:driver]}",
    "--tag", image,
    ROOT.to_s
  ]
  puts "+ #{build.join(" ")}"
  unless system(*build)
    failures << "#{name}: build (#{$?.exitstatus})"
    next
  end

  container = start_mongodb(name, lane[:mongodb])
  failures << "#{name}: native (#{$?.exitstatus})" unless run_suite(container, image, "solid_queue_#{name}_test", NATIVE_TEST_COMMAND)
end

abort "Matrix failures:\n#{failures.join("\n")}" if failures.any?
puts "All #{SELECTED_LANES.size} lanes passed: #{SELECTED_LANES.join(", ")}."
