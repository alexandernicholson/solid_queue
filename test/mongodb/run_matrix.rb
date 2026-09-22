#!/usr/bin/env ruby
# frozen_string_literal: true

require "pathname"

ROOT = Pathname(__dir__).join("../..").expand_path.freeze
RUBY_VERSIONS = ENV.fetch("RUBY_VERSIONS", "3.2,3.3,3.4,4.0").split(",").freeze
RAILS_VERSIONS = ENV.fetch("RAILS_VERSIONS", "7.1,7.2,8.0,8.1").split(",").freeze
MONGO_DRIVER_VERSIONS = ENV.fetch("MONGO_DRIVER_VERSIONS", "2.24,2.26").split(",").freeze
MONGO_CONTAINER = "solid-queue-mongodb-matrix-#{Process.pid}"
FAULT_APP_NAME = "solid-queue-fault-tests"
NATIVE_TEST_COMMAND = [
  "bundle", "exec", "ruby", "-Itest", "-e",
  'Dir["test/mongodb/**/*_test.rb"].sort.each { |file| require File.expand_path(file) }'
].freeze
MONGOID_TEST_COMMAND = [
  "bundle", "exec", "ruby", "-Itest", "-e",
  'Dir["test/mongoid/**/*_test.rb"].sort_by { |file| [ File.basename(file) == "arguments_test.rb" ? 1 : 0, file ] }.each { |file| require File.expand_path(file) }'
].freeze

def run!(*command, **options)
  puts "+ #{command.join(" ")}"
  return if system(*command, **options)

  abort "Command failed with status #{$?.exitstatus}: #{command.join(" ")}"
end

def wait_for_mongodb!(javascript, description)
  60.times do
    return if system(
      "docker", "exec", MONGO_CONTAINER, "mongosh", "--quiet", "--eval", javascript,
      out: File::NULL, err: File::NULL
    )

    sleep 1
  end

  system("docker", "logs", MONGO_CONTAINER)
  abort "MongoDB did not become #{description} within 60 seconds"
end

def image_name(ruby_version, rails_version, driver_version)
  "solid-queue-mongodb:ruby#{ruby_version}-rails#{rails_version}-driver#{driver_version}"
end

def database_name(ruby_version, rails_version, driver_version, suite)
  [ "solid_queue", "ruby#{ruby_version}", "rails#{rails_version}", "driver#{driver_version}", suite, "test" ]
    .join("_").tr(".", "_")
end

def run_suite(image, database, command)
  uri = "mongodb://127.0.0.1:27017/#{database}?replicaSet=rs0&appName=#{FAULT_APP_NAME}"
  arguments = [
    "docker", "run", "--rm",
    "--network", "container:#{MONGO_CONTAINER}",
    "--mount", "type=bind,source=#{ROOT.join("app")},target=/work/app,readonly",
    "--mount", "type=bind,source=#{ROOT.join("config")},target=/work/config,readonly",
    "--mount", "type=bind,source=#{ROOT.join("lib")},target=/work/lib,readonly",
    "--mount", "type=bind,source=#{ROOT.join("test")},target=/work/test,readonly",
    "--mount", "type=bind,source=#{ROOT.join("solid_queue.gemspec")},target=/work/solid_queue.gemspec,readonly",
    "--env", "MONGODB_URI=#{uri}",
    "--env", "MONGODB_FAULT_TESTS=1",
    image,
    *command
  ]
  puts "+ #{arguments.join(" ")}"
  system(*arguments)
end

at_exit do
  system("docker", "rm", "--force", MONGO_CONTAINER, out: File::NULL, err: File::NULL)
end

run!(
  "docker", "run", "--detach", "--name", MONGO_CONTAINER, "--ulimit", "nofile=65536:65536",
  "mongo:8.0", "mongod", "--replSet", "rs0", "--bind_ip_all",
  "--setParameter", "enableTestCommands=1"
)
wait_for_mongodb!("quit(db.adminCommand({ ping: 1 }).ok ? 0 : 1)", "reachable")
run!(
  "docker", "exec", MONGO_CONTAINER, "mongosh", "--quiet", "--eval",
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})'
)
wait_for_mongodb!("quit(db.hello().isWritablePrimary ? 0 : 1)", "writable")

failures = []
RUBY_VERSIONS.product(RAILS_VERSIONS, MONGO_DRIVER_VERSIONS).each do |ruby_version, rails_version, driver_version|
  image = image_name(ruby_version, rails_version, driver_version)
  build = [
    "docker", "build", "--file", ROOT.join("test/mongodb/Dockerfile").to_s,
    "--build-arg", "RUBY_VERSION=#{ruby_version}",
    "--build-arg", "RAILS_VERSION=~> #{rails_version}.0",
    "--build-arg", "MONGO_DRIVER_VERSION=~> #{driver_version}.0",
    "--tag", image,
    ROOT.to_s
  ]
  puts "+ #{build.join(" ")}"
  unless system(*build)
    failures << "#{image}: build (#{$?.exitstatus})"
    next
  end

  { native: NATIVE_TEST_COMMAND, mongoid: MONGOID_TEST_COMMAND }.each do |suite, command|
    database = database_name(ruby_version, rails_version, driver_version, suite)
    failures << "#{image}: #{suite} (#{$?.exitstatus})" unless run_suite(image, database, command)
    abort "Matrix MongoDB stopped during #{image} #{suite}" unless system(
      "docker", "exec", MONGO_CONTAINER, "mongosh", "--quiet", "--eval",
      "quit(db.hello().isWritablePrimary ? 0 : 1)", out: File::NULL, err: File::NULL
    )
  end
end

abort "Matrix failures:\n#{failures.join("\n")}" if failures.any?
puts "All #{RUBY_VERSIONS.length * RAILS_VERSIONS.length * MONGO_DRIVER_VERSIONS.length} matrix lanes passed."
