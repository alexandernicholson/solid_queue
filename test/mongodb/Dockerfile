# syntax=docker/dockerfile:1

ARG RUBY_VERSION
FROM ruby:${RUBY_VERSION}
ARG RUBY_VERSION

ARG RAILS_VERSION
ARG MONGO_DRIVER_VERSION
ARG MONGOID_VERSION=
ARG MONGOID_GIT=
ARG MONGOID_REF=

ENV BUNDLE_GEMFILE=/work/gemfiles/mongodb.gemfile \
    BUNDLE_PATH=/usr/local/bundle \
    RAILS_VERSION=${RAILS_VERSION} \
    MONGO_DRIVER_VERSION=${MONGO_DRIVER_VERSION} \
    MONGOID_VERSION=${MONGOID_VERSION} \
    MONGOID_GIT=${MONGOID_GIT} \
    MONGOID_REF=${MONGOID_REF}

WORKDIR /work
COPY solid_queue.gemspec MIT-LICENSE README.md UPGRADING.md ./
COPY gemfiles/mongodb.gemfile gemfiles/mongodb.gemfile
COPY lib/solid_queue/version.rb lib/solid_queue/version.rb
RUN --mount=type=cache,id=solid-queue-ruby-${RUBY_VERSION}-gem-cache,target=/usr/local/bundle/cache,sharing=locked \
    bundle install --jobs 4 --retry 3
COPY app app
COPY config config
COPY lib lib
COPY test test

CMD ["bundle", "exec", "ruby", "-Itest", "-e", "Dir[\"test/mongodb/**/*_test.rb\"].sort.each { |file| require File.expand_path(file) }"]
