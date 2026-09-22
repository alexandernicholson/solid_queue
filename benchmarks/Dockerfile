FROM ruby:3.4-bookworm

RUN apt-get update \
  && apt-get install --yes --no-install-recommends build-essential libpq-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /work
ENV BUNDLE_GEMFILE=/work/benchmarks/Gemfile \
    BUNDLE_APP_CONFIG=/usr/local/bundle-config \
    BUNDLE_PATH=/usr/local/bundle

COPY benchmarks/Gemfile /work/benchmarks/Gemfile
COPY solid_queue.gemspec /work/solid_queue.gemspec
COPY lib/solid_queue/version.rb /work/lib/solid_queue/version.rb
RUN bundle install --jobs "$(nproc)" --retry 3

COPY app /work/app
COPY config /work/config
COPY lib /work/lib
COPY benchmarks/persistence.rb benchmarks/compare.rb benchmarks/environment.rb /work/benchmarks/

CMD ["bundle", "exec", "ruby", "benchmarks/persistence.rb"]
