# frozen_string_literal: true

class SharedDeduplicatedJob < ActiveJob::Base
  deduplicates key: ->(key, *) { key }
  class_attribute :performed, default: []

  def perform(key, label = key)
    self.class.performed += [ label ]
  end
end

class SharedWindowedDeduplicatedJob < ActiveJob::Base
  deduplicates key: ->(key) { key }, duration: 1.hour

  def perform(*)
  end
end

class SharedRetriedDeduplicatedJob < ActiveJob::Base
  class RetryableError < StandardError; end

  deduplicates key: ->(key) { key }
  retry_on RetryableError, wait: 0, attempts: 2
  class_attribute :attempts, default: 0

  def perform(*)
    self.class.attempts += 1
    raise RetryableError if self.class.attempts == 1
  end
end

class SharedFailingDeduplicatedJob < ActiveJob::Base
  deduplicates key: ->(key) { key }

  def perform(*)
    raise "deduplicated failure"
  end
end

class SharedLimitedDeduplicatedJob < ActiveJob::Base
  deduplicates key: ->(key) { key }
  limits_concurrency key: ->(*) { "shared" }, to: 1, on_conflict: :discard

  def perform(*)
  end
end

class SharedInterruptedDeduplicatedJob < ActiveJob::Base
  deduplicates key: ->(*) { "interrupted" }

  def perform(process_id)
    SolidQueue::ClaimedExecution.release_for_process(process_id)
  end
end
