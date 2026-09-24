# frozen_string_literal: true

class SharedExactlyOnceError < StandardError; end
class SharedExactlyOnceRetryableError < StandardError; end

module SharedExactlyOnceEffects
  extend self

  COLLECTION = "solid_queue_test_exactly_once_effects"

  def record(name)
    if SolidQueue.mongodb?
      SolidQueue::Mongo.client[COLLECTION].insert_one({ name: name }, session: SolidQueue.exactly_once_session)
    else
      ExactlyOnceEffect.create!(name: name)
    end
  end

  def count(name)
    if SolidQueue.mongodb?
      SolidQueue::Mongo.client[COLLECTION].count_documents(name: name)
    else
      ExactlyOnceEffect.where(name: name).count
    end
  end

  def count_elsewhere(name)
    Thread.new { SolidQueue.app_executor.wrap { count(name) } }.value
  end

  def clear
    if SolidQueue.mongodb?
      SolidQueue::Mongo.client[COLLECTION].delete_many({})
    else
      ExactlyOnceEffect.delete_all
    end
  end
end

class SharedExactlyOnceChildJob < ActiveJob::Base
  def perform(name)
    SharedExactlyOnceEffects.record("child-#{name}")
  end
end

class SharedExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  class_attribute :observer
  class_attribute :performed, default: Concurrent::Array.new

  def perform(name, raising: false, enqueuing: false)
    self.class.performed << name
    SharedExactlyOnceEffects.record(name)
    SharedExactlyOnceChildJob.perform_later(name) if enqueuing
    self.class.observer&.call(self)
    raise SharedExactlyOnceError, name if raising
  end
end

class SharedCrashingExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  class_attribute :starts, default: Concurrent::Array.new

  def perform(name)
    self.class.starts << name
    SharedExactlyOnceEffects.record(name)
    SharedExactlyOnceChildJob.perform_later(name)
    Thread.current.kill if self.class.starts.count(name) == 1
  end
end

class SharedRetriedExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  retry_on SharedExactlyOnceRetryableError, wait: 0, attempts: 3

  def perform(name)
    SharedExactlyOnceEffects.record(name)
    SharedExactlyOnceChildJob.perform_later(name)
    raise SharedExactlyOnceRetryableError, name if executions == 1
  end
end

class SharedDiscardedExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  discard_on SharedExactlyOnceError

  def perform(name)
    SharedExactlyOnceEffects.record(name)
    raise SharedExactlyOnceError, name
  end
end

class SharedLimitedExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  limits_concurrency to: 1, key: ->(*) { "exactly-once" }
  class_attribute :crashing, default: false

  def perform(name, raising: false)
    SharedExactlyOnceEffects.record(name)
    Thread.current.kill if self.class.crashing
    raise SharedExactlyOnceError, name if raising
  end
end

class SharedAlwaysDyingExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  class_attribute :starts, default: Concurrent::Array.new

  def perform(name)
    self.class.starts << name
    SharedExactlyOnceEffects.record(name)
    Thread.current.kill
  end
end

class SharedCappedDyingExactlyOnceJob < SharedAlwaysDyingExactlyOnceJob
  retries_on_process_death attempts: 1
end

class SharedLimitedRunTimeExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  limits_run_time max: 5.seconds

  def perform
  end
end

class SharedLongRunTimeExactlyOnceJob < ActiveJob::Base
  limits_run_time max: 4.hours
  delivers :exactly_once

  def perform
  end
end

class SharedExactlyOnceFollowUpJob < ActiveJob::Base
  def perform(name)
    SharedExactlyOnceEffects.record("follow-up-#{name}")
  end
end

class SharedReschedulingExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once

  def perform(name)
    ActiveJob::DeliveryModes.within_attempt do
      SharedExactlyOnceEffects.record(name)
      SharedExactlyOnceChildJob.perform_later(name)
      raise SharedExactlyOnceError, name
    end
  rescue SharedExactlyOnceError
    SharedExactlyOnceFollowUpJob.perform_later(name)
  end
end

class SharedNestedAttemptsExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once

  def perform(name)
    SharedExactlyOnceEffects.record("#{name}-before")
    ActiveJob::DeliveryModes.within_attempt do
      SharedExactlyOnceEffects.record("#{name}-outer")
      ActiveJob::DeliveryModes.within_attempt do
        SharedExactlyOnceEffects.record("#{name}-inner")
        raise SharedExactlyOnceError, name
      end
    rescue SharedExactlyOnceError
      SharedExactlyOnceEffects.record("#{name}-rescued")
    end
    SharedExactlyOnceEffects.record("#{name}-after")
  end
end

class SharedTimeoutReschedulingExactlyOnceJob < ActiveJob::Base
  delivers :exactly_once
  class_attribute :performed, default: Concurrent::Array.new

  def perform(name)
    self.class.performed << name
    sleep 10
  rescue SolidQueue::Processes::RunTimeExceededError
    SharedExactlyOnceFollowUpJob.perform_later(name)
  end
end

class SharedPlainAttemptJob < ActiveJob::Base
  def perform(name)
    ActiveJob::DeliveryModes.within_attempt do
      SharedExactlyOnceEffects.record(name)
      raise SharedExactlyOnceError, name
    end
  rescue SharedExactlyOnceError
  end
end

class SharedPlainEffectJob < ActiveJob::Base
  def perform(name, raising: false)
    SharedExactlyOnceEffects.record(name)
    raise SharedExactlyOnceError, name if raising
  end
end
