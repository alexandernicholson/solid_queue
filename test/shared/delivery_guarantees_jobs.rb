# frozen_string_literal: true

class SharedDeliveryJob < ActiveJob::Base
  class_attribute :recorder

  def perform(key)
    self.class.recorder&.call(key)
  end
end

class SharedInterruptedDeliveryJob < ActiveJob::Base
  class_attribute :starts, default: Concurrent::Array.new
  class_attribute :completions, default: Concurrent::Array.new

  def perform(key)
    self.class.starts << key
    Thread.current.kill if self.class.starts.count(key) == 1
    self.class.completions << key
  end
end

class SharedSweptDeliveryJob < ActiveJob::Base
  limits_run_time max: 0.1.seconds
  class_attribute :observer
  class_attribute :performed, default: Concurrent::Array.new

  def perform(key)
    self.class.performed << key
    sleep(5)
  rescue SolidQueue::Processes::RunTimeExceededError
    self.class.observer&.call(provider_job_id)
  end
end

class SharedAtMostOnceJob < ActiveJob::Base
  delivers :at_most_once
  class_attribute :starts, default: Concurrent::Array.new
  class_attribute :observer

  def perform(key, dying: false)
    self.class.starts << key
    self.class.observer&.call(self)
    Thread.current.kill if dying
  end
end

class SharedPerInstanceDeliveryJob < ActiveJob::Base
  def perform(*)
  end

  def delivery_mode
    arguments.first
  end
end

class SharedCappedDeathRetryJob < ActiveJob::Base
  retries_on_process_death attempts: 2

  def perform
  end
end

class SharedStrictDeathRetryJob < ActiveJob::Base
  retries_on_process_death attempts: 1

  def perform
  end
end
