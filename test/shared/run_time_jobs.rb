# frozen_string_literal: true

class SharedRunTimeLimitedJob < ActiveJob::Base
  limits_run_time max: 10.minutes
  class_attribute :observer

  def perform(duration = 0)
    self.class.observer&.call(provider_job_id)
    sleep(duration)
  end
end

class SharedShortRunTimeJob < ActiveJob::Base
  limits_run_time max: 0.1.seconds

  def perform(duration)
    sleep(duration)
  end
end

class SharedUnlimitedRunTimeJob < ActiveJob::Base
  class_attribute :observer

  def perform(duration = 0)
    self.class.observer&.call(provider_job_id)
    sleep(duration)
  end
end

class SharedStubbornRunTimeJob < ActiveJob::Base
  limits_run_time max: 0.1.seconds
  class_attribute :observer

  def perform
    sleep(5)
  rescue SolidQueue::Processes::RunTimeExceededError
    self.class.observer&.call(provider_job_id)
  end
end

class SharedRetriedRunTimeJob < ActiveJob::Base
  limits_run_time max: 0.1.seconds
  retry_on SolidQueue::Processes::RunTimeExceededError, wait: 0, attempts: 2
  class_attribute :attempts, default: 0

  def perform
    self.class.attempts += 1
    sleep(5) if self.class.attempts == 1
  end
end
