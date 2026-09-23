# frozen_string_literal: true

class SharedWorkOffError < StandardError; end

class SharedWorkOffJob < ActiveJob::Base
  class_attribute :performed, default: []

  def perform(label)
    self.class.performed += [ label ]
  end
end

class SharedFailingWorkOffJob < ActiveJob::Base
  def perform(*)
    raise SharedWorkOffError, "work off failure"
  end
end

class SharedSlowWorkOffJob < ActiveJob::Base
  class_attribute :events, default: Concurrent::Array.new

  def perform(pause)
    sleep pause
    self.class.events << :performed
  end
end

class SharedWorkOffAbort < Exception; end

class SharedAbortingWorkOffJob < ActiveJob::Base
  def perform(*)
    raise SharedWorkOffAbort
  end
end

class SharedRegistrationProbeJob < ActiveJob::Base
  class_attribute :seen

  def perform
    self.class.seen = SolidQueue::Process.find_by(kind: "Worker")&.name
  end
end

class SharedChainingWorkOffJob < ActiveJob::Base
  def perform(label)
    SharedWorkOffJob.set(wait: 0.05.seconds).perform_later(label)
    sleep 0.1
  end
end

class SharedLimitedWorkOffJob < ActiveJob::Base
  limits_concurrency key: ->(key) { "work-off-#{key}" }, to: 1

  def perform(*)
  end
end
