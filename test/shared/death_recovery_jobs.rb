# frozen_string_literal: true

class SharedDeathRecoveryJob < ActiveJob::Base
  class_attribute :executions_seen, default: []

  def perform
    self.class.executions_seen += [ executions ]
  end
end

class SharedDeduplicatedDeathRecoveryJob < ActiveJob::Base
  deduplicates key: ->(key) { key }

  def perform(*)
  end
end
