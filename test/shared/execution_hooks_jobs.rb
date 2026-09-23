# frozen_string_literal: true

class SharedHookedJob < ActiveJob::Base
  class_attribute :events, default: []

  def perform(outcome = "success")
    self.class.events += [ :perform ]
    raise ArgumentError, "hooked failure" if outcome == "failure"
  end
end
