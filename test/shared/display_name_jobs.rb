# frozen_string_literal: true

class SharedPlainDisplayNameJob < ActiveJob::Base
  def perform(*)
  end
end

class SharedCustomDisplayNameJob < ActiveJob::Base
  def perform(*)
  end

  def display_name
    "#{arguments.first}#welcome"
  end
end

class SharedBrokenDisplayNameJob < ActiveJob::Base
  def perform(*)
  end

  def display_name
    raise "unavailable"
  end
end
