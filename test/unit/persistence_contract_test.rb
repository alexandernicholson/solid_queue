# frozen_string_literal: true

require "test_helper"
require_relative "../shared/persistence_contract"

class PersistenceContractTest < ActiveSupport::TestCase
  include PersistenceContract
end
