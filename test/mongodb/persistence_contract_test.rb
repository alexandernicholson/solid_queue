# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/persistence_contract"

class MongoPersistenceContractTest < MongoTestCase
  include PersistenceContract
end
