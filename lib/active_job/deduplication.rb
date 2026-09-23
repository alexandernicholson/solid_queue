# frozen_string_literal: true

module ActiveJob
  module Deduplication
    extend ActiveSupport::Concern
    include ArgumentIdentity

    included do
      class_attribute :deduplication_key, instance_accessor: false
      class_attribute :deduplication_duration, instance_accessor: false
    end

    class_methods do
      def deduplicates(key:, duration: nil)
        self.deduplication_key = key
        self.deduplication_duration = duration
      end
    end

    def deduplication_key
      if (key = self.class.deduplication_key)
        param = key.is_a?(Proc) ? instance_exec(*arguments, &key) : key.to_s

        [ self.class.name, *argument_identity(param) ].compact.join("/")
      end
    end

    def deduplication_duration
      self.class.deduplication_duration
    end
  end
end
