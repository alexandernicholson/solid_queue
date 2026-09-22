# frozen_string_literal: true

module ActiveJob
  module Serializers
    class BsonObjectIdSerializer < ObjectSerializer
      def serialize(object_id)
        super("value" => object_id.to_s)
      end

      def deserialize(hash)
        BSON::ObjectId.from_string(hash.fetch("value"))
      end

      def klass
        BSON::ObjectId
      end
    end
  end
end
