# frozen_string_literal: true

module SolidQueue
  class Pause < Record
    collection_name :pauses

    field :queue_name
    field :created_at

    class << self
      def pause(queue_name)
        document = collection.find_one_and_update(
          { queue_name: queue_name.to_s },
          { "$setOnInsert" => { queue_name: queue_name.to_s, created_at: Time.current } },
          upsert: true,
          return_document: :after,
          **SolidQueue::Mongo.session_options
        )
        from_document(document)
      end

      def resume(queue_name)
        collection.delete_one(
          { queue_name: queue_name.to_s },
          **SolidQueue::Mongo.session_options
        ).deleted_count
      end

      def paused?(queue_name)
        collection.find(
          { queue_name: queue_name.to_s },
          **SolidQueue::Mongo.session_options
        ).projection(_id: 1).limit(1).first.present?
      end

      def queue_names
        collection.find({}, **SolidQueue::Mongo.session_options).distinct(:queue_name)
      end
    end
  end
end
