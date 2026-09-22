# frozen_string_literal: true

module SolidQueue
  module MongoidIntegration
    extend self

    def install!
      install_object_id_serializer!
      install_global_id! if defined?(::Mongoid::Document)
      self
    end

    def after_fork!
      return {} unless defined?(::Mongoid::Clients)

      replacements = {}
      inherited_clients = ::Mongoid::Clients.clients.dup
      replacement_clients = inherited_clients.to_h do |name, inherited_client|
        replacement = replacements[inherited_client.__id__] ||=
          SolidQueue::Mongo.clone_client_for_fork(inherited_client)
        [ name, replacement ]
      end

      if defined?(::Mongoid::Threaded)
        if ::Mongoid::Threaded.respond_to?(:reset!)
          ::Mongoid::Threaded.reset!
        else
          # Mongoid 7 has no reset!; discard inherited thread state without
          # ending the parent's session from the forked process.
          Thread.current.keys.each do |key|
            Thread.current[key] = nil if key == :session || key.to_s.start_with?("[mongoid]:")
          end
        end
      end
      ::Mongoid::Clients.clear
      replacement_clients.each { |name, replacement| ::Mongoid::Clients.set(name, replacement) }
      replacements
    end

    def client(name = SolidQueue.mongoid_client)
      return unless name
      raise LoadError, "Mongoid must be loaded to use SolidQueue.mongoid_client" unless defined?(::Mongoid::Clients)

      ::Mongoid::Clients.with_name(name)
    end

    private
      def install_object_id_serializer!
        return unless defined?(::BSON::ObjectId)

        require "active_job/serializers/bson_object_id_serializer"
        ActiveJob::Serializers.add_serializers(ActiveJob::Serializers::BsonObjectIdSerializer)
      end

      def install_global_id!
        require "global_id/identification"
        ::Mongoid::Document.include(::GlobalID::Identification) unless ::Mongoid::Document < ::GlobalID::Identification
      end
  end
end
