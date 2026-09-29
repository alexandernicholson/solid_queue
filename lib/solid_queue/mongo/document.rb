# frozen_string_literal: true

require "active_model"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/object/deep_dup"
require "active_support/core_ext/object/duplicable"

module SolidQueue
  module Mongo
    module Document
      extend ActiveSupport::Concern

      DEFAULT_NOT_GIVEN = Object.new.freeze

      included do
        include ActiveModel::Validations
        include ActiveModel::Conversion
        include ActiveModel::Callbacks
        extend ActiveModel::Naming

        define_model_callbacks :save, :create, :update, :destroy
        field :created_at, :updated_at
      end

      class_methods do
        def collection_name(value = nil)
          value ? @collection_name = value.to_sym : (@collection_name || inherited_collection_name)
        end

        def collection
          raise NotImplementedError, "#{name} must declare collection_name" unless collection_name
          SolidQueue::Mongo.collection(collection_name)
        end

        def fields
          @all_fields ||= begin
            inherited = superclass.respond_to?(:fields) ? superclass.fields : {}
            inherited.merge(@fields || {}).freeze
          end
        end

        def field(*names, default: DEFAULT_NOT_GIVEN)
          raise ArgumentError, "at least one field name is required" if names.empty?

          @fields ||= {}
          names.each do |name|
            name = name.to_sym
            @fields[name] = default
            @all_fields = nil
            define_method(name) { @attributes[name] }
            define_method("#{name}=") do |value|
              track_mongo_transaction_state
              @attributes[name] = value
            end
          end
        end

        def from_document(document)
          return unless document

          allocate.tap { |record| record.send(:initialize_from_document, document) }
        end

        def create!(attributes = {})
          new(attributes).tap(&:save!)
        end

        def find(value)
          from_document(collection.find({ _id: SolidQueue::Mongo.id!(value) }, **SolidQueue::Mongo.session_options).first) ||
            raise_record_not_found(value)
        end

        def find_by(filter = {})
          from_document(collection.find(normalize_filter(filter), **SolidQueue::Mongo.session_options).first)
        end

        def insert_all!(rows)
          documents = rows.map { |row| document_for_insert(row) }
          return [] if documents.empty?

          collection.insert_many(documents, **SolidQueue::Mongo.session_options)
          documents.map { |document| from_document(document) }
        rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
          raise_persistence_error(error)
        end

        def count(filter = {})
          collection.count_documents(normalize_filter(filter), **SolidQueue::Mongo.session_options)
        end

        def distinct_values_of(field, filter = {})
          collection.distinct(field, normalize_filter(filter), **SolidQueue::Mongo.session_options)
        end

        def delete_all(filter = {})
          collection.delete_many(normalize_filter(filter), **SolidQueue::Mongo.session_options).deleted_count
        rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
          raise_persistence_error(error)
        end

        def transaction(operation: name)
          SolidQueue::Mongo.transaction(operation: operation) { |session| yield session }
        end

        def normalize_filter(filter)
          filter.each_with_object({}) do |(key, value), normalized|
            key.to_sym == :id ? normalized[:_id] = SolidQueue::Mongo.id(value) : normalized[key] = value
          end
        end

        def raise_record_not_found(value)
          error_class = SolidQueue.const_defined?(:RecordNotFound) ? SolidQueue::RecordNotFound : KeyError
          raise error_class, "Couldn't find #{name} with id=#{value.inspect}"
        end

        def raise_persistence_error(error)
          error_class = SolidQueue.const_defined?(:PersistenceError) ? SolidQueue::PersistenceError : RuntimeError
          wrapped = error_class.new("#{error.class}: #{error.message}")
          wrapped.set_backtrace(error.backtrace)
          raise wrapped, cause: error
        end

        private
          def inherited_collection_name
            superclass.collection_name if superclass.respond_to?(:collection_name)
          end

          def document_for_insert(attributes)
            now = Time.current
            attributes.symbolize_keys.tap do |document|
              document[:_id] ||= BSON::ObjectId.new
              document[:created_at] ||= now if fields.key?(:created_at)
              document[:updated_at] ||= now if fields.key?(:updated_at)
            end
          end
      end

      attr_reader :bson_id

      def initialize(attributes = {}, persisted: false, **keyword_attributes)
        @attributes = {}
        @initializing = true
        apply_defaults
        assign_attributes(attributes.to_h.merge(keyword_attributes))
        @persisted = persisted
      ensure
        @initializing = false
      end

      def transaction(operation:, &block)
        SolidQueue::Mongo.transaction(operation: operation, &block)
      end

      def id
        bson_id&.to_s
      end

      def attributes
        @attributes.merge(_id: bson_id, id: id).stringify_keys
      end

      def [](name)
        case name.to_sym
        when :id then id
        when :_id then bson_id
        else @attributes[name.to_sym]
        end
      end
      alias read_attribute []

      def []=(name, value)
        track_mongo_transaction_state
        if %i[id _id].include?(name.to_sym)
          @bson_id = value.is_a?(BSON::ObjectId) ? value : SolidQueue::Mongo.id(value)
        else
          @attributes[name.to_sym] = value
        end
      end
      alias write_attribute []=

      def persisted?
        @persisted
      end

      def new_record?
        !persisted?
      end

      def assign_attributes(attributes)
        track_mongo_transaction_state
        attributes.each do |key, value|
          key = key.to_sym
          if key == :_id || key == :id
            @bson_id = value.is_a?(BSON::ObjectId) ? value : SolidQueue::Mongo.id(value)
          elsif respond_to?("#{key}=")
            public_send("#{key}=", value)
          else
            @attributes[key] = value
          end
        end
        self
      end

      def save!
        track_mongo_transaction_state
        raise ActiveModel::ValidationError, self unless valid?

        run_callbacks :save do
          persisted? ? update_document! : insert_document!
        end
        self
      rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
        self.class.raise_persistence_error(error)
      end

      def update!(attributes)
        assign_attributes(attributes)
        save!
      end

      def destroy!
        track_mongo_transaction_state
        return self unless persisted?

        run_callbacks :destroy do
          result = self.class.collection.delete_one({ _id: bson_id }, **SolidQueue::Mongo.session_options)
          self.class.raise_record_not_found(id) unless result.deleted_count == 1
          @persisted = false
        end
        self
      rescue *SolidQueue::Mongo::DRIVER_ERRORS => error
        self.class.raise_persistence_error(error)
      end

      def reload
        document = self.class.collection.find({ _id: bson_id }, **SolidQueue::Mongo.session_options).first
        self.class.raise_record_not_found(id) unless document
        assign_attributes(document)
        @persisted = true
        self
      end

      def ==(other)
        other.instance_of?(self.class) && bson_id && bson_id == other.bson_id
      end
      alias eql? ==

      def hash
        [ self.class, bson_id ].hash
      end

      def mongo_transaction_snapshot
        { attributes: @attributes.deep_dup, bson_id: @bson_id, persisted: @persisted }
      end

      def restore_mongo_transaction_snapshot(snapshot)
        @attributes = snapshot.fetch(:attributes)
        @bson_id = snapshot[:bson_id]
        @persisted = snapshot.fetch(:persisted)
        self
      end

      private
        def track_mongo_transaction_state
          SolidQueue::Mongo.track_transaction_record(self) unless @initializing
        end

        def initialize_from_document(document)
          @attributes = {}
          document.each do |key, value|
            key = key.to_sym
            if key == :_id || key == :id
              @bson_id = value.is_a?(BSON::ObjectId) ? value : SolidQueue::Mongo.id(value)
            else
              @attributes[key] = value
            end
          end
          @persisted = true
        end

        def apply_defaults
          self.class.fields.each do |name, default|
            next if default.equal?(DEFAULT_NOT_GIVEN)
            @attributes[name] = default.respond_to?(:call) ? default.call : duplicate_default(default)
          end
        end

        def duplicate_default(default)
          default.duplicable? ? default.deep_dup : default
        end

        def insert_document!
          run_callbacks :create do
            now = Time.current
            @bson_id ||= BSON::ObjectId.new
            @attributes[:created_at] ||= now if self.class.fields.key?(:created_at)
            @attributes[:updated_at] ||= now if self.class.fields.key?(:updated_at)
            self.class.collection.insert_one(to_document, **SolidQueue::Mongo.session_options)
            @persisted = true
          end
        end

        def update_document!
          run_callbacks :update do
            @attributes[:updated_at] = Time.current if self.class.fields.key?(:updated_at)
            result = self.class.collection.update_one(
              { _id: bson_id }, { "$set" => @attributes }, **SolidQueue::Mongo.session_options
            )
            self.class.raise_record_not_found(id) if result.matched_count.zero?
          end
        end

        def to_document
          @attributes.merge(_id: bson_id)
        end
    end
  end
end
