# frozen_string_literal: true

module SolidQueue
  class Process < Record
    collection_name :processes

    field :kind, :last_heartbeat_at, :supervisor_id, :pid, :hostname, :name
    field :metadata, default: -> { {} }

    class << self
      def register(**attributes)
        attributes = normalize_registration_attributes(attributes)

        SolidQueue.instrument :register_process, **attributes.except(:supervisor_id) do |payload|
          create!(attributes.merge(last_heartbeat_at: Time.current)).tap do |process|
            payload[:process_id] = process.id
          end
        rescue Exception => error
          payload[:error] = error
          raise
        end
      end

      def find_by(attributes = {})
        attributes = attributes.dup
        if attributes[:supervisor_id] || attributes["supervisor_id"]
          key = attributes.key?(:supervisor_id) ? :supervisor_id : "supervisor_id"
          attributes[key] = SolidQueue::Mongo.id!(attributes[key])
        end
        super(attributes)
      rescue ::Mongo::Error => error
        raise_persistence_error(error)
      end

      def prune(excluding: nil)
        cutoff = SolidQueue.process_alive_threshold.ago
        excluded_id = excluding && SolidQueue::Mongo.id!(excluding.respond_to?(:id) ? excluding.id : excluding)
        filter = { last_heartbeat_at: { "$lte" => cutoff } }
        filter[:_id] = { "$ne" => excluded_id } if excluded_id

        SolidQueue.instrument :prune_processes, size: 0 do |payload|
          collection.find(filter, **SolidQueue::Mongo.session_options).sort(_id: 1).batch_size(50).each do |document|
            process = from_document(document)
            payload[:size] += 1 if process.prune(cutoff: cutoff)
          end
        end
      rescue ::Mongo::Error => error
        raise_persistence_error(error)
      end

      def excluding(process = nil)
        excluded_id = process && SolidQueue::Mongo.id!(process.respond_to?(:id) ? process.id : process)
        filter = excluded_id ? { _id: { "$ne" => excluded_id } } : {}
        collection.find(filter, **SolidQueue::Mongo.session_options).map { |document| from_document(document) }
      rescue ::Mongo::Error => error
        raise_persistence_error(error)
      end

      def admin_list(offset:, limit:)
        collection.find({}, **SolidQueue::Mongo.session_options)
          .sort(last_heartbeat_at: -1, _id: -1).skip(offset).limit(limit)
          .map { |document| from_document(document) }
      rescue ::Mongo::Error => error
        raise_persistence_error(error)
      end

      private
        def normalize_registration_attributes(attributes)
          attributes = attributes.dup
          if supervisor = attributes.delete(:supervisor)
            attributes[:supervisor_id] = SolidQueue::Mongo.id!(supervisor.id)
          elsif attributes[:supervisor_id]
            attributes[:supervisor_id] = SolidQueue::Mongo.id!(attributes[:supervisor_id])
          end
          attributes[:metadata] = (attributes[:metadata] || {}).deep_stringify_keys
          attributes
        end
    end

    def supervisor_id
      value = self[:supervisor_id]
      value&.to_s
    end

    def metadata
      self[:metadata] || {}
    end

    def heartbeat
      heartbeat_at = Time.current
      document = self.class.collection.find_one_and_update(
        { _id: bson_id },
        { "$set" => { last_heartbeat_at: heartbeat_at } },
        return_document: :after,
        **SolidQueue::Mongo.session_options
      )
      raise SolidQueue::RecordNotFound, "Solid Queue process #{id} is no longer registered" unless document

      assign_attributes(document)
      self
    rescue ::Mongo::Error => error
      self.class.raise_persistence_error(error)
    end

    def update_metadata!(metadata)
      normalized = (metadata || {}).deep_stringify_keys
      result = self.class.collection.update_one(
        { _id: bson_id },
        { "$set" => { metadata: normalized, updated_at: Time.current } },
        **SolidQueue::Mongo.session_options
      )
      raise SolidQueue::RecordNotFound, "Solid Queue process #{id} is no longer registered" if result.matched_count.zero?

      self.metadata = normalized
      self
    rescue ::Mongo::Error => error
      self.class.raise_persistence_error(error)
    end

    def deregister(pruned: false)
      SolidQueue.instrument :deregister_process, process: self, pruned: pruned do |payload|
        removed = SolidQueue::Mongo.transaction(operation: "deregister_process") do
          deleted = self.class.collection.find_one_and_delete(
            { _id: bson_id },
            **SolidQueue::Mongo.session_options
          )
          ClaimedExecution.release_for_process(id) if deleted && claims_executions?
          deleted
        end

        deregister_supervisees unless supervised? || pruned || !removed
        removed
      rescue ::Mongo::Error => error
        payload[:error] = error
        self.class.raise_persistence_error(error)
      rescue Exception => error
        payload[:error] = error
        raise
      end
    end

    def prune(cutoff: SolidQueue.process_alive_threshold.ago)
      SolidQueue.instrument :deregister_process, process: self, pruned: true do |payload|
        removed = SolidQueue::Mongo.transaction(operation: "prune_process") do
          deleted = self.class.collection.find_one_and_delete(
            { _id: bson_id, last_heartbeat_at: { "$lte" => cutoff } },
            **SolidQueue::Mongo.session_options
          )

          if deleted && claims_executions?
            heartbeat_at = deleted["last_heartbeat_at"] || deleted[:last_heartbeat_at]
            error = Processes::ProcessPrunedError.new(heartbeat_at)
            ClaimedExecution.fail_for_process(id, error)
          end
          deleted
        end
        !!removed
      rescue ::Mongo::Error => error
        payload[:error] = error
        self.class.raise_persistence_error(error)
      rescue Exception => error
        payload[:error] = error
        raise
      end
    end

    def fail_all_claimed_executions_with(error)
      ClaimedExecution.fail_for_process(id, error) if claims_executions?
    end

    def release_all_claimed_executions
      ClaimedExecution.release_for_process(id) if claims_executions?
    end

    def claims
      return 0 unless claims_executions?

      SolidQueue::Mongo.collection(:jobs).count_documents(
        { state: "claimed", process_id: bson_id },
        **SolidQueue::Mongo.session_options
      )
    rescue ::Mongo::Error => error
      self.class.raise_persistence_error(error)
    end

    private
      def supervised?
        self[:supervisor_id].present?
      end

      def claims_executions?
        kind == "Worker"
      end

      def deregister_supervisees
        self.class.collection.find(
          { supervisor_id: bson_id },
          **SolidQueue::Mongo.session_options
        ).map { |document| self.class.from_document(document) }.each(&:deregister)
      end
  end
end
