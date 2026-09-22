# frozen_string_literal: true

require "mongo"
require "monitor"
require "active_support/isolated_execution_state"

require_relative "mongo/monitoring"
require_relative "mongo/transactions"
require_relative "mongo/indexes"
require_relative "mongo/document"

module SolidQueue
  module Mongo
    COLLECTIONS = %i[jobs processes semaphores pauses recurring_tasks recurring_executions batches batch_executions].freeze
    DRIVER_ERRORS = [
      ::Mongo::Error,
      ::Mongo::Error::AuthError,
      ::Mongo::Error::TimeoutError,
      ::Mongo::Error::ConnectionCheckOutTimeout
    ].uniq.freeze

    CONTEXT_KEY = :solid_queue_mongo_context

    class ConfigurationError < SolidQueue::PersistenceError; end

    class << self
      def client
        context[:client] || configured_client
      end

      def collection(name)
        name = name.to_sym
        raise ArgumentError, "unknown Solid Queue collection: #{name.inspect}" unless COLLECTIONS.include?(name)

        target = client
        ensure_prepared!(target)
        lifecycle_lock.synchronize do
          if target.equal?(@client)
            (@collections ||= {})[name] ||= target["solid_queue_#{name}"].with(write: { w: :majority }, read: { mode: :primary })
          else
            target["solid_queue_#{name}"].with(write: { w: :majority }, read: { mode: :primary })
          end
        end
      end

      def ensure_prepared!(target = client)
        validate_transaction_support!(target) unless @validated_client.equal?(target) && @validated_pid == ::Process.pid
        unless Indexes.prepared?(target)
          raise ConfigurationError, "MongoDB collections and indexes are not prepared. Run `bin/rails solid_queue:prepare`."
        end
      end

      def with_session(session, client: nil)
        raise ArgumentError, "a Mongo::Session is required" unless session

        previous = context.dup
        if previous[:session] && !previous[:session].equal?(session)
          raise ArgumentError, "nested Mongo sessions are not supported"
        end
        owner = session_client(session)
        scoped_client = client || previous[:client] || configured_client
        if owner && !same_cluster?(scoped_client, owner)
          raise ArgumentError, "the supplied session belongs to a different Mongo client cluster"
        end

        context[:session] = session
        context[:client] = scoped_client
        context[:external_session] = true
        context[:external_after_commit] = []
        # The driver has no commit callback for caller-owned transactions. Any
        # registered work remains for the durable maintenance sweep; executing
        # it here could mistake an abort for a commit.
        yield session
      ensure
        self.context = previous if previous
      end

      def current_session
        context[:session]
      end

      def session_options
        options = current_session ? { session: current_session } : {}
        if (deadline = context[:deadline])
          options[:timeout_ms] = [ ((deadline - ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)) * 1000).ceil, 1 ].max
        end
        options
      end

      def after_commit(&block)
        raise ArgumentError, "a block is required" unless block

        if context[:owned_transaction]
          context[:after_commit] << block
        elsif current_session && transaction_in_progress?(current_session)
          context.fetch(:external_after_commit) << block
        else
          block.call
        end
      end

      def id(value)
        return value if value.is_a?(BSON::ObjectId)
        return nil if value.nil?

        BSON::ObjectId.from_string(value.to_s)
      rescue BSON::ObjectId::Invalid
        nil
      end

      def id!(value)
        id(value) || raise(ArgumentError, "invalid BSON ObjectId: #{value.inspect}")
      end

      def duplicate_key?(error, index: nil)
        operation_error = error
        operation_error = operation_error.cause while operation_error.respond_to?(:cause) && operation_error.cause &&
          !operation_error.is_a?(::Mongo::Error::OperationFailure)
        return false unless operation_error.is_a?(::Mongo::Error::OperationFailure)
        return false unless operation_error.respond_to?(:code) && operation_error.code == 11_000
        return true unless index

        operation_error.message.include?(index.to_s)
      end


      def prepare!
        validate_transaction_support!
        Indexes.prepare!(client)
        Monitoring.install!(client)
        true
      end

      def disconnect!
        reset!
      end

      def reset!
        lifecycle_lock.synchronize do
          previous_client = @client
          owned = @owns_client
          close_owned_client
          Monitoring.forget!(previous_client) if owned && previous_client
          @client = nil
          @client_pid = nil
          @client_source = nil
          @collections = nil
          @validated_client = @validated_pid = nil
        end
        Indexes.reset!
        clear_context!
      end

      def after_fork!
        lifecycle_lock.synchronize do
          inherited_client = @client || resolve_configured_client
          replacements = if defined?(SolidQueue::MongoidIntegration)
            SolidQueue::MongoidIntegration.after_fork!
          else
            {}
          end
          Monitoring.forget!(inherited_client) if inherited_client

          # Closing or reconnecting an inherited client asks the server to end
          # sessions that still belong to the parent process. Build an
          # independent client instead and let the child OS discard inherited
          # sockets when it exits.
          if inherited_client && replacements.key?(inherited_client.__id__)
            @client = replacements.fetch(inherited_client.__id__)
            @owns_client = false
          else
            @client = clone_client_for_fork(inherited_client) if inherited_client
            @owns_client = !!@client
          end
          @client_pid = ::Process.pid if @client
          @client_source = SolidQueue.mongo_client if @client
          @collections = nil
          @validated_client = @validated_pid = nil
          Monitoring.install!(@client) if @client
        end
        Indexes.reset!
        clear_context!
        true
      end

      def close
        disconnect!
      end

      def verify!
        client.database.command(ping: 1).first
        validate_transaction_support!
      end

      def silence_logging
        loggers = [ ::Mongo::Logger.logger ]
        loggers << ::Mongoid.logger if defined?(::Mongoid) && ::Mongoid.respond_to?(:logger)
        silence_loggers(loggers.compact.uniq, 0) { yield }
      end

      def validate_transaction_support!(target = client)
        hello = target.database.command(hello: 1).first
        unless hello["setName"] || hello[:setName] || hello["msg"] == "isdbgrid" || hello[:msg] == "isdbgrid"
          raise ConfigurationError, "Solid Queue's MongoDB backend requires a replica set or sharded cluster"
        end
        @validated_client = target
        @validated_pid = ::Process.pid
        true
      rescue *DRIVER_ERRORS => error
        raise ConfigurationError, "Unable to validate MongoDB transaction support: #{error.message}"
      end

      def transaction_in_progress?(session)
        transaction = session.respond_to?(:transaction) && session.transaction
        return transaction.in_progress? if transaction.respond_to?(:in_progress?)
        return session.in_transaction? if session.respond_to?(:in_transaction?)

        false
      end

      def clone_client_for_fork(inherited_client)
        addresses_or_uri = inherited_client.cluster.options[:srv_uri]
        addresses_or_uri ||= inherited_client.cluster.addresses.map(&:to_s)
        options = inherited_client.options.dup
        options[:database] ||= inherited_client.database.name
        ::Mongo::Client.new(addresses_or_uri, options)
      rescue *DRIVER_ERRORS, ArgumentError => error
        raise ConfigurationError, "Unable to create a fork-safe MongoDB client: #{error.message}"
      end

      def context
        ActiveSupport::IsolatedExecutionState[CONTEXT_KEY] ||= {}
      end

      private
        def context=(value)
          ActiveSupport::IsolatedExecutionState[CONTEXT_KEY] = value
        end

        def clear_context!
          ActiveSupport::IsolatedExecutionState.delete(CONTEXT_KEY)
        end

        def silence_loggers(loggers, index, &block)
          return block.call if index >= loggers.length

          logger = loggers[index]
          if logger.respond_to?(:silence)
            logger.silence { silence_loggers(loggers, index + 1, &block) }
          else
            logger.with_level(::Logger::ERROR) { silence_loggers(loggers, index + 1, &block) }
          end
        end

        def configured_client
          source = SolidQueue.respond_to?(:mongo_client) ? SolidQueue.mongo_client : nil
          pid = ::Process.pid

          lifecycle_lock.synchronize do
            if @client && @client_pid != pid
              inherited_client = @client
              replacements = if defined?(SolidQueue::MongoidIntegration)
                SolidQueue::MongoidIntegration.after_fork!
              else
                {}
              end
              Monitoring.forget!(inherited_client)
              if replacements.key?(inherited_client.__id__)
                @client = replacements.fetch(inherited_client.__id__)
                @owns_client = false
              else
                @client = clone_client_for_fork(inherited_client)
                @owns_client = true
              end
              @client_pid = pid
              @client_source = source
              @collections = nil
              @validated_client = @validated_pid = nil
              Monitoring.install!(@client)
            elsif @client && @client_source != source
              previous_client = @client
              close_owned_client
              Monitoring.forget!(previous_client)
              @client = nil
              @collections = nil
              @validated_client = @validated_pid = nil
            end

            @client ||= begin
              @client_pid = pid
              @client_source = source
              build_client(source).tap { |built_client| Monitoring.install!(built_client) }
            end
          end
        end

        def build_client(source)
          candidate = source.respond_to?(:call) ? source.call : source
          if !candidate && defined?(SolidQueue::MongoidIntegration)
            candidate = SolidQueue::MongoidIntegration.client
          end

          if candidate
            @owns_client = false
            candidate
          else
            @owns_client = true
            options = {
              retry_writes: true,
              write: { w: :majority },
              read: { mode: :primary }
            }
            database = SolidQueue.mongo_database if SolidQueue.respond_to?(:mongo_database)
            options[:database] = database if database && !database.to_s.empty?
            ::Mongo::Client.new(mongo_url, options)
          end
        end

        def resolve_configured_client
          source = SolidQueue.respond_to?(:mongo_client) ? SolidQueue.mongo_client : nil
          candidate = source.respond_to?(:call) ? source.call : source
          if !candidate && defined?(SolidQueue::MongoidIntegration)
            candidate = SolidQueue::MongoidIntegration.client
          end
          candidate
        end


        def mongo_url
          if SolidQueue.respond_to?(:mongo_url) && SolidQueue.mongo_url
            SolidQueue.mongo_url
          else
            ENV.fetch("MONGODB_URI", "mongodb://127.0.0.1:27017/solid_queue")
          end
        end

        def close_owned_client
          @client&.close if @owns_client
        rescue *DRIVER_ERRORS
          nil
        ensure
          @owns_client = false
        end

        def lifecycle_lock
          @lifecycle_lock ||= Monitor.new
        end

        def session_client(session)
          session.client if session.respond_to?(:client)
        end

        def same_cluster?(left, right)
          left.respond_to?(:cluster) && right.respond_to?(:cluster) && left.cluster.equal?(right.cluster)
        end
    end
  end
end
