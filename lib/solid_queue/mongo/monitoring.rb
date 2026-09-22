# frozen_string_literal: true

module SolidQueue
  module Mongo
    module Monitoring
      CHECKOUTS_KEY = :solid_queue_mongo_checkouts
      SLOW_CHECKOUT_SECONDS = 0.05

      class Subscriber
        def initialize(channel)
          @channel = channel
        end

        def started(event)
          publish(event, :started)
        end

        def succeeded(event)
          publish(event, :succeeded)
        end

        def failed(event)
          publish(event, :failed)
        end

        def published(event)
          publish(event, :published)
        end

        private
          def publish(event, status)
            case @channel
            when :server_description then publish_server_description(event)
            when :connection_pool then publish_pool_event(event)
            when :command then publish_command(event, status)
            end
          rescue StandardError => error
            SolidQueue.logger&.debug("Solid Queue Mongo monitoring error: #{error.class}: #{error.message}")
          end

          def publish_pool_event(event)
            starts = Thread.current[CHECKOUTS_KEY] ||= {}
            case event
            when ::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutStarted
              starts[self] = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
            when ::Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut,
                ::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed
              started = starts.delete(self)
              Thread.current[CHECKOUTS_KEY] = nil if starts.empty?
              duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started if started
              if event.is_a?(::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed)
                publish_checkout_failure(event, duration)
              elsif duration && duration >= SLOW_CHECKOUT_SECONDS
                SolidQueue.instrument(:mongo_pool_checkout_wait, address: address(event), duration: duration)
              end
            end
          end

          def publish_server_description(event)
            previous = value(event, :previous_description)
            current = value(event, :new_description)
            return unless primary?(previous) != primary?(current) || unknown?(current)

            channel = unknown?(current) ? :mongo_server_unavailable : :mongo_primary_change
            SolidQueue.instrument(channel,
              address: address(event, current), previous_type: description_type(previous),
              new_type: description_type(current))
          end

          def publish_checkout_failure(event, duration)
            SolidQueue.instrument(:mongo_pool_checkout_failed,
              address: address(event), reason: value(event, :reason),
              error: value(event, :error), duration: duration)
          end

          def publish_command(event, status)
            SolidQueue.instrument(:mongo_command,
              status: status, command_name: value(event, :command_name),
              database_name: value(event, :database_name), duration: value(event, :duration),
              address: address(event), error: value(event, :message) || value(event, :error))
          end

          def primary?(description)
            description && description.respond_to?(:primary?) && description.primary?
          end

          def unknown?(description)
            description && description.respond_to?(:unknown?) && description.unknown?
          end

          def description_type(description)
            return unless description
            description.respond_to?(:server_type) ? description.server_type.to_s : description.class.name
          end

          def address(event, description = nil)
            candidate = value(event, :address) || (description && value(description, :address))
            candidate&.to_s
          end

          def value(object, method)
            object.public_send(method) if object && object.respond_to?(method)
          end
      end

      class << self
        def install!(client)
          lock.synchronize do
            return true if installed_clients.any? { |installed, _| installed.equal?(client) }

            listeners = subscriptions.filter_map do |constant, channel|
              topic = ::Mongo::Monitoring.const_get(constant, false) if ::Mongo::Monitoring.const_defined?(constant, false)
              subscribe(client, topic, channel) if topic
            end
            if command_monitoring?
              topic = ::Mongo::Monitoring.const_get(:COMMAND, false) if ::Mongo::Monitoring.const_defined?(:COMMAND, false)
              listeners << subscribe(client, topic, :command) if topic
            end
            installed_clients << [ client, listeners ]
          end
          true
        end

        def forget!(client)
          lock.synchronize do
            entry = installed_clients.find { |installed, _| installed.equal?(client) }
            if entry
              installed_clients.delete(entry)
              entry.last.each { |topic, subscriber| client.unsubscribe(topic, subscriber) }
            end
          end
        end

        def reset!
          lock.synchronize do
            installed_clients.each do |client, listeners|
              listeners.each { |topic, subscriber| client.unsubscribe(topic, subscriber) }
            end
            @installed_clients = []
          end
        end

        private
          def subscriptions
            {
              SERVER_DESCRIPTION_CHANGED: :server_description,
              CONNECTION_POOL: :connection_pool
            }
          end

          def command_monitoring?
            SolidQueue.respond_to?(:mongo_command_monitoring) && SolidQueue.mongo_command_monitoring
          end

          def subscribe(client, topic, channel)
            subscriber = Subscriber.new(channel)
            client.subscribe(topic, subscriber)
            [ topic, subscriber ]
          end

          def installed_clients
            @installed_clients ||= []
          end

          def lock
            @lock ||= Monitor.new
          end
      end
    end
  end
end
