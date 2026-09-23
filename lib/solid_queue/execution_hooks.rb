# frozen_string_literal: true

module SolidQueue
  module ExecutionHooks
    extend AppExecutor

    AROUND_KINDS = %i[ around_claim around_perform around_poll ].freeze
    NOTIFY_KINDS = %i[ on_failure ].freeze
    KINDS = (AROUND_KINDS + NOTIFY_KINDS).freeze

    class NotPerformedError < StandardError
      def initialize(message = "An around_perform hook returned without performing the job")
        super
      end
    end

    @hooks = KINDS.index_with { [] }

    class << self
      def register(kind, &block)
        raise ArgumentError, "#{kind} needs a block" unless block

        hooks_for(kind) << block
        block
      end

      def run(kind, *arguments, &block)
        raise ArgumentError, "#{kind.inspect} is not an around hook; use one of #{AROUND_KINDS.inspect}" unless AROUND_KINDS.include?(kind)

        chain = hooks_for(kind)
        return yield if chain.empty?

        result = nil
        called = false
        innermost = proc do
          unless called
            called = true
            result = block.call
          end
          result
        end
        chain.reverse.inject(innermost) { |inner, hook| proc { hook.call(*arguments, &inner) } }.call
        result
      end

      def notify(kind, *arguments)
        raise ArgumentError, "#{kind.inspect} is not a notification hook; use one of #{NOTIFY_KINDS.inspect}" unless NOTIFY_KINDS.include?(kind)

        hooks_for(kind).each do |hook|
          hook.call(*arguments)
        rescue Exception => error
          handle_thread_error(error)
        end
      end

      def registered?(kind)
        hooks_for(kind).any?
      end

      def clear
        KINDS.each { |kind| @hooks[kind] = [] }
      end

      private
        def hooks_for(kind)
          @hooks.fetch(kind) { raise ArgumentError, "Unknown execution hook #{kind.inspect}; use one of #{KINDS.inspect}" }
        end
    end
  end
end
