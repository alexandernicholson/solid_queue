# frozen_string_literal: true

module SolidQueue
  class QueueSelector
    attr_reader :raw_queues, :model

    def initialize(queue_list, model)
      @raw_queues = Array(queue_list).map { |queue| queue.to_s.strip }.reject(&:empty?)
      @raw_queues = [ "*" ] if @raw_queues.empty?
      @model = model
    end

    # Queue names to filter on, or [ nil ] for every queue
    # (Active Record's counterpart is scoped_relations)
    def scoped_queues
      all? ? [ nil ] : queue_names
    end

    private
      def all?
        include_all_queues? && paused_queues.empty?
      end

      def queue_names
        @queue_names ||= eligible_queues - paused_queues
      end

      def eligible_queues
        if include_all_queues? then all_queues
        else
          raw_queues.each_with_object([]) do |queue, names|
            matches = prefixed_name?(queue) ? prefixed_names(queue.delete_suffix("*")) : [ queue ]
            matches.each { |name| names << name unless names.include?(name) }
          end
        end
      end

      def include_all_queues?
        raw_queues.include?("*")
      end

      def all_queues
        model.distinct_values_of(:queue_name).compact.sort
      end

      def prefixed_names(prefix)
        model.distinct_values_of(:queue_name, queue_name: Regexp.new("^#{Regexp.escape(prefix)}")).compact.sort
      end

      def prefixed_name?(queue)
        queue.end_with?("*")
      end

      def paused_queues
        @paused_queues ||= Pause.queue_names
      end
  end
end
