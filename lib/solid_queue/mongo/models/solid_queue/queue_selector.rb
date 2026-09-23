# frozen_string_literal: true

module SolidQueue
  class QueueSelector
    attr_reader :raw_queues, :relation

    def initialize(queue_list, relation)
      @raw_queues = Array(queue_list).map { |queue| queue.to_s.strip }.reject(&:empty?)
      @raw_queues = [ "*" ] if @raw_queues.empty?
      @relation = relation
    end

    def filters
      if raw_queues.include?("*") && paused_queue_names.empty?
        [ nil ]
      else
        queue_names
      end
    end

    def queue_names
      @queue_names ||= begin
        selected = if raw_queues.include?("*")
          distinct_queues
        else
          raw_queues.each_with_object([]) do |queue, names|
            matches = prefix?(queue) ? queues_with_prefix(queue.delete_suffix("*")) : [ queue ]
            matches.each { |name| names << name unless names.include?(name) }
          end
        end
        selected - paused_queue_names
      end
    end

    private
      def state
        relation.type.to_s
      end

      def prefix?(queue)
        queue.end_with?("*")
      end

      def paused_queue_names
        @paused_queue_names ||= Pause.queue_names
      end

      def distinct_queues
        Job.distinct_queue_names(state: state).compact.sort
      end

      def queues_with_prefix(prefix)
        pattern = Regexp.new("^#{Regexp.escape(prefix)}")
        Job.collection.find(
          { state: state, queue_name: pattern },
          **SolidQueue::Mongo.session_options
        ).distinct(:queue_name).compact.sort
      end
  end
end
