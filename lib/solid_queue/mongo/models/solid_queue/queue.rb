# frozen_string_literal: true

module SolidQueue
  class Queue
    attr_accessor :name

    class << self
      def all
        Job.distinct_queue_names.map { |name| new(name) }
      end

      def find_by_name(name)
        new(name)
      end
    end

    def initialize(name)
      @name = name.to_s
    end

    def paused?
      Pause.paused?(name)
    end

    def pause
      Pause.pause(name)
    end

    def resume
      Pause.resume(name)
    end

    def clear(batch_size: 500)
      Job.discard_ready_in_queue(name, batch_size: batch_size)
    end

    def size
      @size ||= metrics.fetch(:size)
    end

    def latency
      @latency ||= ((Time.current - (metrics[:oldest_created_at] || Time.current)).to_i)
    end

    def human_latency
      ActiveSupport::Duration.build(latency).inspect
    end

    def ==(queue)
      name == queue.name
    end
    alias_method :eql?, :==

    def hash
      name.hash
    end

    private
      def metrics
        @metrics ||= Job.ready_metrics(name)
      end
  end
end
