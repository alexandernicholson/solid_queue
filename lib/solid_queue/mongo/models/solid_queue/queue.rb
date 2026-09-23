# frozen_string_literal: true

module SolidQueue
  class Queue
    attr_accessor :name

    class << self
      def all
        Job.distinct_values_of(:queue_name).map { |name| new(name) }
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
      ReadyExecution.discard_all_in_batches(batch_size: batch_size, queue_name: name)
    end

    def size
      @size ||= metrics.fetch(:size)
    end

    def latency
      @latency ||= begin
        now = Time.current
        (now - (metrics[:oldest_created_at] || now)).to_i
      end
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
