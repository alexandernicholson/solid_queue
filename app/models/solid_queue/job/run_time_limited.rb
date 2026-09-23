# frozen_string_literal: true

module SolidQueue
  class Job
    module RunTimeLimited
      def run_time_limit
        limits = [ job_class.try(:run_time_limit), SolidQueue.max_run_time ]
        limits << SolidQueue.exactly_once_timeout if exactly_once?
        limits.compact.min
      end
    end
  end
end
