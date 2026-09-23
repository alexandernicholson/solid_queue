# frozen_string_literal: true

module SolidQueue
  class Job
    module RunTimeLimited
      def run_time_limit
        [ job_class.try(:run_time_limit), SolidQueue.max_run_time ].compact.min
      end
    end
  end
end
