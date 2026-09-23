# frozen_string_literal: true

require "test_helper"
require_relative "../../shared/display_name_jobs"
require_relative "../../shared/display_name_behaviour"

class SolidQueue::DisplayNameTest < ActiveSupport::TestCase
  include DisplayNameBehaviour

  self.use_transactional_tests = false

  private
    def rename_job_class(job, class_name)
      job.update_columns(class_name: class_name, arguments: job.arguments.merge("job_class" => class_name))
    end
end
