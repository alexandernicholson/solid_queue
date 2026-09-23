# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/display_name_jobs"
require_relative "../shared/display_name_behaviour"

class MongoDisplayNameTest < MongoTestCase
  include DisplayNameBehaviour

  private
    def rename_job_class(job, class_name)
      arguments = ActiveSupport::JSON.encode(job.arguments.merge("job_class" => class_name))
      SolidQueue::Mongo.collection(:jobs).update_one({ _id: job.bson_id }, { "$set" => { class_name: class_name, arguments: arguments } })
    end
end
