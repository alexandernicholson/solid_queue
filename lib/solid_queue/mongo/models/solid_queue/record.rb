# frozen_string_literal: true

module SolidQueue
  class Record
    include SolidQueue::Mongo::Document
  end
end

ActiveSupport.run_load_hooks :solid_queue_record, SolidQueue::Record
