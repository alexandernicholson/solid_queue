# frozen_string_literal: true

require_relative "test_helper"
require_relative "../shared/delivery_guarantees_jobs"
require_relative "../shared/delivery_guarantees_behaviour"

class MongoDeliveryGuaranteesTest < MongoTestCase
  include DeliveryGuaranteesBehaviour

  RESULTS = "solid_queue_test_delivery_results"

  def setup
    super
    results.delete_many({})
  end

  test "forked workers perform every job exactly once" do
    skip "fork is unavailable" unless ::Process.respond_to?(:fork)

    keys = 300.times.map { |number| "forked-#{number}" }
    ActiveJob.perform_all_later(keys.map { |key| SharedDeliveryJob.new(key) })
    SharedDeliveryJob.recorder = ->(key) { SolidQueue::Mongo.client[RESULTS].insert_one(key: key, pid: ::Process.pid) }

    children = 3.times.map do
      fork do
        SolidQueue::Mongo.after_fork!
        process_id = SolidQueue::Process.register(kind: "Worker", name: "forked-#{::Process.pid}", pid: ::Process.pid, hostname: "test").id
        loop do
          claims = SolidQueue::ReadyExecution.claim([ "*" ], 5, process_id)
          break if claims.empty?

          claims.each(&:perform)
        end
        exit!(0)
      rescue Exception
        exit!(1)
      end
    end
    statuses = children.map { |pid| ::Process.waitpid2(pid).last }

    assert statuses.all?(&:success?)
    counts = results.aggregate([ { "$group" => { _id: "$key", count: { "$sum" => 1 } } } ]).to_h { |row| [ row["_id"], row["count"] ] }
    assert_equal keys.sort, counts.keys.sort
    assert_equal [ 1 ], counts.values.uniq
    assert_equal 0, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::ClaimedExecution.count
  ensure
    children&.each do |pid|
      ::Process.kill(:KILL, pid)
      ::Process.waitpid(pid)
    rescue Errno::ESRCH, Errno::ECHILD
    end
  end

  private
    def results
      SolidQueue::Mongo.client[RESULTS]
    end

    def unregistered_process_id
      BSON::ObjectId.new
    end
end
