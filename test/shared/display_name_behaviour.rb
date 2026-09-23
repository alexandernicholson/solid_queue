# frozen_string_literal: true

module DisplayNameBehaviour
  def test_display_name_uses_the_active_job_display_name_when_it_defines_one
    job = SolidQueue::Job.find(SharedCustomDisplayNameJob.perform_later("User").provider_job_id)

    assert_equal "User#welcome", job.display_name
  end

  def test_display_name_falls_back_to_the_class_name
    plain = SolidQueue::Job.find(SharedPlainDisplayNameJob.perform_later.provider_job_id)
    broken = SolidQueue::Job.find(SharedBrokenDisplayNameJob.perform_later.provider_job_id)
    missing = SolidQueue::Job.find(SharedPlainDisplayNameJob.perform_later.provider_job_id)
    rename_job_class(missing, "SharedRemovedDisplayNameJob")

    assert_equal "SharedPlainDisplayNameJob", plain.display_name
    assert_equal "SharedBrokenDisplayNameJob", broken.display_name
    assert_equal "SharedRemovedDisplayNameJob", SolidQueue::Job.find(missing.id).display_name
  end

  def test_admin_job_attributes_include_the_display_name
    active_job = SharedCustomDisplayNameJob.perform_later("Account")
    job = SolidQueue::Admin.find_job(active_job.job_id)

    assert_equal "Account#welcome", SolidQueue::Admin.job_attributes(job)[:display_name]
  end

  def test_failure_and_release_payloads_include_display_names
    process_id = register_worker_process
    active_job = SharedCustomDisplayNameJob.perform_later("Order")
    released_job = SharedCustomDisplayNameJob.perform_later("Invoice")
    claims = SolidQueue::ReadyExecution.claim([ "default" ], 2, process_id)
    released = claims.find { |claim| claim.job_id.to_s == released_job.provider_job_id.to_s }

    release_events = capture_events("release_claimed.solid_queue") { released.release }
    failure_events = capture_events("fail_many_claimed.solid_queue") do
      SolidQueue::ClaimedExecution.fail_for_process(process_id, SolidQueue::Processes::ProcessMissingError.new)
    end

    assert_equal [ "Invoice#welcome" ], release_events.map { |event| event.payload[:display_name] }
    assert_equal({ active_job.provider_job_id.to_s => "Order#welcome" }, failure_events.last.payload[:display_names].transform_keys(&:to_s))
  end

  private
    def register_worker_process
      SolidQueue::Process.register(kind: "Worker", name: "display-#{SecureRandom.hex(4)}", pid: ::Process.pid, hostname: "test").id
    end

    def capture_events(name)
      events = []
      subscriber = ->(*arguments) { events << ActiveSupport::Notifications::Event.new(*arguments) }
      ActiveSupport::Notifications.subscribed(subscriber, name) { yield }
      events
    end
end
