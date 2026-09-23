# frozen_string_literal: true

module SolidQueue
  # Backend-neutral query and action surface for administrative clients.
  # It deliberately returns model objects and plain values instead of exposing
  # an Active Record relation, so clients work unchanged with MongoDB storage.
  module Admin
    QueueInfo = Struct.new(:name, :size, :paused, keyword_init: true)

    STATUS_MAP = {
      pending: :ready,
      failed: :failed,
      in_progress: :claimed,
      blocked: :blocked,
      scheduled: :scheduled,
      finished: :finished
    }.freeze

    module_function

    def queues(offset: 0, limit: nil, name: nil, paused: nil)
      entries = if mongodb?
        names = mongo_collection(:jobs).distinct(:queue_name, {}, **mongo_options)
        sizes = mongo_collection(:jobs).aggregate([
          { "$match" => { "state" => "ready" } },
          { "$group" => { "_id" => "$queue_name", "count" => { "$sum" => 1 } } }
        ], **mongo_options).each_with_object({}) { |row, counts| counts[row["_id"]] = row["count"] }
        pauses = mongo_collection(:pauses).distinct(:queue_name, {}, **mongo_options).index_with(true)

        names.sort.map { |queue_name| QueueInfo.new(name: queue_name, size: sizes.fetch(queue_name, 0), paused: pauses.key?(queue_name)) }
      else
        records = Queue.all
        names = records.map(&:name)
        pauses = Pause.where(queue_name: names).pluck(:queue_name).index_with(true)
        sizes = ReadyExecution.where(queue_name: names).group(:queue_name).count

        records.map { |queue| QueueInfo.new(name: queue.name, size: sizes.fetch(queue.name, 0), paused: pauses.key?(queue.name)) }
      end

      entries.select! { |queue| queue.name == name.to_s } if name
      entries.select! { |queue| queue.paused == paused } unless paused.nil?
      entries = entries.drop(offset)
      limit ? entries.first(limit) : entries
    end

    def queue_size(name)
      Queue.find_by_name(name).size
    end

    def clear_queue(name)
      Queue.find_by_name(name).clear
    end

    def pause_queue(name)
      Queue.find_by_name(name).pause
    end

    def resume_queue(name)
      Queue.find_by_name(name).resume
    end

    def queue_paused?(name)
      Queue.find_by_name(name).paused?
    end

    def jobs(status:, offset: 0, limit: nil, **filters)
      mongodb? ? mongo_jobs(status:, offset:, limit:, **filters) : active_record_jobs(status:, offset:, limit:, **filters)
    end

    def jobs_count(status:, offset: 0, limit: nil, **filters)
      if mongodb?
        selector = mongo_job_selector(status:, **filters)
        count = [ mongo_collection(:jobs).count_documents(selector, **mongo_options) - offset, 0 ].max
        limit ? [ count, limit ].min : count
      else
        scope = active_record_jobs_scope(status:, **filters).offset(offset)
        scope = scope.limit(limit) if limit
        scope.count
      end
    end

    def failures(offset: 0, limit: nil, **filters)
      jobs(status: :failed, offset:, limit:, **filters)
    end

    def failures_count(offset: 0, limit: nil, **filters)
      jobs_count(status: :failed, offset:, limit:, **filters)
    end

    def find_job(active_job_id, status: nil, **filters)
      if mongodb?
        document = mongo_collection(:jobs).find(
          mongo_job_selector(status:, active_job_id:, **filters),
          **mongo_options
        ).sort(_id: -1).first
        document && Job.from_document(document)
      elsif status.nil? && filters[:recurring_task_id].blank?
        scope = Job.where(active_job_id: active_job_id).order(id: :desc)
        scope = scope.where(queue_name: filters[:queue_name]) if filters[:queue_name].present?
        scope = scope.where(class_name: filters[:job_class_name]) if filters[:job_class_name].present?
        scope = scope.where(batch_id: filters[:batch_id]) if filters[:batch_id].present?
        scope.first
      else
        active_record_jobs_scope(status:, active_job_id:, **filters).order(id: :desc).first
      end
    end

    def retry_jobs(status: :failed, offset: 0, limit: nil, **filters)
      selected = jobs(status:, offset:, limit:, **filters)
      FailedExecution.retry_all(selected)
    end

    def retry_job(active_job_id, status: :failed, **filters)
      job = find_job(active_job_id, status:, **filters)
      return unless job

      mongodb? ? FailedExecution.from_document(job.attributes).retry : job.failed_execution.retry
    end

    def discard_jobs(status:, offset: 0, limit: nil, **filters)
      selected = jobs(status:, offset:, limit:, **filters)
      execution_class(status).discard_all_from_jobs(selected)
    end

    def discard_job(active_job_id, status:, **filters)
      job = find_job(active_job_id, status:, **filters)
      return unless job

      mongodb? ? job.discard : job.public_send("#{STATUS_MAP.fetch(status.to_sym)}_execution").discard
    end

    def dispatch_job(active_job_id, status:, **filters)
      job = find_job(active_job_id, status:, **filters)
      return unless job

      if mongodb?
        Mongo.transaction(operation: "admin_dispatch_job") do
          update = if job.blocked?
            { "$set" => { state: "ready" }, "$unset" => { expires_at: true } }
          else
            { "$set" => { scheduled_at: Time.current } }
          end
          result = mongo_collection(:jobs).update_one(
            { _id: job.bson_id, state: job.state },
            update,
            **mongo_options
          )
          result.modified_count == 1
        end
      elsif job.blocked?
        Job.transaction do
          job.dispatch_bypassing_concurrency_limits
          job.blocked_execution.destroy!
        end
      else
        job.scheduled_execution.update!(scheduled_at: Time.current)
      end
    end

    def batches(status:, offset: 0, limit: nil)
      if mongodb?
        selector = mongo_batch_selector(status)
        cursor = mongo_collection(:batches).find(selector, **mongo_options).sort(_id: -1).skip(offset)
        cursor = cursor.limit(limit) if limit
        cursor.map { |document| Batch.from_document(document) }
      else
        scope = active_record_batch_scope(status).order(id: :desc).offset(offset)
        scope = scope.limit(limit) if limit
        scope.to_a
      end
    end

    def batches_count(status:, limit: nil)
      if mongodb?
        count = mongo_collection(:batches).count_documents(mongo_batch_selector(status), **mongo_options)
        limit ? [ count, limit ].min : count
      else
        scope = active_record_batch_scope(status)
        limit ? scope.limit(limit).count : scope.count
      end
    end

    def find_batch(id)
      if mongodb?
        document = mongo_collection(:batches).find({ _id: Mongo.id(id) }, **mongo_options).first
        document && Batch.from_document(document)
      else
        Batch.find_by(id: id)
      end
    end

    # Counts all requested states for each batch in a bounded set. Finished
    # batches use their frozen counters and therefore do not hit the jobs
    # collection.
    def batch_job_counts(batches, statuses: STATUS_MAP.keys - [ :finished ])
      live = batches.reject { |batch| batch.finished_at.present? }
      return {} if live.empty?

      if mongodb?
        batch_ids = live.map { |batch| Mongo.id(batch.id) }
        requested_states = statuses.filter_map { |status| STATUS_MAP[status.to_sym]&.to_s }
        rows = mongo_collection(:jobs).aggregate([
          { "$match" => { "batch_id" => { "$in" => batch_ids }, "state" => { "$in" => requested_states } } },
          { "$group" => { "_id" => { "batch_id" => "$batch_id", "state" => "$state" }, "count" => { "$sum" => 1 } } }
        ], **mongo_options)
        outstanding = mongo_collection(:batch_executions).aggregate([
          { "$match" => { "batch_id" => { "$in" => batch_ids }, "kind" => { "$ne" => "logical" } } },
          { "$group" => { "_id" => "$batch_id", "count" => { "$sum" => 1 } } }
        ], **mongo_options)

        result = Hash.new { |hash, key| hash[key] = Hash.new(0) }
        rows.each { |row| result[row.dig("_id", "batch_id").to_s][status_for_state(row.dig("_id", "state"))] = row["count"] }
        outstanding.each { |row| result[row["_id"].to_s][:outstanding] = row["count"] }
        result
      else
        batch_ids = live.map(&:id)
        jobs = Job.where(batch_id: batch_ids).group(:batch_id)
        result = Hash.new { |hash, key| hash[key] = Hash.new(0) }
        statuses.each do |status|
          klass = execution_class(status)
          klass.joins(:job).merge(jobs).group(:batch_id).count.each { |batch_id, count| result[batch_id.to_s][status.to_sym] = count }
        end
        BatchExecution.where(batch_id: batch_ids).group(:batch_id).count.each { |batch_id, count| result[batch_id.to_s][:outstanding] = count }
        result
      end
    end

    def job_attributes(job, status: nil)
      if mongodb?
        {
          raw_data: job.attributes.merge("arguments" => job.arguments),
          failed_at: job.failed? ? job.finished_at : nil,
          error: job.error,
          blocked_until: job.blocked? ? job.expires_at : nil,
          worker_id: job.claimed? ? job.process_id : nil,
          started_at: job.claimed? ? job.claimed_at : nil,
          display_name: job.display_name
        }
      else
        attributes = { raw_data: job.as_json, display_name: job.display_name }
        case (STATUS_MAP[status&.to_sym] || job.status)&.to_sym
        when :failed
          attributes[:failed_at] = job.failed_execution.created_at
          attributes[:error] = job.failed_execution.error
        when :blocked
          attributes[:blocked_until] = job.blocked_execution.expires_at
        when :claimed
          attributes[:worker_id] = job.claimed_execution.process_id
          attributes[:started_at] = job.claimed_execution.created_at
        end
        attributes
      end
    end

    def processes(offset: 0, limit: nil, kind: "Worker")
      if mongodb?
        cursor = mongo_collection(:processes).find({ kind: kind }, **mongo_options).sort(_id: 1).skip(offset)
        cursor = cursor.limit(limit) if limit
        cursor.map { |document| Process.from_document(document) }
      else
        scope = Process.where(kind: kind).offset(offset)
        scope = scope.limit(limit) if limit
        scope.to_a
      end
    end

    def processes_count(kind: "Worker")
      mongodb? ? mongo_collection(:processes).count_documents({ kind: kind }, **mongo_options) : Process.where(kind: kind).count
    end

    def find_process(id)
      if mongodb?
        document = mongo_collection(:processes).find({ _id: Mongo.id(id) }, **mongo_options).first
        document && Process.from_document(document)
      else
        Process.find_by(id: id)
      end
    end

    def recurring_tasks
      mongodb? ? RecurringTask.admin_all : RecurringTask.all.to_a
    end

    def find_recurring_task(key)
      mongodb? ? RecurringTask.admin_find(key) : RecurringTask.find_by(key: key)
    end

    def recurring_last_enqueued_at(task_keys)
      return {} if task_keys.empty?

      if mongodb?
        RecurringExecution.last_enqueued_at_by_task(task_keys)
      else
        RecurringExecution.where(task_key: task_keys).group(:task_key).maximum(:run_at)
      end
    end

    def enqueue_recurring_task(key, at: Time.current)
      find_recurring_task(key)&.enqueue(at: at)
    end

    def recurring_task_valid?(key)
      find_recurring_task(key)&.valid?
    end

    def execution_class(status)
      state = STATUS_MAP.fetch(status.to_sym)
      "SolidQueue::#{state.to_s.capitalize}Execution".constantize
    end
    private_class_method :execution_class

    def active_record_jobs(status:, offset:, limit:, **filters)
      scope = active_record_jobs_scope(status:, **filters)
      scope = if status&.to_sym == :finished
        scope.order(finished_at: :desc)
      elsif filters[:recurring_task_id].present?
        scope.order(id: :desc)
      elsif status&.to_sym == :scheduled
        scope.order(scheduled_at: :asc, priority: :asc, id: :asc)
      elsif status&.to_sym == :failed
        scope.order(id: :desc)
      else
        scope.order(id: :asc)
      end
      scope = scope.offset(offset) if offset
      scope = scope.limit(limit) if limit
      scope = if status && status.to_sym != :finished
        scope.includes(:"#{STATUS_MAP.fetch(status.to_sym)}_execution")
      elsif filters[:recurring_task_id].present?
        scope.includes(:ready_execution, :claimed_execution, :failed_execution, :blocked_execution, :scheduled_execution)
      else
        scope
      end
      scope.to_a
    end
    private_class_method :active_record_jobs

    def active_record_jobs_scope(status:, active_job_id: nil, queue_name: nil, job_class_name: nil, worker_id: nil, recurring_task_id: nil, batch_id: nil, finished_at: nil, scheduled_at: nil, enqueued_at: nil)
      if recurring_task_id.present?
        scope = Job.joins(:recurring_execution).where(solid_queue_recurring_executions: { task_key: recurring_task_id })
        scope = if status&.to_sym == :finished
          scope.merge(Job.finished)
        elsif status
          scope.joins(:"#{STATUS_MAP.fetch(status.to_sym)}_execution")
        else
          scope
        end
      elsif status&.to_sym == :finished
        scope = Job.finished
      else
        scope = Job.joins(:"#{STATUS_MAP.fetch(status.to_sym)}_execution")
      end

      scope = scope.where(solid_queue_jobs: { active_job_id: active_job_id }) if active_job_id.present?
      scope = scope.where(solid_queue_jobs: { queue_name: queue_name }) if queue_name.present?
      scope = scope.where(solid_queue_jobs: { class_name: job_class_name }) if job_class_name.present?
      scope = scope.where(solid_queue_jobs: { batch_id: batch_id }) if batch_id.present?
      if worker_id.present? && status&.to_sym == :in_progress
        scope = scope.where(solid_queue_claimed_executions: { process_id: worker_id })
      elsif worker_id.present?
        raise ArgumentError, "Filtering by worker id is only supported for in-progress jobs"
      end
      scope = scope.where(solid_queue_jobs: { finished_at: finished_at }) if finished_at.present?
      scope = scope.where(solid_queue_jobs: { scheduled_at: scheduled_at }) if scheduled_at.present?
      scope = scope.where(solid_queue_jobs: { created_at: enqueued_at }) if enqueued_at.present?
      scope
    end
    private_class_method :active_record_jobs_scope

    def mongo_jobs(status:, offset:, limit:, **filters)
      selector = mongo_job_selector(status:, **filters)
      sort = if status&.to_sym == :finished
        { finished_at: -1 }
      elsif filters[:recurring_task_id].present? || status&.to_sym == :failed
        { _id: -1 }
      elsif status&.to_sym == :scheduled
        { scheduled_at: 1, priority: 1, _id: 1 }
      else
        { _id: 1 }
      end
      cursor = mongo_collection(:jobs).find(selector, **mongo_options).sort(sort).skip(offset || 0)
      cursor = cursor.limit(limit) if limit
      cursor.map { |document| Job.from_document(document) }
    end
    private_class_method :mongo_jobs

    def mongo_job_selector(status:, active_job_id: nil, queue_name: nil, job_class_name: nil, worker_id: nil, recurring_task_id: nil, batch_id: nil, finished_at: nil, scheduled_at: nil, enqueued_at: nil)
      selector = {}
      selector[:state] = STATUS_MAP.fetch(status.to_sym).to_s if status
      selector[:active_job_id] = active_job_id if active_job_id.present?
      selector[:queue_name] = queue_name if queue_name.present?
      selector[:class_name] = job_class_name if job_class_name.present?
      selector[:process_id] = Mongo.id(worker_id) if worker_id.present?
      raise ArgumentError, "Filtering by worker id is only supported for in-progress jobs" if worker_id.present? && status&.to_sym != :in_progress
      selector[:batch_id] = Mongo.id(batch_id) if batch_id.present?
      selector[:finished_at] = range if finished_at.present? && (range = mongo_range(finished_at)).present?
      selector[:scheduled_at] = range if scheduled_at.present? && (range = mongo_range(scheduled_at)).present?
      selector[:created_at] = range if enqueued_at.present? && (range = mongo_range(enqueued_at)).present?

      if recurring_task_id.present?
        ids = mongo_collection(:recurring_executions).find(
          { task_key: recurring_task_id },
          projection: { job_id: 1 },
          **mongo_options
        ).map { |row| row["job_id"] || row[:job_id] }
        selector[:_id] = { "$in" => ids }
      end
      selector
    end
    private_class_method :mongo_job_selector

    def mongo_range(value)
      return value unless value.is_a?(Range)

      {}.tap do |filter|
        filter["$gte"] = value.begin unless value.begin.nil?
        filter[value.exclude_end? ? "$lt" : "$lte"] = value.end unless value.end.nil?
      end
    end
    private_class_method :mongo_range

    def active_record_batch_scope(status)
      case status&.to_sym
      when :finished then Batch.finished
      when :unfinished then Batch.unfinished
      when :failed then Batch.failed
      else Batch.all
      end
    end
    private_class_method :active_record_batch_scope

    def mongo_batch_selector(status)
      case status&.to_sym
      when :finished then { finished_at: { "$exists" => true, "$ne" => nil } }
      when :unfinished then { finished_at: nil }
      when :failed then { failed_at: { "$exists" => true, "$ne" => nil } }
      else {}
      end
    end
    private_class_method :mongo_batch_selector

    def status_for_state(state)
      STATUS_MAP.key(state.to_sym)
    end
    private_class_method :status_for_state

    def mongodb?
      SolidQueue.respond_to?(:mongodb?) && SolidQueue.mongodb?
    end
    private_class_method :mongodb?

    def mongo_collection(name)
      Mongo.collection(name)
    end
    private_class_method :mongo_collection

    def mongo_options
      Mongo.session_options
    end
    private_class_method :mongo_options
  end
end
