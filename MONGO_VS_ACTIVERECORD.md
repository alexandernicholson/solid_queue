# MongoDB vs Active Record backends

Solid Queue has two persistence backends that implement the same model API:

- **Active Record** (default): `app/models/solid_queue/`, running on MySQL, PostgreSQL, or SQLite.
- **MongoDB** (experimental): `lib/solid_queue/mongo/models/solid_queue/`, running on the MongoDB Ruby Driver directly. It doesn't use Mongoid.

Workers, the dispatcher, the scheduler, the supervisor, and `SolidQueue::Admin` call the same class and instance methods on either backend. This document explains how the two backends differ underneath, and how their method names map onto each other.

## Loading

The backend is chosen by `config.solid_queue.backend`, which is read before initializers run. When it is `:mongodb`, the engine (`lib/solid_queue/engine.rb`) tells the autoloader to ignore `app/models` and to load `lib/solid_queue/mongo/models` instead. Both directories define the same constants (`SolidQueue::Job`, `SolidQueue::ReadyExecution`, and so on), so only one set is ever loaded.

`test/shared/persistence_contract.rb` lists the class and instance methods that both backends must implement. It runs against Active Record in `test/unit/persistence_contract_test.rb` and against MongoDB in `test/mongodb/persistence_contract_test.rb`.

## Data model

### Active Record: one table per execution state

A job is a row in `solid_queue_jobs`. What the job is currently doing is recorded by which **execution table** holds a row for it:

| Table | Meaning |
|---|---|
| `solid_queue_ready_executions` | waiting to be claimed |
| `solid_queue_scheduled_executions` | waiting for `scheduled_at` |
| `solid_queue_blocked_executions` | waiting for a concurrency slot |
| `solid_queue_claimed_executions` | claimed by a worker process |
| `solid_queue_failed_executions` | failed, with the error |

A finished job has no execution row, and has `finished_at` set when `preserve_finished_jobs` is on. Moving a job between states means deleting a row from one execution table and inserting one into another, inside a transaction. `Job#status` finds the state by checking which execution association is present.

### MongoDB: one document with a `state` field

A job attempt is a single document in `solid_queue_jobs`, and its `state` field is `ready`, `scheduled`, `blocked`, `claimed`, `failed`, or `finished`. The fields that live on execution rows in Active Record live on the job document instead: `process_id`, `claim_token`, `claim_generation`, `claimed_at`, `started_at`, `expires_at`, and `error`.

The execution classes still exist, but they are **views over the jobs collection**, not separate collections:

- `SolidQueue::Execution` subclasses `SolidQueue::Job`, not `Record`.
- Each subclass scopes its queries to its own state: `ReadyExecution.count` counts jobs with `state: "ready"`.
- `ReadyExecution.create_all_from_jobs(jobs)` sets `state: "ready"` on those job documents.
- `Execution#job_id` is the document's own `id`, and `Execution#job` rebuilds a `Job` from the same attributes.

Moving between states is a single conditional update on one document, for example `{ _id: id, state: "scheduled" } → $set state: "ready"`. The condition on the current state is what makes the transition safe, instead of a row lock.

Automatic Active Job retries create a new attempt document with the same `active_job_id`. Batch counts key on `active_job_id`, so they treat all attempts as one logical job.

### Other collections

| Collection | Active Record table | Notes |
|---|---|---|
| `solid_queue_processes` | `solid_queue_processes` | same shape |
| `solid_queue_semaphores` | `solid_queue_semaphores` | adds a `version` counter |
| `solid_queue_pauses` | `solid_queue_pauses` | same shape |
| `solid_queue_recurring_tasks` | `solid_queue_recurring_tasks` | arguments stored as BSON, not JSON text |
| `solid_queue_recurring_executions` | `solid_queue_recurring_executions` | not an `Execution` subclass in Mongo |
| `solid_queue_batches` | `solid_queue_batches` | adds a `version` counter, written by every batch mutation |
| `solid_queue_batch_executions` | `solid_queue_batch_executions` | adds `kind: "logical"` markers used to count distinct jobs per batch |

Mongo IDs are `BSON::ObjectId`s internally and strings everywhere Solid Queue exposes them (`Job#id`, `batch_id`, `process_id`, notification payloads).

## Queries and records

Active Record models return **relations**: lazy, chainable query objects. A lot of the Active Record code depends on this, for example `where(process_id: id).release_all`, `ReadyExecution.queued_as(name).discard_all_in_batches`, or `QueueSelector#scoped_relations` returning one relation per queue.

The Mongo models are built on `SolidQueue::Mongo::Document` (`lib/solid_queue/mongo/document.rb`), a small ActiveModel-based layer with fields, callbacks, validations, `find`, `find_by`, `count`, `delete_all`, `distinct_values_of`, `insert_all!`, `save!`, `destroy!`, and `reload`. There are **no relations**: class methods query the collection directly and return loaded arrays. Where Active Record would scope a relation, the Mongo model does one of these instead:

- takes a filter hash (`discard_all_in_batches(queue_name: "default")`, `distinct_values_of(:queue_name, filter)`),
- takes the records to act on as an argument (`ClaimedExecution.release_all(executions)`, `fail_all_with(error, executions)`),
- or returns plain values instead of relations (`QueueSelector#scoped_queues` returns queue names).

## Concurrency control

### Claiming jobs

**Active Record** selects candidates with `SELECT ... FOR UPDATE SKIP LOCKED`, so concurrent workers skip each other's rows. Then, in the same transaction, it inserts claimed executions and deletes the ready ones.

**MongoDB** has no row locks. `ReadyExecution.claim`:

1. **`select_candidates`** reads candidate IDs from the ready index, skipping IDs this process already has in flight. This in-process reservation is the counterpart of `SKIP LOCKED`.
2. **`lock_candidates`** runs one `update_many` over those IDs with the condition `state: "ready"`, setting `state: "claimed"`, `process_id`, a fresh `claim_token`, and incrementing `claim_generation`. Another process that got to a job first has already changed its state, so the update skips it.
3. It then reads back the documents carrying that `claim_token` to learn which jobs it won.

How the worker handles an `update_many` error depends on what the server returned:

- **Transport error, no server response:** the outcome is unknown, and the update might still be running on the server. The worker raises `AmbiguousClaimError`, an unrecoverable error, so the process is stopped and its claims are recovered like any other dead process, rather than guessing.
- **Server response with an error, such as a write-concern error:** the update was applied on the primary, so the worker keeps whatever the token read-back returns and runs those jobs. The read-back uses the collection's default read concern (normally `local`), not `majority`. A write-concern error means the claim may not have reached a majority, so after a failover it can roll back while the worker is running the job, and another worker can claim the job again. Jobs that need protection against this must be idempotent.

### Finalizing claims

Finishing, failing, or releasing a claim matches `process_id`, `claim_token`, and `claim_generation` together (`ClaimedExecution#ownership_filter`). A worker that lost its claim, for example after being pruned, can't finish or fail a newer claim on the same job. Active Record gets the same protection by locking the claimed execution row and checking it still exists.

### Transactions

Active Record uses ordinary database transactions and `after_commit` callbacks.

MongoDB requires a replica set or sharded cluster, because it uses multi-document transactions (`lib/solid_queue/mongo/transactions.rb`):

- Transactions that Solid Queue starts use snapshot reads and majority writes. When the app passes its own session through `SolidQueue.with_mongo_session` and has already started a transaction on it, Solid Queue joins that transaction and inherits its read and write concerns; the app must start it with `read_concern: { level: :snapshot }` (or `majority`) and `write_concern: { w: :majority }` to get the same guarantees.
- Outside transactions, queue collections use primary reads with the default read concern and `w: :majority` writes.
- Transient errors retry the whole transaction, and unknown commit results retry the commit, both within `mongo_transaction_timeout` (5 seconds by default).
- `SolidQueue::Mongo.after_commit` queues work to run only after a commit that Solid Queue owns.
- In-memory record changes made inside a transaction are rolled back if it aborts.
- Application writes and queue writes are atomic together only when the app wraps enqueueing in `SolidQueue.with_mongo_session` with the same client and session.

Many Mongo operations that are several statements in Active Record are single atomic document updates instead (`find_one_and_update`, conditional `update_one`, upserts), so they need no transaction at all.

### Semaphores

**Active Record** (`Semaphore::Proxy`) locks the semaphore row, or creates it with `value = limit - 1`, and falls back to a conditional decrement when a concurrent create wins.

**MongoDB** does it in one transaction: upsert the semaphore with `value: limit` if missing, then conditionally decrement where `value > 0`. Signalling is a conditional increment where `value < limit`.

On both backends the semaphore is a **lease**, not a mutex, even though the methods are named `acquire_concurrency_lock` and `release_concurrency_lock`:

- A job takes its permit when it becomes `ready`, not when `perform` starts, and the permit expires after the job's `duration`. Dispatcher maintenance then deletes the expired semaphore and can let another job with the same key proceed while the first is still waiting or running. Choose a `duration` longer than queue wait plus run time.
- Releasing a permit isn't tied to the job that holds it. After an expired semaphore is recreated, the old holder's release returns a slot on the new semaphore, so more jobs than `to:` can run at once until the counts settle.

Jobs whose correctness depends on never overlapping need their own guard, such as an idempotency key or a lock in the system they modify.

### Batches

Both backends track outstanding work with `batch_executions` and finish a batch when none remain. In Mongo, every addition and every completion check writes the parent batch document, bumping `version`. Two transactions racing on the same batch therefore conflict on that document, and MongoDB's write-conflict retry turns what would otherwise be a write-skew race into a serialized retry. The Active Record backend relies on row locks and a re-check instead, including a PostgreSQL-specific one in `Batch#finalize`.

## Enqueueing

**Active Record** inserts the job row, and `after_create :prepare_for_execution` dispatches or schedules it by inserting the matching execution row.

**MongoDB** decides the state **before** inserting a single job, so its document is written once with its final state:

- A job with no concurrency key or batch is inserted directly as `ready` or `scheduled`, without a transaction.
- Otherwise, one transaction creates batch markers, waits on the semaphore, and inserts the job as `ready`, `scheduled`, or `blocked`. With `on_conflict: :discard` it inserts nothing.
- `enqueue_all` inserts every document in one transaction. Documents that are due and have a concurrency key are inserted without a state and then dispatched in the same transaction, becoming `ready` or `blocked`, or deleted on `on_conflict: :discard`.
- Documents larger than 16 MiB minus a 128 KiB lifecycle reserve are rejected with `EnqueueError` before any write.

Because MongoDB decides state at insert time, it has no counterpart to Active Record's `Job.prepare_all_for_execution`, `dispatch_all`, or `schedule_all`. Scheduled jobs are dispatched in bulk by `ScheduledExecution.dispatch_jobs`.

## Method name mapping

The Mongo method names follow Active Record wherever a method does the same job. The tables below list where the names **differ**, or where a name is shared but works differently. Anything not listed has the same name and role on both backends.

### Same name, different mechanism

| Method | Active Record | MongoDB |
|---|---|---|
| `ReadyExecution.select_and_lock` | transaction with `FOR UPDATE SKIP LOCKED` | in-process candidate reservation, then token claim; returns `[claimed, candidates_seen]` for instrumentation |
| `ReadyExecution.select_candidates` | `SKIP LOCKED` select | reads IDs, skipping ones this process has in flight |
| `ReadyExecution.lock_candidates` | inserts claimed executions, deletes ready ones | `update_many` to `claimed` with a `claim_token`, then reads back by token |
| `Job#ready`, `Job#block` | create a ready or blocked execution row | set `state` on the job document |
| `Execution#discard_jobs` | deletes job rows | also completes batch tracking and removes recurring markers |
| `Execution.discard_all_in_batches` | runs on the current relation | takes filter keywords, e.g. `queue_name:` |
| `ClaimedExecution.release_all` / `fail_all_with` | run on the current relation | take the executions as an argument (default: every claimed execution) |
| `ClaimedExecution#failed_with` | finalize only | also wraps the finalization in `finalizing`, so `perform` calls it without wrapping it twice |
| `prioritized_within(range)` | scope on `ReadyExecution` and `ScheduledExecution` | private `Execution` helper that adds `$gte`/`$lt`/`$lte` bounds on `priority` to a filter hash |
| `Execution.discard_all_in_queue` | scopes to the queue's unfinished jobs, then `discard_all_in_batches` | `discard_all_in_batches(queue_name:)` |
| `Record.distinct_values_of` | loose index scan emulation on PostgreSQL | `distinct` on the collection; execution classes scope it to their state |
| `Semaphore.wait` / `signal` | `Proxy` with row lock and create-or-decrement | single transaction with upsert and conditional `$inc` |

### Different names

| Active Record | MongoDB | Why |
|---|---|---|
| `QueueSelector#scoped_relations` | `QueueSelector#scoped_queues` | returns queue names (or `[ nil ]` for all queues), not relations |
| `QueueSelector#relation` | `QueueSelector#model` | it is the model class, not a relation |
| `RecurringExecution.create_or_insert!` (public, raises `AlreadyRecorded`) | `RecurringExecution.create_unique_by` (private, returns `true`/`false`) | the Active Record name describes choosing between `create!` and `insert(unique_by:)` by database; Mongo always inserts and relies on the unique index. `record` still raises `AlreadyRecorded` |
| `BatchExecution` scopes `with_finished_jobs`, `with_failed_jobs` | `BatchExecution.sweep_stale_executions` | one `$lookup` aggregation replaces the two joined scopes |
| `batch.batch_executions.exists?` / `.count` | `BatchExecution.outstanding_for_batch?` / `count_for_batch` | no relations; these skip `logical` markers |
| `ClaimedExecution` scope `orphaned` | private `ClaimedExecution.orphaned` | `$lookup` against processes; returns loaded executions |
| `ScheduledExecution` scopes `due`, `next_batch` | private `ScheduledExecution.next_batch` | returns loaded jobs |
| `Execution::Dispatching.dispatch_jobs(job_ids)` | private `ScheduledExecution.dispatch_jobs(jobs)` | takes jobs, not IDs; only scheduled executions dispatch in bulk |
| `FailedExecution#expand_error_details_from_exception` (`before_save`) | `FailedExecution.error_from(exception)` | failure is a state change on the job document, so the error payload is built up front, with byte limits |
| `Job#destroy` on concurrency conflict | private `Job#destroy_pending!` | no callback-driven destroy; deletes the document and cleans up batch and recurring records explicitly |
| `Process#prune` | `Process#prune(cutoff:)` | re-checks the heartbeat cutoff atomically when deleting |
| `Process.prune` via `prunable.excluding(...).non_blocking_lock` | `Process.prune(excluding:)` | no lock; each prune is a conditional `find_one_and_delete` |
| `Queue#size` / `#latency` via `ReadyExecution.queued_as` | `Job.ready_metrics(queue_name)` | one helper for size and oldest `created_at` |
| `Pause.create_or_find_by!` / `where(...).delete_all` / `exists?` | `Pause.pause` / `resume` / `paused?` / `queue_names` | class methods over upserts and deletes |

### MongoDB only

These have no Active Record counterpart, because they deal with things Active Record doesn't have:

- `Job.find_many`, `Job.refresh_existing`: reload several jobs in one query, since there are no relations to reload.
- `Job.delete_recurring_markers`: removes recurring execution markers when jobs are deleted. Active Record gets this from `has_one :recurring_execution, dependent: :destroy` and foreign keys.
- `Job#arguments=`: arguments are stored as a JSON string inside the document.
- `BatchExecution.complete(job)`: removes a job's batch marker and schedules the completion check after commit. Active Record does this through `destroy!` callbacks.
- `ReadyExecution::AmbiguousClaimError`, `claimed_by_token`, `resolve_claim_outcome`: claim outcome resolution, described [above](#claiming-jobs).
- `RecurringTask.admin_all`, `admin_find`: used by `SolidQueue::Admin`, which queries Active Record through relations instead.
- `Process#claims`, `Process.admin_list`: currently have no callers.
- `SolidQueue::Mongo.prepare!`: creates collections and indexes (`bin/rails solid_queue:prepare`), replacing the SQL schema and migrations.
