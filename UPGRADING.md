# Adopting the experimental MongoDB backend

Solid Queue includes an **experimental** native MongoDB backend. The SQL `:active_record` backend remains the default, so existing installations keep their configuration and schema. Active Job still uses `:solid_queue`; select `config.solid_queue.backend = :mongodb` for queue storage.

Drain an existing SQL queue before switching. Stop new enqueues, process or account for ready, scheduled, blocked, in-progress, and failed jobs, then stop the SQL queue processes. Deploy the MongoDB setting to producers and queue processes together. Keep the old records available for inspection; switching back also requires draining or accounting for MongoDB work. The backend setting does not transfer jobs.

Provide Ruby 3.2+, Rails 7.1+, `mongo >= 2.24, < 3`, and a transaction-capable MongoDB replica set or sharded cluster. For a new installation:

```bash
bin/rails generate solid_queue:install --backend=mongodb
bundle install
bin/rails solid_queue:prepare
bin/jobs
```

The generator adds the driver dependency and selects MongoDB in `config/application.rb` instead of installing an SQL queue schema. Set the backend in `config/application.rb` or an environment file, not in an initializer. Set `MONGODB_URI` or `config.solid_queue.mongo_url` before preparation; `config.solid_queue.mongo_database` overrides the URI database. For an existing application, install the driver and select the backend explicitly. Run `solid_queue:prepare` before starting producers or queue processes and on upgrades that change indexes; MongoDB index updates use this task rather than SQL migration tasks. It validates the topology and prepares collections and indexes; queue access raises a configuration error if they are missing. `bin/rails solid_queue:check` validates process configuration. `config.solid_queue.mongo_client` accepts a driver client or callable. MongoDB queue storage doesn't use Mongoid and doesn't require an SQL queue database.

Update consumers that expect integer queue IDs or Active Record relations. MongoDB job, process, and batch IDs exposed by native models and notifications are strings. Workers still respect configured queue order and priority; equal-priority ObjectIds are not globally insertion-ordered across processes within a second. Active Job arguments are kept as serialized JSON, preserving large integers beyond BSON int64. The MongoDB backend registers a `BSON::ObjectId` serializer. MongoDB limits a document to 16 MiB: Solid Queue reserves 128 KiB for lifecycle fields and raises `SolidQueue::Job::EnqueueError` for an oversized enqueue. Constrained and batched enqueues roll back together; failure details are bounded.

For atomic application and queue writes, use the same `Mongo::Client` and explicit `Mongo::Session` with `SolidQueue.with_mongo_session(session, client: client)`. Keep dispatcher concurrency and batch maintenance enabled to repair work deferred by caller-owned transactions, whose commits have no driver callback. Use a transactional outbox for a loss-free handoff between clients/databases that cannot share a transaction. Transactions may retry; make jobs with external effects idempotent. Graceful shutdown releases claims; abnormal process loss fails them. Active Job retry/discard policy and manual Admin retry/discard apply as on SQL. Finished-job cleanup follows `preserve_finished_jobs` and `clear_finished_jobs_after`; keep the installed cleanup schedule and dispatcher maintenance running. See the README's [MongoDB transactions](README.md#mongodb-transactions-and-delivery-guarantees) and [batch maintenance](README.md#batch-maintenance) for examples.

`SolidQueue::Admin` exposes native queue, job, batch, process, and recurring-task actions and queries. The released dashboard consumer expects Active Record relations; its MongoDB-aware adaptation has not been released. Focused lifecycle, payload, integration, and dashboard-adaptation checks have passed. The declared compatibility matrix and comparative benchmark remain release verification tools, not completed performance or broad-matrix proof.

# Upgrading to add deduplication
Deduplication (`deduplicates key: ...`) needs new SQL columns and a table. Fresh installs get them with the base schema; existing Active Record installations need to copy the migration and run it:

```bash
bin/rails solid_queue:update
bin/rails db:migrate
```

MongoDB installations run `bin/rails solid_queue:prepare` instead, which creates the deduplication collection and indexes.

# Upgrading to version 1.7.x
This version introduces support for grouping jobs into batches, which needs new tables. Fresh installs get them with the base schema; existing installations need to copy the migration that adds them and run it:

```bash
bin/rails solid_queue:update
bin/rails db:migrate
```

The migration is optional for now: until you run it, everything works as before, batches aside. It will become part of the required schema in Solid Queue 2.0.

The copied migration is yours to adapt—for example, on PostgreSQL with a large jobs table, you can build the jobs index concurrently (`algorithm: :concurrently` with `disable_ddl_transaction!`) so it doesn't block enqueues while it runs.

# Upgrading to version 1.5.x
Ruby 3.1 is no longer supported, as it reached end-of-life in March 2025. Solid Queue now requires Ruby 3.2 or newer. If you're still on Ruby 3.1, Bundler will continue to resolve solid_queue 1.4.x for you, but you won't receive any new versions until you upgrade Ruby.

Recurring schedules that don't specify a time zone are now interpreted in your application's configured time zone (`config.time_zone`) by default, instead of the system's local time. This only affects schedules without an explicit time zone (e.g. `every day at 9am`); schedules that already include one (e.g. `0 9 * * * America/New_York`) are unaffected.

If your `config.time_zone` differs from the system time where your processes run, recurring jobs may fire at a different wall-clock time than before. To keep the previous behavior, set:

```ruby
config.solid_queue.time_zone = nil
```

# Upgrading to version 1.x
The value returned for `enqueue_after_transaction_commit?` has changed to `true`, and it's no longer configurable. If you want to change this, you need to use Active Job's configuration options.

# Upgrading to version 0.9.x
This version has two breaking changes regarding configuration:
- The default configuration file has changed from `config/solid_queue.yml` to `config/queue.yml`.
- Recurring tasks are now defined in `config/recurring.yml` (by default). Before, they would be defined as part of the _dispatcher_ configuration. Now they've been upgraded to their own configuration file, and a dedicated process (the _scheduler_) to manage them. Check the _Recurring tasks_ section in the `README` to learn how to configure them in detail. They still follow the same format as before when they lived under `dispatchers > recurring_tasks`.

# Upgrading to version 0.8.x
*IMPORTANT*: This version collapsed all migrations into a single `db/queue_schema.rb`, that will use a separate `queue` database on install. If you're upgrading from a version < 0.6.0, you need to upgrade to 0.6.0 first, ensure all migrations are up-to-date, and then upgrade further. You don't have to switch to a separate `queue` database or use the new `db/queue_schema.rb` file, these are for people starting on a version >= 0.8.x. You can continue using your existing database (be it separate or the same as your app) as long as you run all migrations defined up to version 0.6.0.

# Upgrading to version 0.7.x

This version removed the new async mode introduced in version 0.4.0 and introduced a new binstub that can be used to start Solid Queue's supervisor.

To install the binstub `bin/jobs`, you can just run:
```
bin/rails generate solid_queue:install
```


# Upgrading to version 0.6.x

## New migration in 3 steps
This version adds two new migrations to modify the `solid_queue_processes` table. The goal of that migration is to add a new column that needs to be `NOT NULL`. This needs to be done with two migrations and the following steps to ensure it happens without downtime and with new processes being able to register just fine:
1. Run the first migration that adds the new column, nullable
2. Deploy the updated Solid Queue code that uses this column
2. Run the second migration. This migration does two things:
  - Backfill existing rows that would have the column as NULL
  - Make the column not nullable and add a new index

Besides, it adds another migration with no effects to the `solid_queue_recurring_tasks` table. This one can be run just fine whenever, as the column affected is not used.

To install the migrations:
```bash
$ bin/rails solid_queue:install:migrations
```

Or, if you're using a different database for Solid Queue:

```bash
$ bin/rails solid_queue:install:migrations DATABASE=<the_name_of_your_solid_queue_db>
```

And then follow the steps above, running first one, then deploying the code, then running the second one.

## New behaviour when workers are killed
From this version onwards, when a worker is killed and the supervisor can detect that, it'll fail in-progress jobs claimed by that worker. For this to work correctly, you need to run the above migration and ensure you restart any supervisors you'd have. 


# Upgrading to version 0.5.x
This version includes a new migration to improve recurring tasks. To install it, just run:

```bash
$ bin/rails solid_queue:install:migrations
```

Or, if you're using a different database for Solid Queue:

```bash
$ bin/rails solid_queue:install:migrations DATABASE=<the_name_of_your_solid_queue_db>
```

And then run the migrations.


# Upgrading to version 0.4.x
This version introduced an _async_ mode (this mode has been removed in version 0.7.0) to run the supervisor and have all workers and dispatchers run as part of the same process as the supervisor, instead of separate, forked, processes. Together with this, we introduced some changes in how the supervisor is started. Prior this change, you could choose whether you wanted to run workers, dispatchers or both, by starting Solid Queue as `solid_queue:work` or `solid_queue:dispatch`. From version 0.4.0, the only option available is:

```
$ bundle exec rake solid_queue:start
```
Whether the supervisor starts workers, dispatchers or both will depend on your configuration. For example, if you don't configure any dispatchers, only workers will be started. That is, with this configuration:

```yml
production:
  workers:
    - queues: [ real_time, background ]
      threads: 5
      polling_interval: 0.1
      processes: 3
```
the supervisor will run 3 workers, each one with 5 threads, and no supervisors. With this configuration:
```yml
production:
  dispatchers:
    - polling_interval: 1
      batch_size: 500
      concurrency_maintenance_interval: 300
```
the supervisor will run 1 dispatcher and no workers.


# Upgrading to version 0.3.x
This version introduced support for [recurring (cron-style) jobs](https://github.com/rails/solid_queue/blob/main/README.md#recurring-tasks), and it needs a new DB migration for it. To install it, just run:

```bash
$ bin/rails solid_queue:install:migrations
```

Or, if you're using a different database for Solid Queue:

```bash
$ bin/rails solid_queue:install:migrations DATABASE=<the_name_of_your_solid_queue_db>
```

And then run the migrations.
