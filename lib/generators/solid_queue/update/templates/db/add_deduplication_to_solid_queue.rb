class AddDeduplicationToSolidQueue < ActiveRecord::Migration[7.1]
  def change
    add_column :solid_queue_jobs, :deduplication_key, :string, if_not_exists: true
    add_column :solid_queue_claimed_executions, :started_at, :datetime, if_not_exists: true

    create_table :solid_queue_deduplications, if_not_exists: true do |t|
      t.string :key, null: false
      t.string :active_job_id, null: false
      t.bigint :job_id
      t.datetime :expires_at
      t.datetime :created_at, null: false

      t.index :key, unique: true
      t.index :active_job_id
      t.index :expires_at
    end
  end
end
