class AddClaimStartsToSolidQueue < ActiveRecord::Migration[7.1]
  def up
    add_column :solid_queue_claimed_executions, :started_at, :datetime, if_not_exists: true
    drop_table :solid_queue_deduplications, if_exists: true
    remove_column :solid_queue_jobs, :deduplication_key, :string, if_exists: true
  end

  def down
    remove_column :solid_queue_claimed_executions, :started_at, :datetime, if_exists: true
  end
end
