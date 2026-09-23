class AddRunTimeLimitsToSolidQueue < ActiveRecord::Migration[7.1]
  def change
    add_column :solid_queue_claimed_executions, :timeout_at, :datetime, if_not_exists: true
    add_index :solid_queue_claimed_executions, :timeout_at, if_not_exists: true
  end
end
