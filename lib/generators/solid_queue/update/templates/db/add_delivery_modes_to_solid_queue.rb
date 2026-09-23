class AddDeliveryModesToSolidQueue < ActiveRecord::Migration[7.1]
  def change
    add_column :solid_queue_jobs, :delivery_mode, :string, if_not_exists: true
  end
end
