class AddUpsertedMetricsTable < ActiveRecord::Migration[5.0]
  def up
    execute <<-SQL
      CREATE TABLE strawshed_metrics (
        timestamp       timestamp,
        resource_id     bigint,
        resource_type   varchar,
        values          jsonb,
        PRIMARY KEY(timestamp, resource_id, resource_type)
      );
    SQL
  end

  def down
    drop_table :strawshed_metrics
  end
end
