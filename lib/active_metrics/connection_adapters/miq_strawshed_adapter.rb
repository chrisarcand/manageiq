require 'active_metrics/connection_adapters/abstract_adapter'

module ActiveMetrics
  module ConnectionAdapters
    class MiqStrawshedAdapter < AbstractAdapter
      include Vmdb::Logging

      # TODO Use the actual configuration from the initializer or whatever
      def self.create_connection(_config)
        ActiveRecord::Base.connection
      end

      def write_multiple(*metrics)
        metrics.flatten!

        write_rows(denormalized_metrics(metrics))
      end

      private

      # Output:
      # {
      #   resource_1 => {
      #     timestamp_1 => { metric_name_1 => value_1, metric_name_2 => value_2 }
      #     timestamp_2 => { metric_name_1 => value_3, metric_name_2 => value_4 }
      #   },
      #   resource_2 => { ...
      # }
      def denormalized_metrics(raw_metrics)
        {}.tap do |index|
          raw_metrics.each do |m|
            resource = m[:resource] || m.fetch_path(:tags, :resource_type).safe_constantize.find(m.fetch_path(:tags, :resource_id))
            metric_name = m[:metric_name].to_sym
            metric_value = m[:value]

            values = index.fetch_path(resource, m[:timestamp]) || {}
            index.store_path(resource, m[:timestamp], values.merge(metric_name => m[:value]))
          end
        end
      end

      def write_rows(data)
        log_header = "[realtime]" # This adapter ONLY writes realtime metrics, currently
        _log.info("#{log_header} Processing all performance rows...")
        rows = []

        Benchmark.realtime_block(:process_perfs) do
          data.each do |resource, by_timestamp|
            by_timestamp.each do |timestamp, values|
              rows << "('%s', '%s', '%s', '%s')" % [timestamp, resource.id, resource.type, values.to_json]
            end
          end
        end

        Benchmark.realtime_block(:process_perfs_db) do
          # TODO: ON CONFLICT hook to merge values for idempotent captures
          query = <<-SQL
            INSERT INTO strawshed_metrics (timestamp, resource_id, resource_type, values) VALUES
              #{rows.join(',')};
          SQL
          raw_connection.execute(query)
        end

        _log.info("#{log_header} Processing #{data.length} performance rows...Complete")
      end
    end
  end
end
