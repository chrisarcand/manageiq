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
        binding.pry
        metrics.flatten!

        flatten_metrics(metrics).each do |interval_name, by_resource|
          by_resource.each do |resource, by_timestamp|
            data       = by_timestamp.values
            write_rows(interval_name, resource, data)
          end
        end

        metrics
      end

      private

      def flatten_metrics(metrics)
        {}.tap do |index|
          metrics.each do |m|
            interval_name = m.fetch_path(:tags, :capture_interval_name)
            resource = m[:resource] || m.fetch_path(:tags, :resource_type).safe_constantize.find(m.fetch_path(:tags, :resource_id))
            fields = index.fetch_path(interval_name, resource, m[:timestamp]) || m[:tags].symbolize_keys.except(:resource_type, :resource_id).merge(:timestamp => m[:timestamp])
            fields[m[:metric_name].to_sym] = m[:value]
            index.store_path(interval_name, resource, m[:timestamp], fields)
          end
        end
      end

      def write_rows(interval_name, resource, data)
        raise NotImplementedError, "#{self.class} cannot process intervals of type #{interval_name}" unless interval_name == 'realtime'
        log_header = "[#{interval_name}]"
        _log.info("#{log_header} Processing #{data.length} performance rows...")

        Benchmark.realtime_block(:process_perfs) do
          binding.pry
        end

        Benchmark.realtime_block(:process_perfs_db) do
        end

        _log.info("#{log_header} Processing #{data.length} performance rows...Complete")
      end
    end
  end
end
