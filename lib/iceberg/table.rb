module Iceberg
  class Table
    def initialize(table, catalog)
      @table = table
      @catalog = catalog
    end

    def format_version
      metadata.format_version
    end

    def uuid
      metadata.uuid
    end

    def location
      metadata.location
    end

    def last_sequence_number
      metadata.last_sequence_number
    end

    def next_sequence_number
      metadata.next_sequence_number
    end

    def last_column_id
      metadata.last_column_id
    end

    def last_partition_id
      metadata.last_partition_id
    end

    def last_updated_at
      ms = metadata.last_updated_ms
      Time.at(ms / 1000, ms % 1000, :millisecond)
    end

    def schemas
      metadata.schemas
    end

    def schema_by_id(schema_id)
      metadata.schema_by_id(schema_id)
    end

    def current_schema
      metadata.current_schema
    end
    alias_method :schema, :current_schema

    def current_schema_id
      metadata.current_schema_id
    end
    alias_method :schema_id, :current_schema_id

    def default_partition_spec_id
      metadata.default_partition_spec_id
    end

    def snapshots
      metadata.snapshots
    end

    def snapshot_by_id(snapshot_id)
      metadata.snapshot_by_id(snapshot_id)
    end

    def history
      metadata.history
    end

    def metadata_log
      metadata.metadata_log
    end

    def current_snapshot
      metadata.current_snapshot
    end

    def current_snapshot_id
      metadata.current_snapshot_id
    end

    def sort_orders
      metadata.sort_orders
    end

    def default_sort_order
      metadata.default_sort_order
    end

    def default_sort_order_id
      metadata.default_sort_order_id
    end

    def properties
      metadata.properties
    end

    def statistics
      metadata.statistics
    end

    def partition_statistics
      metadata.partition_statistics
    end

    def statistics_for_snapshot(snapshot_id)
      metadata.statistics_for_snapshot(snapshot_id)
    end

    def partition_statistics_for_snapshot(snapshot_id)
      metadata.partition_statistics_for_snapshot(snapshot_id)
    end

    def encryption_keys
      metadata.encryption_keys
    end

    def next_row_id
      metadata.next_row_id
    end

    def scan(snapshot_id: nil)
      TableScan.new(@table.scan(snapshot_id), self)
    end

    def to_a(snapshot_id: nil)
      scan(snapshot_id: snapshot_id).to_a
    end

    def to_polars(snapshot_id: nil, storage_options: nil)
      require "polars-df"

      if Gem::Version.new(Polars::VERSION) < Gem::Version.new("0.26.1")
        raise "Requires polars-df >= 0.26.1"
      end

      Polars.scan_iceberg(self, snapshot_id:, storage_options:)
    end

    def append(df)
      check_catalog
      df = ArrowRecordBatch.new(df, schema.arrow_c_schema) if df.is_a?(Array)
      @table = @table.append(df.arrow_c_stream, @catalog)
      nil
    end

    # hide internal state
    def inspect
      to_s
    end

    private

    def metadata
      @table.metadata
    end

    def check_catalog
      raise Error, "Read-only table" if @catalog.nil?
    end
  end
end
