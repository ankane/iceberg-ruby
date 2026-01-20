module Iceberg
  class Table
    def initialize(table, catalog)
      @table = table
      @catalog = catalog
    end

    def format_version
      @table.format_version
    end

    def uuid
      @table.uuid
    end

    def location
      @table.location
    end

    def last_sequence_number
      @table.last_sequence_number
    end

    def next_sequence_number
      @table.next_sequence_number
    end

    def last_column_id
      @table.last_column_id
    end

    def last_partition_id
      @table.last_partition_id
    end

    def schemas
      @table.schemas
    end

    def schema_by_id(schema_id)
      @table.schema_by_id(schema_id)
    end

    def current_schema
      @table.current_schema
    end
    alias_method :schema, :current_schema

    def current_schema_id
      @table.current_schema_id
    end
    alias_method :schema_id, :current_schema_id

    def default_partition_spec_id
      @table.default_partition_spec_id
    end

    def snapshots
      @table.snapshots
    end

    def snapshot_by_id(snapshot_id)
      @table.snapshot_by_id(snapshot_id)
    end

    def history
      @table.history
    end

    def metadata_log
      @table.metadata_log
    end

    def current_snapshot
      @table.current_snapshot
    end

    def current_snapshot_id
      @table.current_snapshot_id
    end

    def properties
      @table.properties
    end

    def scan(snapshot_id: nil)
      TableScan.new(@table.scan(snapshot_id), self)
    end

    def to_polars(snapshot_id: nil, storage_options: nil)
      require "polars-df"

      if Gem::Version.new(Polars::VERSION) < Gem::Version.new("0.23")
        raise "Requires polars-df >= 0.23"
      end

      Polars.scan_iceberg(self, snapshot_id:, storage_options:)
    end

    def append(df)
      check_catalog
      @table = @table.append(df.arrow_c_stream, @catalog)
      nil
    end

    # hide internal state
    def inspect
      to_s
    end

    private

    def check_catalog
      raise Error, "Read-only table" if @catalog.nil?
    end
  end
end
