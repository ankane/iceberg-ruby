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

    def to_polars(snapshot_id: nil, storage_options: nil)
      require "polars-df"

      files = @table.scan(snapshot_id).plan_files
      if files.empty?
        schema =
          current_schema.fields.to_h do |field|
            dtype =
              case field[:type]
              when "int"
                Polars::Int32
              when "long"
                Polars::Int64
              when "double"
                Polars::Float64
              when "string"
                Polars::String
              when "timestamp"
                Polars::Datetime
              else
                raise Todo
              end

            [field[:name], dtype]
          end

        Polars::LazyFrame.new(schema: schema)
      else
        sources = files.map { |v| v[:data_file_path] }

        deletion_files = [
          "iceberg-position-delete",
          files.map.with_index
            .select { |v, i| v[:deletes].any? }
            .to_h { |v, i| [i, v[:deletes].map { |d| d[:file_path] }] }
        ]

        Polars.scan_parquet(
          sources,
          storage_options: storage_options,
          # TODO
          # cast_options: Polars::ScanCastOptions._default_iceberg,
          # allow_missing_columns: true,
          # extra_columns: "ignore",
          # _column_mapping: column_mapping,
          _deletion_files: deletion_files
        )
      end
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
