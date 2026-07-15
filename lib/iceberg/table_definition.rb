module Iceberg
  class TableDefinition
    TYPES = %w[
      boolean int long float double decimal date time
      timestamp timestamptz timestamp_nano timestamptz_nano
      string uuid fixed binary
    ]

    TYPE_ALIASES = {
      "integer" => "int",
      "bigint" => "long"
    }

    attr_reader :fields

    def initialize
      @fields = []
    end

    (TYPES + TYPE_ALIASES.keys).each do |type|
      define_method type do |name, **options|
        column(name, type, **options)
      end
    end

    def column(name, type, null: true, default: nil, comment: nil, limit: nil, precision: nil, scale: nil)
      unless type.is_a?(Type)
        type = type.to_s
        type =
          case TYPE_ALIASES.fetch(type, type)
          when "boolean"
            BooleanType.new
          when "int"
            IntType.new
          when "long"
            LongType.new
          when "float"
            FloatType.new
          when "double"
            DoubleType.new
          when "decimal"
            DecimalType.new(precision, scale)
          when "date"
            DateType.new
          when "time"
            TimeType.new
          when "timestamp"
            TimestampType.new
          when "timestamptz"
            TimestamptzType.new
          when "timestamp_nano"
            TimestampNanoType.new
          when "timestamptz_nano"
            TimestamptzNanoType.new
          when "string"
            StringType.new
          when "uuid"
            UUIDType.new
          when "fixed"
            FixedType.new(limit)
          when "binary"
            BinaryType.new
          else
            type
          end
      end

      @fields << NestedField.new(
        field_id: @fields.size + 1,
        name: name.to_s,
        field_type: type,
        required: !null,
        doc: comment,
        # no need for initial default (and not supported until v3)
        write_default: default
      )
    end
  end
end
