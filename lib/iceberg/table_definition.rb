module Iceberg
  class TableDefinition
    TYPES = %w[
      boolean int long float double decimal date time
      timestamp timestamptz timestamp_ns timestamptz_ns
      string fixed uuid binary
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
      type = type.to_s
      @fields << {
        id: @fields.size + 1,
        name: name.to_s,
        type: TYPE_ALIASES.fetch(type, type),
        required: !null,
        doc: comment,
        # no need for initial default (and not supported until v3)
        write_default: default,
        limit: limit,
        precision: precision,
        scale: scale
      }
    end
  end
end
