# TODO move class to Rust in 0.12.0
module Iceberg
  class Schema
    attr_reader :schema_id, :_schema

    def initialize(fields, schema_id: nil, _schema: nil)
      @_schema =
        if _schema
          _schema
        elsif fields.respond_to?(:arrow_c_schema)
          RbSchema.new(fields)
        else
          RbSchema.new(fields.map { |f| NestedField.new(f) })
        end
      @schema_id = schema_id || @_schema.schema_id
    end

    def fields
      @_schema.fields.map(&:to_h)
    end

    def arrow_c_schema
      @_schema.arrow_c_schema
    end
  end
end
