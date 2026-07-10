# TODO move class to Rust in 0.12.0
module Iceberg
  class Schema
    # TODO remove schema_id in 0.12.0
    def initialize(fields, schema_id: nil)
      @schema =
        if fields.is_a?(RbSchema)
          fields
        elsif fields.respond_to?(:arrow_c_schema)
          RbSchema.new(fields)
        else
          RbSchema.new(fields.map { |f| f.is_a?(NestedField) ? f : NestedField.new(f) })
        end
      @schema_id = schema_id
    end

    def fields
      @schema.fields.map(&:to_h)
    end

    def schema_id
      @schema_id || @schema.schema_id
    end

    def arrow_c_schema
      @schema.arrow_c_schema
    end
  end
end
