# TODO move class to Rust
module Iceberg
  class Schema
    def initialize(fields)
      @schema =
        if fields.is_a?(RbSchema)
          fields
        elsif fields.respond_to?(:arrow_c_schema)
          RbSchema.new(fields)
        else
          RbSchema.new(fields.map { |f| f.is_a?(NestedField) ? f : NestedField.new(f) })
        end
    end

    def fields
      @schema.fields.map(&:to_h)
    end

    def schema_id
      @schema.schema_id
    end

    def arrow_c_schema
      @schema.arrow_c_schema
    end
  end
end
