# TODO move class to Rust in 0.12.0
module Iceberg
  class Schema
    attr_reader :fields, :schema_id, :_schema

    def initialize(fields, schema_id: nil, _schema: nil)
      if fields.respond_to?(:arrow_c_schema) || fields.is_a?(Array)
        _schema = RbSchema.new(fields)
        fields = nil
      end
      @fields = fields || _schema&.fields.map(&:to_h)
      @schema_id = schema_id || _schema&.schema_id
      @_schema = _schema
    end

    def arrow_c_schema
      @_schema&.arrow_c_schema
    end
  end
end
