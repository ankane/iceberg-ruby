# TODO move class to Rust in 0.12.0
module Iceberg
  class Schema
    attr_reader :fields, :schema_id

    def initialize(fields, schema_id: nil, _schema: nil)
      @fields = fields
      @schema_id = schema_id
      @_schema = _schema
    end

    def arrow_c_schema
      @_schema&.arrow_c_schema
    end
  end
end
