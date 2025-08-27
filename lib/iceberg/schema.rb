module Iceberg
  class Schema
    attr_reader :fields, :schema_id

    def initialize(fields, schema_id: nil)
      @fields = fields
      @schema_id = schema_id
    end
  end
end
