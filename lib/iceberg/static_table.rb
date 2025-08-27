module Iceberg
  class StaticTable < Table
    def initialize(metadata_location)
      table = RbTable.from_metadata_file(metadata_location)
      super(table, nil)
    end
  end
end
