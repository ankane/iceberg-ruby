module Iceberg
  class MemoryCatalog < Catalog
    # warehouse is default storage location
    def initialize(warehouse: nil)
      @catalog = RbCatalog.new_memory(warehouse)
    end
  end
end
