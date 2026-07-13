module Iceberg
  class MemoryCatalog < Catalog
    # warehouse is default storage location
    def initialize(warehouse: nil, default_namespace: nil)
      _initialize(
        RbCatalog.new_memory(warehouse),
        default_namespace:
      )
    end
  end
end
