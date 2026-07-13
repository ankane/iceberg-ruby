module Iceberg
  class SqlCatalog < Catalog
    # warehouse is default storage location
    # name is stored in SQL table
    def initialize(uri:, warehouse:, name: "main", properties: {}, default_namespace: nil)
      _initialize(
        RbCatalog.new_sql(uri, warehouse, name, properties),
        default_namespace:
      )
    end
  end
end
