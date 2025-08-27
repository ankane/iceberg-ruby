module Iceberg
  class SqlCatalog < Catalog
    # warehouse is default storage location
    # name is stored in SQL table
    def initialize(uri:, warehouse:, name: "main", properties: {})
      @catalog = RbCatalog.new_sql(uri, warehouse, name, properties)
    end
  end
end
