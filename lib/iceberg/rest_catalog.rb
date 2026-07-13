module Iceberg
  class RestCatalog < Catalog
    # warehouse is passed to REST server
    def initialize(uri:, warehouse: nil, properties: {}, default_namespace: nil)
      _initialize(
        RbCatalog.new_rest(uri, warehouse, properties),
        default_namespace:
      )
    end
  end
end
