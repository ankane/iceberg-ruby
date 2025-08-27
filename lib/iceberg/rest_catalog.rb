module Iceberg
  class RestCatalog < Catalog
    # warehouse is passed to REST server
    def initialize(uri:, warehouse: nil, properties: {})
      @catalog = RbCatalog.new_rest(uri, warehouse, properties)
    end
  end
end
