module Iceberg
  class GlueCatalog < Catalog
    # warehouse is URI of S3 storage bucket
    def initialize(warehouse:, default_namespace: nil)
      _initialize(
        RbCatalog.new_glue(warehouse),
        default_namespace:
      )
    end
  end
end
