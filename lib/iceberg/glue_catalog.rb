module Iceberg
  class GlueCatalog < Catalog
    # warehouse is URI of S3 storage bucket
    def initialize(warehouse:)
      @catalog = RbCatalog.new_glue(warehouse)
    end
  end
end
