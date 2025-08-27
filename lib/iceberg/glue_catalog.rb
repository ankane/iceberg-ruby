module Iceberg
  class GlueCatalog < Catalog
    # warehouse is URI of S3 storage bucket
    def initialize(warehouse:)
      # requires glue feature
      raise Error, "Feature not enabled" unless RbCatalog.respond_to?(:new_glue)

      @catalog = RbCatalog.new_glue(warehouse)
    end
  end
end
