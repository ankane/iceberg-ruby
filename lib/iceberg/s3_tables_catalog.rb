module Iceberg
  class S3TablesCatalog < Catalog
    def initialize(arn:, default_namespace: nil)
      _initialize(
        RbCatalog.new_s3tables(arn),
        default_namespace:
      )
    end
  end
end
