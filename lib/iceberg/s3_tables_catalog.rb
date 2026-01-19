module Iceberg
  class S3TablesCatalog < Catalog
    def initialize(arn:)
      @catalog = RbCatalog.new_s3tables(arn)
    end
  end
end
