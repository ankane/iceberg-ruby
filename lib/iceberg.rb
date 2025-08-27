# ext
begin
  require "iceberg/#{RUBY_VERSION.to_f}/iceberg"
rescue LoadError
  require "iceberg/iceberg"
end

# modules
require_relative "iceberg/catalog"
require_relative "iceberg/schema"
require_relative "iceberg/table"
require_relative "iceberg/static_table"
require_relative "iceberg/table_definition"
require_relative "iceberg/version"

# catalogs
require_relative "iceberg/glue_catalog"
require_relative "iceberg/memory_catalog"
require_relative "iceberg/rest_catalog"
require_relative "iceberg/sql_catalog"

module Iceberg
  class Error < StandardError; end
  class InvalidDataError < Error; end
  class NamespaceAlreadyExistsError < Error; end
  class NamespaceNotFoundError < Error; end
  class TableAlreadyExistsError < Error; end
  class TableNotFoundError < Error; end
  class UnsupportedFeatureError < Error; end

  class Todo < Error
    def message
      "not implemented yet"
    end
  end
end
