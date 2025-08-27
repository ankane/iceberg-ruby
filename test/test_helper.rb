require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"

$catalog = ENV["CATALOG"] || "rest"
puts "Using #{$catalog}"

class Minitest::Test
  def setup
    drop_namespace("iceberg_ruby_test")
    catalog.create_namespace("iceberg_ruby_test")
  end

  def catalog
    @catalog ||= begin
      case $catalog
      when "glue"
        Iceberg::GlueCatalog.new(
          warehouse: "s3://#{s3_bucket}/glue_catalog"
        )
      when "memory"
        Iceberg::MemoryCatalog.new(
          warehouse: "#{tmpdir}/memory_catalog"
        )
      when "rest"
        Iceberg::RestCatalog.new(
          uri: "http://localhost:8181"
        )
      when "sql"
        Iceberg::SqlCatalog.new(
          uri: "postgres://localhost/iceberg_ruby_test",
          warehouse: "#{tmpdir}/sql_catalog"
        )
      else
        raise "Unsupported catalog"
      end
    end
  end

  # TODO clean-up
  def tmpdir
    @@tmpdir ||= Dir.mktmpdir
  end

  def s3_bucket
    ENV.fetch("S3_BUCKET")
  end

  def memory?
    catalog.is_a?(Iceberg::MemoryCatalog)
  end

  def rest?
    catalog.is_a?(Iceberg::RestCatalog)
  end

  def sql?
    catalog.is_a?(Iceberg::SqlCatalog)
  end

  def supports_updates?
    !memory? && !sql?
  end

  def drop_namespace(namespace)
    return unless catalog.namespace_exists?(namespace)

    catalog.list_tables(namespace).each do |t|
      catalog.drop_table(t)
    end
    catalog.drop_namespace(namespace)
  end
end
