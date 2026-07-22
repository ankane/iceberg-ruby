require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"

$catalog = ENV["CATALOG"] || "rest"
puts "Using #{$catalog}"

class Minitest::Test
  def setup
    drop_namespace("iceberg_ruby_test")
    catalog.create_namespace("iceberg_ruby_test")
    GC.stress = true if stress?

    # avoid hanging
    GC.start
  end

  def teardown
    GC.stress = false if stress?
  end

  def stress?
    ENV["STRESS"]
  end

  def catalog
    @catalog ||= begin
      case $catalog
      when "glue"
        Iceberg::GlueCatalog.new(
          warehouse: "s3://#{s3_bucket}/glue_catalog",
          **catalog_options
        )
      when "memory"
        Iceberg::MemoryCatalog.new(
          warehouse: ENV.key?("S3_BUCKET") ? "s3://#{s3_bucket}/memory_catalog" : "#{tmpdir}/memory_catalog",
          **catalog_options
        )
      when "rest"
        Iceberg::RestCatalog.new(
          uri: ENV.fetch("REST_CATALOG_URI", "http://localhost:8181"),
          **catalog_options
        )
      when "s3tables"
        Iceberg::S3TablesCatalog.new(
          arn: ENV.fetch("S3_TABLES_ARN"),
          **catalog_options
        )
      when "sql"
        Iceberg::SqlCatalog.new(
          uri: "postgres://localhost/iceberg_ruby_test",
          warehouse: "#{tmpdir}/sql_catalog",
          **catalog_options
        )
      else
        raise "Unsupported catalog"
      end
    end
  end

  def catalog_options
    {
      default_namespace: "iceberg_ruby_test"
    }
  end

  # TODO clean-up
  def tmpdir
    @@tmpdir ||= Dir.mktmpdir
  end

  def s3_bucket
    ENV.fetch("S3_BUCKET")
  end

  def glue?
    catalog.is_a?(Iceberg::GlueCatalog)
  end

  def memory?
    catalog.is_a?(Iceberg::MemoryCatalog)
  end

  def rest?
    catalog.is_a?(Iceberg::RestCatalog)
  end

  def s3tables?
    catalog.is_a?(Iceberg::S3TablesCatalog)
  end

  def sql?
    catalog.is_a?(Iceberg::SqlCatalog)
  end

  def create_events
    table = catalog.create_table("events", schema: {"a" => "int", "b" => "string"})
    load_events(table)
    table
  end

  def load_events(table = nil)
    table ||= catalog.load_table("events")
    table.append([{"a" => 1, "b" => "one"}, {"a" => 2, "b" => "two"}, {"a" => 3, "b" => "three"}])
  end

  def drop_namespace(namespace)
    return unless catalog.namespace_exists?(namespace)

    catalog.list_tables(namespace).each do |t|
      if rest?
        catalog.drop_table(t)
      else
        catalog.purge_table(t)
      end
    end
    catalog.drop_namespace(namespace)
  end

  def static_table
    @static_table ||= Iceberg::StaticTable.new(File.expand_path("support/metadata.json", __dir__))
  end
end
