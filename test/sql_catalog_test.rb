require_relative "test_helper"

class SqlCatalogTest < Minitest::Test
  def test_tls
    catalog = tls_catalog("require")
    catalog.drop_table("iceberg_ruby_tls.events") if catalog.table_exists?("iceberg_ruby_tls.events")
    catalog.create_namespace("iceberg_ruby_tls", if_not_exists: true)

    table =
      catalog.create_table("iceberg_ruby_tls.events") do |t|
        t.long "id"
        t.string "name"
      end
    table.append([{"id" => 1, "name" => "one"}, {"id" => 2, "name" => "two"}])

    rows = catalog.load_table("iceberg_ruby_tls.events").to_a.sort_by { |r| r["id"] }
    assert_equal [{"id" => 1, "name" => "one"}, {"id" => 2, "name" => "two"}], rows
  end

  def test_tls_verify_ca
    error = assert_raises(Iceberg::Error) do
      tls_catalog("verify-ca")
    end
    assert_match "invalid peer certificate", error.message
  end

  def test_error_source
    error = assert_raises(Iceberg::Error) do
      Iceberg::SqlCatalog.new(uri: "postgres://localhost/iceberg_ruby_test?sslmode=bogus", warehouse: tmpdir)
    end
    assert_match "sqlx error", error.message
    assert_match "unknown value \"bogus\" for `ssl_mode`", error.message
  end

  private

  # Postgres server with TLS enabled and a self-signed certificate
  def tls_catalog(sslmode)
    skip unless ENV["TEST_SQL_TLS_URI"]

    Iceberg::SqlCatalog.new(
      uri: "#{ENV["TEST_SQL_TLS_URI"]}?sslmode=#{sslmode}",
      warehouse: "#{tmpdir}/sql_tls_catalog"
    )
  end
end
