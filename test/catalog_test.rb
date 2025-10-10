require_relative "test_helper"

class CatalogTest < Minitest::Test
  def test_tables
    assert_equal false, catalog.table_exists?("iceberg_ruby_test.events")
    refute_includes catalog.list_tables("iceberg_ruby_test"), ["iceberg_ruby_test", "events"]

    assert_kind_of Iceberg::Table, catalog.create_table("iceberg_ruby_test.events")

    assert_equal true, catalog.table_exists?("iceberg_ruby_test.events")
    assert_includes catalog.list_tables("iceberg_ruby_test"), ["iceberg_ruby_test", "events"]

    assert_nil catalog.rename_table("iceberg_ruby_test.events", "iceberg_ruby_test.events2")
    assert_equal true, catalog.table_exists?("iceberg_ruby_test.events2")
    assert_equal false, catalog.table_exists?("iceberg_ruby_test.events")

    assert_nil catalog.drop_table("iceberg_ruby_test.events2")

    assert_equal false, catalog.table_exists?("iceberg_ruby_test.events2")
    refute_includes catalog.list_tables("iceberg_ruby_test"), ["iceberg_ruby_test", "events2"]
  end

  def test_tables_array_ident
    assert_equal false, catalog.table_exists?(["iceberg_ruby_test", "events"])
    refute_includes catalog.list_tables("iceberg_ruby_test"), ["iceberg_ruby_test", "events"]

    assert_kind_of Iceberg::Table, catalog.create_table(["iceberg_ruby_test", "events"])

    assert_equal true, catalog.table_exists?(["iceberg_ruby_test", "events"])
    assert_includes catalog.list_tables("iceberg_ruby_test"), ["iceberg_ruby_test", "events"]

    assert_nil catalog.rename_table(["iceberg_ruby_test", "events"], ["iceberg_ruby_test", "events2"])
    assert_equal true, catalog.table_exists?(["iceberg_ruby_test", "events2"])
    assert_equal false, catalog.table_exists?(["iceberg_ruby_test", "events"])

    assert_nil catalog.drop_table(["iceberg_ruby_test", "events2"])

    assert_equal false, catalog.table_exists?(["iceberg_ruby_test", "events2"])
    refute_includes catalog.list_tables(["iceberg_ruby_test"]), ["iceberg_ruby_test", "events2"]
  end

  def test_tables_dot
    catalog.create_table(["iceberg_ruby_test", "events.dot"])
    assert_equal [["iceberg_ruby_test", "events.dot"]], catalog.list_tables("iceberg_ruby_test")
    assert_equal true, catalog.table_exists?(["iceberg_ruby_test", "events.dot"])
    assert_equal false, catalog.table_exists?("iceberg_ruby_test.events.dot")
  ensure
    catalog.drop_table(["iceberg_ruby_test", "events.dot"])
  end

  def test_drop_table_if_exists
    catalog.drop_table("iceberg_ruby_test.events", if_exists: true)
  end

  def test_drop_table_missing
    error = assert_raises(Iceberg::Error) do
      catalog.drop_table("iceberg_ruby_test.events")
    end
    if memory? || sql?
      assert_match "No such table", error.message
    else
      assert_equal "Tried to drop a table that does not exist", error.message
    end
  end

  def test_load_table_missing
    error = assert_raises(Iceberg::Error) do
      catalog.load_table("iceberg_ruby_test.events")
    end
    if memory? || sql?
      assert_match "No such table", error.message
    else
      assert_equal "Tried to load a table that does not exist", error.message
    end
  end

  def test_load_table_missing_namespace
    error = assert_raises(Iceberg::InvalidDataError) do
      catalog.load_table("events")
    end
    assert_match "Namespace identifier can't be empty!", error.message
  end

  def test_register_table_missing
    error = assert_raises(Iceberg::Error) do
      catalog.register_table("iceberg_ruby_test.events", "metadata.json")
    end
    if rest?
      assert_match "metadata.json is not a valid metadata file", error.message
    elsif sql?
      assert_match "Registering a table is not supported yet", error.message
    else
      assert_match "No such file or directory", error.message
    end
  end

  def test_inspect
    assert_equal catalog.inspect, catalog.to_s
    refute_match "@catalog", catalog.inspect
  end
end
