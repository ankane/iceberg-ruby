require_relative "test_helper"

class SqlTest < Minitest::Test
  def setup
    skip
    super
  end

  def test_result
    create_events
    result = catalog.sql("SELECT * FROM iceberg_ruby_test.events")
    assert_equal ["a", "b"], result.columns
    assert_equal [[1, "one"], [2, "two"], [3, "three"]], result.rows
    assert_equal [{"a" => 1, "b" => "one"}, {"a" => 2, "b" => "two"}, {"a" => 3, "b" => "three"}], result.to_a
    assert_equal ({"a" => 1, "b" => "one"}), result.first
    assert_equal false, result.empty?
  end

  def test_types
    assert_kind_of Integer, catalog.sql("SELECT 1").rows[0][0]
    assert_kind_of TrueClass, catalog.sql("SELECT true").rows[0][0]
    assert_kind_of FalseClass, catalog.sql("SELECT false").rows[0][0]
    assert_kind_of NilClass, catalog.sql("SELECT NULL").rows[0][0]
  end

  def test_params
    assert_kind_of Integer, catalog.sql("SELECT $1", [1]).rows[0][0]
    assert_kind_of Float, catalog.sql("SELECT $1", [1.0]).rows[0][0]
    assert_kind_of TrueClass, catalog.sql("SELECT $1", [true]).rows[0][0]
    assert_kind_of FalseClass, catalog.sql("SELECT $1", [false]).rows[0][0]
    assert_kind_of NilClass, catalog.sql("SELECT $1", [nil]).rows[0][0]
  end

  def test_update
    create_events
    error = assert_raises do
      catalog.sql("UPDATE iceberg_ruby_test.events SET b = $1 WHERE a = $2", ["two!", 2])
    end
    assert_match "UPDATE not supported for Base table", error.message
  end

  def test_delete
    create_events
    error = assert_raises do
      catalog.sql("DELETE FROM iceberg_ruby_test.events WHERE a = $1", [2])
    end
    assert_match "DELETE not supported for Base table", error.message
  end

  def test_view
    create_events
    error = assert_raises do
      catalog.sql("CREATE VIEW iceberg_ruby_test.events_view AS SELECT a AS c, b AS d FROM iceberg_ruby_test.events")
    end
    assert_match "register_table does not support tables with data", error.message
  end

  def test_empty_view
    catalog.sql("CREATE TABLE iceberg_ruby_test.events (a bigint, b text)")
    catalog.sql("CREATE VIEW iceberg_ruby_test.events_view AS SELECT a AS c, b AS d FROM iceberg_ruby_test.events")
    result = catalog.sql("SELECT * FROM iceberg_ruby_test.events_view")
    # TODO fix
    assert_equal [], result.columns
  end

  def test_error
    error = assert_raises do
      catalog.sql("SELECT 123 AS a, 123 AS a")
    end
    assert_match "Projections require unique expression names", error.message
  end

  private

  def create_events
    catalog.sql("CREATE TABLE iceberg_ruby_test.events (a bigint, b text)")
    load_events
  end

  def load_events
    params = [1, "one", 2, "two", 3, "three"]
    catalog.sql("INSERT INTO iceberg_ruby_test.events VALUES ($1, $2), ($3, $4), ($5, $6)", params)
  end
end
