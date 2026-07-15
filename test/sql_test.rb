require_relative "test_helper"

class SqlTest < Minitest::Test
  def setup
    skip
  end

  def test_result
    create_events
    result = catalog.sql("SELECT * FROM events")
    assert_equal ["a", "b"], result.columns
    assert_equal [[1, "one"], [2, "two"], [3, "three"]], result.rows
    assert_equal [{"a" => 1, "b" => "one"}, {"a" => 2, "b" => "two"}, {"a" => 3, "b" => "three"}], result.to_a
    assert_equal ({"a" => 1, "b" => "one"}), result.first
    assert_equal false, result.empty?
  end

  def test_types
    assert_kind_of Integer, catalog.sql("SELECT 1").rows[0][0]
    assert_kind_of Float, catalog.sql("SELECT 1.0").rows[0][0]
    assert_kind_of Date, catalog.sql("SELECT current_date").rows[0][0]
    assert_kind_of Time, catalog.sql("SELECT current_timestamp").rows[0][0]
    assert_kind_of TrueClass, catalog.sql("SELECT true").rows[0][0]
    assert_kind_of FalseClass, catalog.sql("SELECT false").rows[0][0]
    assert_kind_of NilClass, catalog.sql("SELECT NULL").rows[0][0]
    assert_kind_of String, catalog.sql("SELECT 'hello'").rows[0][0]
  end

  def test_params
    assert_kind_of Integer, catalog.sql("SELECT $1", [1]).rows[0][0]
    assert_kind_of Float, catalog.sql("SELECT $1", [1.0]).rows[0][0]
    assert_kind_of Date, catalog.sql("SELECT $1", [Date.today]).rows[0][0]
    assert_kind_of Time, catalog.sql("SELECT $1", [Time.now]).rows[0][0]
    assert_kind_of TrueClass, catalog.sql("SELECT $1", [true]).rows[0][0]
    assert_kind_of FalseClass, catalog.sql("SELECT $1", [false]).rows[0][0]
    assert_kind_of NilClass, catalog.sql("SELECT $1", [nil]).rows[0][0]
    assert_kind_of String, catalog.sql("SELECT $1", ["hello"]).rows[0][0]
  end

  def test_extra_params
    # DataFusion does not throw error
    assert_equal [[1]], catalog.sql("SELECT $1", [1, 2]).rows
  end

  def test_insert
    catalog.sql("CREATE TABLE events (a int, b bigint)")
    catalog.sql("INSERT INTO events VALUES (CAST($1 AS int), $2)", [1, 2])
    # TODO fix
    error = assert_raises(Iceberg::Error) do
      catalog.sql("INSERT INTO events VALUES ($1, $2)", [1, 2])
    end
    assert_match "Incompatible type", error.message
  end

  def test_update
    create_events
    error = assert_raises(Iceberg::Error) do
      catalog.sql("UPDATE events SET b = $1 WHERE a = $2", ["two!", 2])
    end
    assert_match "UPDATE not supported for Base table", error.message
  end

  def test_delete
    create_events
    error = assert_raises(Iceberg::Error) do
      catalog.sql("DELETE FROM events WHERE a = $1", [2])
    end
    assert_match "DELETE not supported for Base table", error.message
  end

  def test_view
    catalog.sql("CREATE TABLE events (a bigint, b text)")
    catalog.sql("CREATE VIEW events_view AS SELECT a AS c, b AS d FROM events")
    load_events
    result = catalog.sql("SELECT * FROM events_view")
    # TODO fix
    assert_equal true, result.empty?
  end

  def test_view_data
    create_events
    error = assert_raises(Iceberg::Error) do
      catalog.sql("CREATE VIEW events_view AS SELECT a AS c, b AS d FROM events")
    end
    assert_match "register_table does not support tables with data", error.message
  end

  def test_empty_result
    create_events
    result = catalog.sql("SELECT * FROM events LIMIT 0")
    # TODO fix
    assert_equal [], result.columns
  end

  def test_multiple_statements
    error = assert_raises(Iceberg::Error) do
      catalog.sql("SELECT 1; SELECT 2")
    end
    assert_equal "This feature is not implemented: The context currently only supports a single SQL statement", error.message
  end

  def test_duplicate_columns
    error = assert_raises(Iceberg::Error) do
      catalog.sql("SELECT 123 AS a, 123 AS a")
    end
    assert_match "Projections require unique expression names", error.message
  end

  def test_schema_changes
    skip unless ENV["TEST_PYTHON"] && rest?

    system "python3", "test/support/schema_changes.py", exception: true

    result = catalog.sql("SELECT * FROM events ORDER BY 1")
    assert_equal ["c"], result.columns
    assert_equal [[1], [2], [3]], result.rows
  end
end
