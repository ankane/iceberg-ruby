require_relative "test_helper"

class TableTest < Minitest::Test
  def test_metadata
    table = catalog.create_table("iceberg_ruby_test.events")
    assert_equal 2, table.format_version
    assert_kind_of String, table.uuid
    assert_match "iceberg_ruby_test/events", table.location
    assert_equal 0, table.last_sequence_number
    assert_equal 1, table.next_sequence_number
    assert_equal 0, table.last_column_id
    assert_equal 999, table.last_partition_id
    assert_equal 0, table.schema_id
    assert_equal 0, table.default_partition_spec_id
    assert_nil table.current_snapshot_id
    assert_kind_of Hash, table.properties
  end

  def test_to_polars
    skip unless supports_updates?

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    table.append(df)
    assert_equal df, table.to_polars.collect
  end

  def test_to_polars_empty
    table = catalog.create_table("iceberg_ruby_test.events") { |t| t.bigint "a" }
    df = table.to_polars.collect
    assert_equal ["a"], df.columns
    assert_equal [Polars::Int64], df.dtypes
  end

  def test_append
    df = Polars::DataFrame.new([
      Polars::Series.new("i32", [1, 2, 3], dtype: Polars::Int32),
      Polars::Series.new("i64", [1, 2, 3], dtype: Polars::Int64),
      Polars::Series.new("f32", [1, 2, 3], dtype: Polars::Float32),
      Polars::Series.new("f64", [1, 2, 3], dtype: Polars::Float64),
      Polars::Series.new("bool", [true, false, true], dtype: Polars::Boolean)
    ])
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)

    if supports_updates?
      table.append(df)
      assert_equal df, table.to_polars.collect
    else
      assert_raises(Iceberg::UnsupportedFeatureError) do
        table.append(df)
      end
    end
  end

  def test_append_column_order
    skip unless supports_updates?

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    table.append(df.with_columns("b", "a"))
    assert_equal df, table.to_polars.collect
  end

  def test_append_type_mismatch
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    error = assert_raises(ArgumentError) do
      table.append(df.cast(Polars::Float64))
    end
    assert_match "target schema is not superset of current schema", error.message
  end

  def test_append_missing_column
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    error = assert_raises(ArgumentError) do
      table.append(df.drop("a"))
    end
    assert_match "target schema is not superset of current schema", error.message
  end

  def test_inspect
    table = catalog.create_table("iceberg_ruby_test.events") { |t| t.integer "a" }
    assert_equal table.inspect, table.to_s
    refute_match "@table", table.inspect
  end
end
