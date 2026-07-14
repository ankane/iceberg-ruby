require_relative "test_helper"

class PolarsTest < Minitest::Test
  include Polars::Testing

  # TODO remove in 0.12.0
  def setup
    skip
  end

  def test_to_polars
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("events", schema: df.schema)
    table.append(df)
    assert_frame_equal df, table.to_polars.collect
  end

  def test_to_polars_snapshot_id
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("events", schema: df.schema)
    table.append(df)
    snapshot_id = table.current_snapshot_id
    table.append(df)
    assert_frame_equal df, table.to_polars(snapshot_id:).collect
  end

  def test_to_polars_empty
    table = catalog.create_table("events") { |t| t.bigint "a" }
    df = table.to_polars.collect
    assert_equal ["a"], df.columns
    assert_equal [Polars::Int64], df.dtypes
  end

  def test_to_polars_schema_changes
    skip unless ENV["TEST_PYTHON"] && rest?

    system "python3", "test/support/schema_changes.py", exception: true

    table = catalog.load_table("events")
    expected = Polars::DataFrame.new({"c" => [1, 2, 3]})
    assert_frame_equal expected, table.to_polars.sort("c").collect
  end

  def test_append
    df =
      Polars::DataFrame.new([
        Polars::Series.new("bool", [true, false, true], dtype: Polars::Boolean),
        Polars::Series.new("i32", [1, 2, 3], dtype: Polars::Int32),
        Polars::Series.new("i64", [1, 2, 3], dtype: Polars::Int64),
        Polars::Series.new("f32", [1.5, 2.5, 3.5], dtype: Polars::Float32),
        Polars::Series.new("f64", [1.5, 2.5, 3.5], dtype: Polars::Float64)
        # Polars::Series.new("date", [Date.today, Date.today + 1, Date.today + 2], dtype: Polars::Date),
        # Polars::Series.new("datetime[us]", [Time.at(0), Time.at(1), Time.at(2)], dtype: Polars::Datetime.new("us")),
        # Polars::Series.new("str", ["one", "two", "three"], dtype: Polars::String),
        # Polars::Series.new("binary", ["one".b, "two".b, "three".b], dtype: Polars::Binary)
      ])
    table = catalog.create_table("events", schema: df.schema)
    assert_nil table.append(df)
    assert_frame_equal df, table.to_polars.collect
  end

  def test_append_column_order
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("events", schema: df.schema)
    table.append(df.with_columns("b", "a"))
    assert_frame_equal df, table.to_polars.collect
  end

  def test_append_type_mismatch
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    table = catalog.create_table("events", schema: df.schema)
    error = assert_raises(ArgumentError) do
      table.append(df.cast(Polars::Float64))
    end
    assert_match "target schema is not superset of current schema", error.message
  end

  def test_append_missing_column
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("events", schema: df.schema)
    error = assert_raises(ArgumentError) do
      table.append(df.drop("a"))
    end
    assert_match "target schema is not superset of current schema", error.message
  end

  def test_static_table_to_polars
    df = static_table.to_polars.collect
    assert_equal [Polars::Int32, Polars::String], df.dtypes
  end
end
