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

  def test_snapshots
    skip unless supports_updates?

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    table.append(df)

    snapshots = table.snapshots
    assert_equal 1, snapshots.size

    snapshot = snapshots.last
    assert_kind_of Integer, snapshot[:snapshot_id]
    assert_nil snapshot.fetch(:parent_snapshot_id)
    assert_equal 1, snapshot[:sequence_number]
    assert_match "iceberg_ruby_test/events/metadata", snapshot[:manifest_list]
    assert_equal 0, snapshot[:schema_id]
  end

  def test_to_polars
    skip unless supports_updates?

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    table.append(df)
    assert_frame_equal df, table.to_polars.collect
  end

  def test_to_polars_snapshot_id
    skip unless supports_updates?

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    table = catalog.create_table("iceberg_ruby_test.events", schema: df.schema)
    table.append(df)
    snapshot_id = table.current_snapshot_id
    table.append(df)
    assert_frame_equal df, table.to_polars(snapshot_id:).collect
  end

  def test_to_polars_empty
    table = catalog.create_table("iceberg_ruby_test.events") { |t| t.bigint "a" }
    df = table.to_polars.collect
    assert_equal ["a"], df.columns
    assert_equal [Polars::Int64], df.dtypes
  end

  def test_to_polars_schema_changes
    skip unless ENV["TEST_PYTHON"] && rest?

    system "python3", "test/support/schema_changes.py", exception: true

    table = catalog.load_table("iceberg_ruby_test.events")
    expected = Polars::DataFrame.new({"c" => [1, 2, 3]})
    assert_frame_equal expected, table.to_polars(_schema_changes: true).sort("c").collect
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
      assert_nil table.append(df)
      assert_frame_equal df, table.to_polars.collect
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
    assert_frame_equal df, table.to_polars.collect
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
