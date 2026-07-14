require_relative "test_helper"

class TableTest < Minitest::Test
  def test_metadata
    table = create_events
    assert_equal 2, table.format_version
    assert_kind_of String, table.uuid
    if s3tables?
      assert_match "table-s3/metadata", table.location
    elsif glue?
      assert_match "iceberg_ruby_test.db/events", table.location
    else
      assert_match "iceberg_ruby_test/events", table.location
    end
    assert_equal 1, table.last_sequence_number
    assert_equal 2, table.next_sequence_number
    assert_equal 2, table.last_column_id
    assert_equal 999, table.last_partition_id
    assert_kind_of Time, table.last_updated_at
    assert_equal 0, table.schema_id
    assert_equal 0, table.default_partition_spec_id
    assert_kind_of Integer, table.current_snapshot_id
    assert_kind_of Array, table.sort_orders
    assert_kind_of Iceberg::SortOrder, table.default_sort_order
    assert_equal 0, table.default_sort_order_id
    assert_kind_of Hash, table.properties
    assert_kind_of Array, table.statistics
    assert_kind_of Array, table.partition_statistics
    assert_nil table.statistics_for_snapshot(0)
    assert_nil table.partition_statistics_for_snapshot(0)
    assert_kind_of Array, table.encryption_keys
    assert_equal 0, table.next_row_id
  end

  def test_snapshots
    table = create_events
    snapshots = table.snapshots
    assert_equal 1, snapshots.size

    snapshot = snapshots.last
    assert_kind_of Integer, snapshot[:snapshot_id]
    assert_nil snapshot.fetch(:parent_snapshot_id)
    assert_equal 1, snapshot[:sequence_number]
    if s3tables?
      assert_match "table-s3/metadata", snapshot[:manifest_list]
    elsif glue?
      assert_match "iceberg_ruby_test.db/events/metadata", snapshot[:manifest_list]
    else
      assert_match "iceberg_ruby_test/events/metadata", snapshot[:manifest_list]
    end
    assert_equal 0, snapshot[:schema_id]
  end

  def test_append
    table =
      catalog.create_table("events") do |t|
        t.boolean "boolean"
        t.int "int"
        t.long "long"
        t.float "float"
        t.double "double"
        t.date "date"
        t.timestamp "timestamp"
        t.string "string"
        t.binary "binary"
      end
    data = [
      {"boolean" => true, "int" => 1, "long" => 1, "float" => 1.5, "double" => 1.5, "date" => Date.today, "timestamp" => Time.at(0), "string" => "one", "binary" => "one".b},
      {"boolean" => false, "int" => 2, "long" => 2, "float" => 2.5, "double" => 2.5, "date" => Date.today + 1, "timestamp" => Time.at(1), "string" => "two", "binary" => "two".b},
      {"boolean" => true, "int" => 3, "long" => 3, "float" => 3.5, "double" => 3.5, "date" => Date.today + 2, "timestamp" => Time.at(2), "string" => "three", "binary" => "three".b}
    ]
    assert_nil table.append(data)
    assert_equal data, table.to_a
  end

  def test_append_type_mismatch
    table = catalog.create_table("events", schema: {"a" => "int"})
    error = assert_raises(TypeError) do
      table.append([{"a" => "one"}])
    end
    assert_equal "no implicit conversion of String into Integer", error.message
  end

  def test_append_missing_column
    table = catalog.create_table("events", schema: {"a" => "int", "b" => "string"})
    table.append([{"a" => 1}, {"a" => 2}, {"a" => 3}])
    assert_equal [{"a" => 1, "b" => nil}, {"a" => 2, "b" => nil}, {"a" => 3, "b" => nil}], table.to_a
  end

  def test_inspect
    table = create_events
    assert_equal table.inspect, table.to_s
    refute_match "@table", table.inspect
  end
end
