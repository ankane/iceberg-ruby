require_relative "test_helper"

class CreateTableTest < Minitest::Test
  def test_block
    table =
      catalog.create_table("iceberg_ruby_test.events") do |t|
        t.integer "a", default: 1
        t.bigint "b", null: false, comment: "Hello"
        t.string "c", default: "Test", comment: "World"
      end

    fields = table.schema.fields
    assert_equal [1, 2, 3], fields.map { |v| v[:id] }
    assert_equal ["a", "b", "c"], fields.map { |v| v[:name] }
    assert_equal ["int", "long", "string"], fields.map { |v| v[:type] }
    assert_equal [false, true, false], fields.map { |v| v[:required] }
    assert_equal [nil, nil, nil], fields.map { |v| v[:initial_default] }
    assert_equal [1, nil, "Test"], fields.map { |v| v[:write_default] }
    assert_equal [nil, "Hello", "World"], fields.map { |v| v[:doc] }
  end

  def test_block_types
    skip "Conversion from Timestamptz is not supported" if glue?

    table =
      catalog.create_table("iceberg_ruby_test.events") do |t|
        t.boolean "boolean", default: true
        t.int "int", default: 1
        t.integer "integer", default: 2
        t.long "long", default: 1
        t.bigint "bigint", default: 2
        t.float "float", default: 1
        t.double "double", default: 2
        t.date "date"
        t.timestamp "timestamp"
        t.timestamptz "timestamptz"
        t.string "string", default: "Test"
        t.uuid "uuid"
        t.binary "binary"
      end

    fields = table.schema.fields
    expected = %w[boolean int int long long float double date timestamp timestamptz string uuid binary]
    assert_equal expected, fields.map { |v| v[:type] }
    expected = [true, 1, 2, 1, 2, 1, 2, nil, nil, nil, "Test", nil, nil]
    assert_equal expected, fields.map { |v| v[:write_default] }
  end

  def test_schema_hash
    schema = {
      "a" => "int",
      "b" => "long"
    }
    table = catalog.create_table("iceberg_ruby_test.events", schema: schema)
    fields = table.schema.fields
    assert_equal ["a", "b"], fields.map { |v| v[:name] }
    assert_equal ["int", "long"], fields.map { |v| v[:type] }
  end

  def test_schema_hash_symbols
    schema = {
      a: "int",
      b: "long"
    }
    table = catalog.create_table("iceberg_ruby_test.events", schema: schema)
    fields = table.schema.fields
    assert_equal ["a", "b"], fields.map { |v| v[:name] }
    assert_equal ["int", "long"], fields.map { |v| v[:type] }
  end

  def test_schema_hash_empty
    table = catalog.create_table("iceberg_ruby_test.events", schema: {})
    assert_empty table.schema.fields
  end

  def test_schema_class
    table =
      catalog.create_table("iceberg_ruby_test.events") do |t|
        t.integer "a"
        t.bigint "b"
      end

    schema = table.schema
    catalog.drop_table("iceberg_ruby_test.events")
    table2 = catalog.create_table("iceberg_ruby_test.events", schema: schema)
    assert_equal schema.fields, table2.schema.fields
  end

  def test_no_schema_no_block
    table = catalog.create_table("iceberg_ruby_test.events")
    assert_empty table.schema.fields
  end

  def test_schema_block
    error = assert_raises(ArgumentError) do
      catalog.create_table("iceberg_ruby_test.events", schema: {}) { }
    end
    assert_equal "Must pass schema or block", error.message
  end

  def test_already_exists
    catalog.create_table("iceberg_ruby_test.events")
    error = assert_raises(Iceberg::Error) do
      catalog.create_table("iceberg_ruby_test.events")
    end
    assert_match "already exists", error.message
  end
end
