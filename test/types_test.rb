require_relative "test_helper"

class TypesTest < Minitest::Test
  def test_primitive
    schema =
      Iceberg::Schema.new(
        Iceberg::NestedField.new(field_id: 1, name: "boolean", field_type: Iceberg::BooleanType.new),
        Iceberg::NestedField.new(field_id: 2, name: "int", field_type: Iceberg::IntType.new),
        Iceberg::NestedField.new(field_id: 3, name: "long", field_type: Iceberg::LongType.new),
        Iceberg::NestedField.new(field_id: 4, name: "float", field_type: Iceberg::FloatType.new),
        Iceberg::NestedField.new(field_id: 5, name: "double", field_type: Iceberg::DoubleType.new),
        Iceberg::NestedField.new(field_id: 6, name: "decimal", field_type: Iceberg::DecimalType.new(3, 2)),
        Iceberg::NestedField.new(field_id: 7, name: "date", field_type: Iceberg::DateType.new),
        Iceberg::NestedField.new(field_id: 8, name: "time", field_type: Iceberg::TimeType.new),
        Iceberg::NestedField.new(field_id: 9, name: "timestamp", field_type: Iceberg::TimestampType.new),
        Iceberg::NestedField.new(field_id: 10, name: "timestamptz", field_type: Iceberg::TimestamptzType.new),
        Iceberg::NestedField.new(field_id: 11, name: "timestamp_nano", field_type: Iceberg::TimestampNanoType.new),
        Iceberg::NestedField.new(field_id: 12, name: "timestamptz_nano", field_type: Iceberg::TimestamptzNanoType.new),
        Iceberg::NestedField.new(field_id: 13, name: "string", field_type: Iceberg::StringType.new),
        Iceberg::NestedField.new(field_id: 14, name: "uuid", field_type: Iceberg::UUIDType.new),
        Iceberg::NestedField.new(field_id: 15, name: "fixed", field_type: Iceberg::FixedType.new(3)),
        Iceberg::NestedField.new(field_id: 16, name: "binary", field_type: Iceberg::BinaryType.new)
      )
    table = catalog.create_table("events", schema: schema)
    assert_equal schema, table.schema
  end

  def test_struct
    struct_type =
      Iceberg::StructType.new(
        Iceberg::NestedField.new(field_id: 2, name: "boolean", field_type: Iceberg::BooleanType.new),
        Iceberg::NestedField.new(field_id: 3, name: "int", field_type: Iceberg::IntType.new)
      )
    schema =
      Iceberg::Schema.new(
        Iceberg::NestedField.new(field_id: 1, name: "struct", field_type: struct_type)
      )
    table = catalog.create_table("events", schema: schema)
    assert_equal schema, table.schema
  end

  def test_list
    list_type =
      Iceberg::ListType.new(
        Iceberg::NestedField.new(field_id: 2, name: "boolean", field_type: Iceberg::BooleanType.new)
      )
    schema =
      Iceberg::Schema.new(
        Iceberg::NestedField.new(field_id: 1, name: "list", field_type: list_type)
      )
    table = catalog.create_table("events", schema: schema)
    assert_equal schema, table.schema
  end

  def test_map
    map_type =
      Iceberg::MapType.new(
        Iceberg::NestedField.new(field_id: 2, name: "boolean", field_type: Iceberg::BooleanType.new),
        Iceberg::NestedField.new(field_id: 3, name: "int", field_type: Iceberg::IntType.new),
      )
    schema =
      Iceberg::Schema.new(
        Iceberg::NestedField.new(field_id: 1, name: "map", field_type: map_type)
      )
    table = catalog.create_table("events", schema: schema)
    assert_equal schema, table.schema
  end
end
