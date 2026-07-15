require_relative "test_helper"

class SchemaTest < Minitest::Test
  def test_fields
    fields = schema.fields
    assert_equal ["a", "b"], fields.map(&:name)
    assert_equal [Iceberg::IntType.new, Iceberg::LongType.new], fields.map(&:field_type)
  end

  def test_highest_field_id
    assert_equal 2, schema.highest_field_id
  end

  def test_inspect
    assert_match "#<Iceberg::Schema fields=[#<Iceberg::NestedField field_id=1", schema.inspect
  end

  private

  def schema
    @schema ||= catalog.create_table("events", schema: {"a" => "int", "b" => "long"}).schema
  end
end
