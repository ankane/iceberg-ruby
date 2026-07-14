require_relative "test_helper"

class SchemaTest < Minitest::Test
  def test_fields
    fields = schema.fields
    assert_equal ["a", "b"], fields.map { |v| v[:name] }
    assert_equal ["int", "long"], fields.map { |v| v[:type] }
  end

  def test_inspect
    assert_match "#<Iceberg::Schema fields=", schema.inspect
  end

  private

  def schema
    @schema ||= catalog.create_table("events", schema: {"a" => "int", "b" => "long"}).schema
  end
end
