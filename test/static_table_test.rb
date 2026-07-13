require_relative "test_helper"

class StaticTableTest < Minitest::Test
  def test_schema
    fields = static_table.schema.fields
    assert_equal ["a", "b"], fields.map { |v| v[:name] }
    assert_equal ["int", "string"], fields.map { |v| v[:type] }
  end

  def test_location
    assert_equal "file:///tmp/warehouse/main.db/events", static_table.location
  end

  def test_to_a
    assert_empty static_table.to_a
  end

  def test_append
    error = assert_raises(Iceberg::Error) do
      static_table.append(nil)
    end
    assert_equal "Read-only table", error.message
  end
end
