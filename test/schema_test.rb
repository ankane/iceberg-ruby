require_relative "test_helper"

class SchemaTest < Minitest::Test
  def test_fields
    table =
      catalog.create_table("iceberg_ruby_test.events") do |t|
        t.integer "a"
        t.bigint "b"
      end

    fields = table.schema.fields
    assert_equal ["a", "b"], fields.map { |v| v[:name] }
    assert_equal ["int", "long"], fields.map { |v| v[:type] }
  end
end
