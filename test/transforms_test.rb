require_relative "test_helper"

class TransformsTest < Minitest::Test
  def test_identity
    partition_spec = Iceberg::PartitionSpec.new(Iceberg::PartitionField.new(source_id: 1, field_id: 1000, transform: Iceberg::IdentityTransform.new, name: "b"))
    table = catalog.create_table("events", schema: {"a" => "int"}, partition_spec:)
    assert_equal partition_spec, table.default_partition_spec
  end
end
