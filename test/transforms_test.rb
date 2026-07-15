require_relative "test_helper"

class TransformsTest < Minitest::Test
  def setup
    skip if rest? || sql? # TODO fix
    super
  end

  def test_identity
    assert_transform Iceberg::IdentityTransform.new, "int"
  end

  def test_bucket
    assert_transform Iceberg::BucketTransform.new(3), "int"
  end

  def test_truncate
    assert_transform Iceberg::TruncateTransform.new(3), "string"
    refute_transform Iceberg::TruncateTransform.new(3), "date"
  end

  def test_year
    assert_transform Iceberg::YearTransform.new, "timestamp"
    refute_transform Iceberg::YearTransform.new, "int"
  end

  def test_month
    assert_transform Iceberg::MonthTransform.new, "timestamp"
    refute_transform Iceberg::MonthTransform.new, "int"
  end

  def test_day
    assert_transform Iceberg::DayTransform.new, "timestamp"
    refute_transform Iceberg::DayTransform.new, "int"
  end

  def test_hour
    assert_transform Iceberg::HourTransform.new, "timestamp"
    refute_transform Iceberg::HourTransform.new, "int"
  end

  def test_void
    assert_transform Iceberg::VoidTransform.new, "int"
  end

  def test_unknown
    assert_transform Iceberg::UnknownTransform.new, "int"
  end

  private

  def assert_transform(transform, field_type)
    partition_spec = Iceberg::PartitionSpec.new(Iceberg::PartitionField.new(source_id: 1, field_id: 1000, transform: transform, name: "b"))
    table = catalog.create_table("events", schema: {"a" => field_type}, partition_spec:)
    assert_equal partition_spec, table.default_partition_spec
  end

  def refute_transform(transform, field_type)
    partition_spec = Iceberg::PartitionSpec.new(Iceberg::PartitionField.new(source_id: 1, field_id: 1000, transform: transform, name: "b"))
    error = assert_raises(Iceberg::InvalidDataError) do
      catalog.create_table("events", schema: {"a" => field_type}, partition_spec:)
    end
    assert_match(/Invalid source type: \S+ for transform/, error.message)
  end
end
