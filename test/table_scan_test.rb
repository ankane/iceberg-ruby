require_relative "test_helper"

class TableScanTest < Minitest::Test
  def test_plan_files
    table = catalog.create_table("iceberg_ruby_test.events")
    assert_empty table.scan.plan_files
  end

  def test_snapshot
    table = catalog.create_table("iceberg_ruby_test.events")
    assert_nil table.scan.snapshot
  end

  def test_table
    table = catalog.create_table("iceberg_ruby_test.events")
    assert_equal table, table.scan.table
  end
end
