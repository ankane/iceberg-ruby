require_relative "test_helper"

class TableScanTest < Minitest::Test
  def test_plan_files
    table = create_events
    assert_equal 1, table.scan.plan_files.size
  end

  def test_snapshot
    table = create_events
    refute_nil table.scan.snapshot
  end

  def test_table
    table = create_events
    assert_equal table, table.scan.table
  end
end
