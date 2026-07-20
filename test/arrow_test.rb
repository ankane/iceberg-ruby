require_relative "test_helper"

class ArrowTest < Minitest::Test
  def test_to_arrow
    table = create_events
    arr = table.scan.to_arrow
    assert_equal ["int32", "string"], arr.schema.fields.map(&:type)
    assert_equal [1, 2, 3], arr.child(0).to_a
    assert_equal ["one", "two", "three"], arr.child(1).to_a
  end
end
