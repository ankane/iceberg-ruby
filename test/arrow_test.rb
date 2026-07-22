require_relative "test_helper"

class ArrowTest < Minitest::Test
  def test_from_arrow
    rows = [{"a" => 1, "b" => "one"}, {"a" => 2, "b" => nil}, {"a" => nil, "b" => "three"}]
    arr = Nanoarrow::Array.new(rows, Nanoarrow.struct({"a" => Nanoarrow.int64, "b" => Nanoarrow.string}))
    table = catalog.create_table("events", schema: arr.schema)
    table.append(arr)
    assert_equal rows, table.to_a
  end

  def test_to_arrow
    table = create_events
    arr = table.scan.to_arrow
    assert_equal ["int32", "string"], arr.schema.fields.map(&:type)
    assert_equal [1, 2, 3], arr.child(0).to_a
    assert_equal ["one", "two", "three"], arr.child(1).to_a
  end
end
