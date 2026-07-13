require_relative "test_helper"

class SqlTest < Minitest::Test
  def setup
    skip
    super
  end

  def test_result
    create_events
    result = catalog.sql("SELECT * FROM iceberg_ruby_test.events")
    assert_equal ["a", "b"], result.columns
    assert_equal [[1, "one"], [2, "two"], [3, "three"]], result.rows
    assert_equal [{"a" => 1, "b" => "one"}, {"a" => 2, "b" => "two"}, {"a" => 3, "b" => "three"}], result.to_a
    assert_equal ({"a" => 1, "b" => "one"}), result.first
    assert_equal false, result.empty?
  end

  def test_error
    error = assert_raises do
      catalog.sql("SELECT 123 AS a, 123 AS a")
    end
    assert_match "Projections require unique expression names", error.message
  end

  private

  def create_events
    catalog.create_table("iceberg_ruby_test.events") do |t|
      t.bigint "a"
      t.string "b"
    end

    params = [1, "one", 2, "two", 3, "three"]
    result = catalog.sql("INSERT INTO iceberg_ruby_test.events VALUES ($1, $2), ($3, $4), ($5, $6)", params)
    assert_equal ["count"], result.columns
    assert_equal [[3]], result.rows
  end
end
