require_relative "test_helper"

class NamespaceTest < Minitest::Test
  def test_namespaces
    catalog.drop_namespace("iceberg_ruby_test")

    assert_equal false, catalog.namespace_exists?("iceberg_ruby_test")
    refute_includes catalog.list_namespaces, ["iceberg_ruby_test"]

    assert_nil catalog.create_namespace("iceberg_ruby_test")

    assert_equal true, catalog.namespace_exists?("iceberg_ruby_test")
    assert_includes catalog.list_namespaces, ["iceberg_ruby_test"]

    properties = catalog.namespace_properties("iceberg_ruby_test")
    assert_kind_of Hash, properties

    if rest? || s3tables?
      assert_raises(Iceberg::UnsupportedFeatureError) do
        catalog.update_namespace("iceberg_ruby_test", properties: {})
      end
    else
      assert_nil catalog.update_namespace("iceberg_ruby_test", properties: {})
    end

    assert_nil catalog.drop_namespace("iceberg_ruby_test")

    assert_equal false, catalog.namespace_exists?("iceberg_ruby_test")
    refute_includes catalog.list_namespaces, ["iceberg_ruby_test"]
  end

  def test_namespaces_nested
    skip if s3tables? || glue? || rest?

    begin
      assert_nil catalog.create_namespace("iceberg_ruby_test.nested")
      assert_includes catalog.list_namespaces, ["iceberg_ruby_test"]
      refute_includes catalog.list_namespaces, ["iceberg_ruby_test", "nested"]
      assert_includes catalog.list_namespaces("iceberg_ruby_test"), ["iceberg_ruby_test", "nested"]
      refute_includes catalog.list_namespaces("iceberg_ruby_test"), ["iceberg_ruby_test"]

      catalog.create_table("iceberg_ruby_test.nested.events")
      assert_nil catalog.drop_table("iceberg_ruby_test.nested.events")

      assert_nil catalog.drop_namespace("iceberg_ruby_test.nested")
    ensure
      drop_namespace("iceberg_ruby_test.nested")
    end
  end

  def test_namespaces_dot
    error = assert_raises(ArgumentError) do
      catalog.create_namespace(["iceberg_ruby_test.dot"])
    end
    assert_equal "Unsupported namespace", error.message
  end

  def test_create_namespace_if_not_exists
    catalog.create_namespace("iceberg_ruby_test", if_not_exists: true)
  end

  def test_create_namespace_already_exists
    assert_raises(Iceberg::NamespaceAlreadyExistsError) do
      catalog.create_namespace("iceberg_ruby_test")
    end
  end

  def test_drop_namespace_if_exists
    catalog.drop_namespace("iceberg_ruby_test2", if_exists: true)
  end

  def test_drop_namespace_missing
    assert_raises(Iceberg::NoSuchNamespaceError) do
      catalog.drop_namespace("iceberg_ruby_test2")
    end
  end

  def test_drop_namespace_not_empty
    catalog.create_table("iceberg_ruby_test.events")
    if memory?
      catalog.drop_namespace("iceberg_ruby_test")
    else
      error = assert_raises(Iceberg::Error) do
        catalog.drop_namespace("iceberg_ruby_test")
      end
      if rest?
        assert_equal "Received response with unexpected status code", error.message
      else
        assert_match "is not empty", error.message
      end
    end
  end
end
