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

    if rest?
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
    assert_nil catalog.create_namespace("iceberg_ruby_test.nested")
    assert_includes catalog.list_namespaces, ["iceberg_ruby_test"]
    refute_includes catalog.list_namespaces, ["iceberg_ruby_test", "nested"]
    if memory?
      # TODO fix
      assert_includes catalog.list_namespaces("iceberg_ruby_test"), ["nested"]
    else
      assert_includes catalog.list_namespaces("iceberg_ruby_test"), ["iceberg_ruby_test", "nested"]
    end
    refute_includes catalog.list_namespaces("iceberg_ruby_test"), ["iceberg_ruby_test"]

    catalog.create_table("iceberg_ruby_test.nested.events")
    assert_nil catalog.drop_table("iceberg_ruby_test.nested.events")

    assert_nil catalog.drop_namespace("iceberg_ruby_test.nested")
  ensure
    drop_namespace("iceberg_ruby_test.nested")
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
    error = assert_raises(Iceberg::Error) do
      catalog.create_namespace("iceberg_ruby_test")
    end
    if memory?
      assert_match "Cannot create namespace", error.message
    elsif sql?
      assert_match "already exists", error.message
    else
      assert_equal "Tried to create a namespace that already exists", error.message
    end
  end

  def test_drop_namespace_if_exists
    catalog.drop_namespace("iceberg_ruby_test.events", if_exists: true)
  end

  def test_drop_namespace_missing
    error = assert_raises(Iceberg::Error) do
      catalog.drop_namespace("iceberg_ruby_test.events")
    end
    if memory? || sql?
      assert_match "No such namespace", error.message
    else
      assert_equal "Tried to drop a namespace that does not exist", error.message
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
      assert_match "is not empty", error.message
    end
  end
end
