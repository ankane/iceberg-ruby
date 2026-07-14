module Iceberg
  class Catalog
    def _initialize(catalog, default_namespace:)
      @catalog = catalog
      @default_namespace = default_namespace
    end

    def list_namespaces(parent = nil)
      @catalog.list_namespaces(parent)
    end

    def create_namespace(namespace, properties: {}, if_not_exists: nil)
      @catalog.create_namespace(namespace, properties)
    rescue Error => e
      # ideally all catalogs would use NamespaceAlreadyExistsError
      if !if_not_exists || (e.message != "Cannot create namespace" && !e.message.include?("already exists"))
        raise e
      end
      nil
    end

    def namespace_exists?(namespace)
      @catalog.namespace_exists?(namespace)
    end

    def namespace_properties(namespace)
      @catalog.namespace_properties(namespace)
    end

    def update_namespace(namespace, properties:)
      @catalog.update_namespace(namespace, properties)
    end

    def drop_namespace(namespace, if_exists: nil)
      @catalog.drop_namespace(namespace)
    rescue Error => e
      # ideally all catalogs would use NamespaceNotFoundError
      if !if_exists || (e.message != "Tried to drop a namespace that does not exist" && !e.message.include?("No such namespace") && !e.message.include?("The specified namespace does not exist") && !e.message.include?("not found"))
        raise e
      end
      nil
    end

    def list_tables(namespace = nil)
      @catalog.list_tables(namespace || @default_namespace)
    end

    def create_table(table_name, schema: nil, location: nil)
      if !schema.nil? && block_given?
        raise ArgumentError, "Must pass schema or block"
      end

      if block_given?
        table_definition = TableDefinition.new
        yield table_definition
        schema = Schema.new(table_definition.fields)
      elsif schema.is_a?(Schema)
        # do nothing
      elsif schema.respond_to?(:arrow_c_schema)
        schema = Schema.new(schema)
      elsif schema.is_a?(Hash)
        table_definition = TableDefinition.new
        schema.each do |k, v|
          table_definition.column(k, v)
        end
        schema = Schema.new(table_definition.fields)
      elsif schema.nil?
        schema = Schema.new([])
      end

      Table.new(@catalog.create_table(with_namespace(table_name), schema, location), @catalog)
    end

    def load_table(table_name)
      Table.new(@catalog.load_table(with_namespace(table_name)), @catalog)
    end

    def drop_table(table_name, if_exists: nil)
      @catalog.drop_table(with_namespace(table_name))
    rescue Error => e
      # ideally all catalogs would use TableNotFoundError
      if !if_exists || (e.message != "Tried to drop a table that does not exist" && !e.message.include?("No such table") && !e.message.include?("The specified table does not exist") && !e.message.include?("not found"))
        raise e
      end
      nil
    end

    def table_exists?(table_name)
      @catalog.table_exists?(with_namespace(table_name))
    rescue NamespaceNotFoundError
      false
    end

    def rename_table(table_name, new_name)
      @catalog.rename_table(with_namespace(table_name), with_namespace(new_name))
    end

    def register_table(table_name, metadata_location)
      @catalog.register_table(with_namespace(table_name), metadata_location)
    end

    def sql(sql, params = [])
      # requires datafusion feature
      raise Todo unless @catalog.respond_to?(:session_context)

      session_context.sql(sql, params)
    end

    # hide internal state
    def inspect
      to_s
    end

    private

    def with_namespace(table_name)
      if @default_namespace && table_name.is_a?(String) && !table_name.include?(".")
        [@default_namespace, table_name]
      else
        table_name
      end
    end

    def session_context
      @session_context ||= @catalog.session_context(@default_namespace)
    end
  end
end
