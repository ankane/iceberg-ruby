# Iceberg Ruby

[Apache Iceberg](https://iceberg.apache.org/) for Ruby

[![Build Status](https://github.com/ankane/iceberg-ruby/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/iceberg-ruby/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "iceberg"
```

## Getting Started

Create a client for an Iceberg catalog

```ruby
catalog = Iceberg::RestCatalog.new(uri: "http://localhost:8181")
```

Create a namespace

```ruby
catalog.create_namespace("main")
```

Create a table

```ruby
catalog.create_table("main.events") do |t|
  t.bigint "id"
  t.float "value"
end
```

Or with [Polars](https://github.com/ankane/ruby-polars)

```ruby
df = Polars::DataFrame.new({"id" => [1, 2], "value" => [3.0, 4.0]})
table = catalog.create_table("main.events", schema: df.schema)
table.append(df)
```

Load a table

```ruby
table = catalog.load_table("main.events")
```

Query a table

```ruby
table.to_polars.collect
```

## Catalog Types

### REST

```ruby
Iceberg::RestCatalog.new(
  uri: "http://localhost:8181"
)
```

### S3 Tables

[unreleased]

```ruby
Iceberg::S3TablesCatalog.new(
  arn: "arn:aws:s3tables:..."
)
```

### Glue

[unreleased]

```ruby
Iceberg::GlueCatalog.new(
  warehouse: "s3://my-bucket"
)
```

### SQL

```ruby
Iceberg::SqlCatalog.new(
  uri: "postgres://localhost:5432/iceberg",
  warehouse: "s3://my-bucket"
)
```

### Memory

```ruby
Iceberg::MemoryCatalog.new(
  warehouse: "/tmp/warehouse"
)
```

## Namespaces

List namespaces

```ruby
catalog.list_namespaces
```

Create a namespace

```ruby
catalog.create_namespace("main")
```

Check if a namespace exists

```ruby
catalog.namespace_exists?("main")
```

Get the properties of a namespace

```ruby
catalog.namespace_properties("main")
```

Update a namespace

```ruby
catalog.update_namespace("main", properties: {})
```

Drop a namespace

```ruby
catalog.drop_namespace("main")
```

## Tables

List tables

```ruby
catalog.list_tables("main")
```

Create a table

```ruby
catalog.create_table("main.events") do |t|
  t.integer "id"
  t.float "value"
end
```

Load a table

```ruby
catalog.load_table("main.events")
```

Check if a table exists

```ruby
catalog.table_exists?("main.events")
```

Rename a table

```ruby
catalog.rename_table("main.events", "main.events2")
```

Register a table

```ruby
catalog.register_table("main.events", "metadata.json")
```

Drop a table

```ruby
catalog.drop_table("main.events")
```

## Static Tables

Load a static table

```ruby
Iceberg::StaticTable.new("metadata.json")
```

## History

View the [changelog](https://github.com/ankane/iceberg-ruby/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/iceberg-ruby/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/iceberg-ruby/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/iceberg-ruby.git
cd iceberg-ruby
bundle install
bundle exec rake compile

# memory catalog
bundle exec rake test:memory

# REST catalog
docker run -p 8181:8181 apache/iceberg-rest-fixture
bundle exec rake test:rest

# S3 Tables catalog
bundle exec rake test:s3tables

# Glue catalog
bundle exec rake test:glue

# SQL catalog
createdb iceberg_ruby_test
bundle exec rake test:sql
```
