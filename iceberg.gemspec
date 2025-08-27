require_relative "lib/iceberg/version"

Gem::Specification.new do |spec|
  spec.name          = "iceberg"
  spec.version       = Iceberg::VERSION
  spec.summary       = "Apache Iceberg for Ruby"
  spec.homepage      = "https://github.com/ankane/iceberg-ruby"
  spec.license       = "Apache-2.0"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{lib}/**/*"]
  spec.require_path  = "lib"
  spec.extensions    = ["ext/iceberg/extconf.rb"]

  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency "rb_sys"
end
