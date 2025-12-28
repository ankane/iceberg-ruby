require "bundler/gem_tasks"
require "rake/testtask"
require "rake/extensiontask"

CATALOGS = %w(glue memory rest sql)

CATALOGS.each do |catalog|
  namespace :test do
    task("env:#{catalog}") { ENV["CATALOG"] = catalog }

    Rake::TestTask.new(catalog => "env:#{catalog}") do |t|
      t.description = "Run tests for #{catalog}"
      t.test_files = FileList["test/**/*_test.rb"]
    end
  end
end

desc "Run all catalog tests"
task :test do
  CATALOGS.each do |catalog|
    next if catalog == "glue"

    Rake::Task["test:#{catalog}"].invoke
  end
end

task default: :test

platforms = [
  "x86_64-linux",
  "x86_64-linux-musl",
  "aarch64-linux",
  "aarch64-linux-musl",
  "x86_64-darwin",
  "arm64-darwin",
  "x64-mingw-ucrt"
]

gemspec = Bundler.load_gemspec("iceberg.gemspec")
Rake::ExtensionTask.new("iceberg", gemspec) do |ext|
  ext.lib_dir = "lib/iceberg"
  ext.cross_compile = true
  ext.cross_platform = platforms
  ext.cross_compiling do |spec|
    spec.dependencies.reject! { |dep| dep.name == "rb_sys" }
    spec.files.reject! { |file| File.fnmatch?("ext/*", file, File::FNM_EXTGLOB) }
  end
end

task :remove_ext do
  path = "lib/iceberg/iceberg.bundle"
  File.unlink(path) if File.exist?(path)
end

Rake::Task["build"].enhance [:remove_ext]
