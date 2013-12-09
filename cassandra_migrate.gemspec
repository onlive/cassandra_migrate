# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassandra_migrate/version'

Gem::Specification.new do |spec|
  spec.name          = "cassandra_migrate"
  spec.version       = CassandraMigrate::VERSION
  spec.authors       = ["Noah Gibbs"]
  spec.email         = ["noah.gibbs@onlive.com"]
  spec.description   = %q{TODO: Write a gem description}
  spec.summary       = %q{TODO: Write a gem summary}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_runtime_dependency "trollop"
  spec.add_runtime_dependency "cql-rb"
  spec.add_runtime_dependency "erubis"
end
