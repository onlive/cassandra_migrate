# Cassandra Migrate

This gem is designed to allow Cassandra migrations in very roughly the
style of Rails migrations, but with a few extra features...  And
without the Rails-style DSL for running the migrations.  CQL is
basically fine.  But we *do* want Erb!

## Installation

Add this line to your application's Gemfile:

    gem 'cassandra_migrate'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install cassandra_migrate

You don't have to use cassandra_migrate as part of an app, but you'll
need a recent (1.9.2+) Ruby with Rubygems.

## Usage

You'll need a directory for Cassandra migrations.  Every migration
should have a filename of the form:

20131023000000_create_keyspace_argus_up.cql.erb

The first fourteen digits are the date in YYYYMMDD format, followed
by six digits of your choice -- base them on time, or just make sure
they don't conflict.  You can't have two migrations with exactly the
same fourteen-digit time code!

Then you can use a freeform description, like "create_keyspace_argus"
above.  Then an action, like "up" or "down" (later, scripts as well),
and one or more extensions to tell Cassandra Migrate how to use the
file.  Most commonly you'll want .cql or .cql.erb as the extension.
Such files will be run through Cassandra, optionally after Erubis
processing.

Here's an example that uses Erb, in this case to set the replication
factor of the keyspace via an environment variable:

~~~
# 20131024001100_create_keyspace_cryptic_up.cql.erb
CREATE KEYSPACE "cryptic" WITH REPLICATION =
  { 'class' : 'SimpleStrategy', 'replication_factor' : <%= ENV['CASS_REPLICATION'] || 1 %> };
~~~

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Future Directions

A cassandra_migrate_rails gem could add generators for simple
Cassandra migrations from Rails.

We could add Ruby, bash or other scripts/executables to be run before
and after migrations, or as the migration itself.
