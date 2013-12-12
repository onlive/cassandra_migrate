# Copyright (C) 2013 OL2, inc.  See LICENSE.txt for details.

require "cassandra_migrate/version"

require "cql"
require "erubis"

require "digest/sha1"

class CassandraMigrate
  attr_accessor :host
  attr_accessor :port
  attr_accessor :migration_dir

  private

  def cql_client
    return @cassandra_client if @cassandra_client

    STDERR.puts "Connecting to Cassandra: #{host.inspect} / #{port.inspect}"
    @cassandra_client = Cql::Client.connect(hosts: [host].flatten, port: port, consistency: :quorum)

    @cassandra_client
  end

  def execute_cql(cql, options = {})
    if options[:dry_run]
      puts "Dry run, execute: #{cql}"
      return
    end

    last_result = nil
    # Can only execute single chunks at once
    cql.split(";").map(&:strip).select {|s| s != ""}.each do |statement|
      # Prep-then-execute so that a syntax error will be detectable as such
      last_result = cql_client.execute statement
      puts "Executing CQL: #{statement}"
    end

    last_result
  end

  def migrations_in_dir(refresh = false)
    return @migrations_in_dir if @migrations_in_dir && !refresh

    @migrations_in_dir = {}
    Dir[File.join migration_dir, "*"].each do |file|
      unless File.basename(file) =~ /^(\d{14})_/
        puts "No match: #{file.inspect}"
        next
      end

      unless /^(?<date_str>\d{14})_(?<desc>[^.]+)_(?<action>[^_.]+)(?<extensions>\..*)$/ =~ File.basename(file)
        puts "No match with regexp: #{file.inspect}"
        next
      end

      @migrations_in_dir[date_str] ||= {}
      migration = @migrations_in_dir[date_str]
      migration[:actions] ||= {}

      if migration[:desc] && migration[:desc] != desc
        raise "Only one migration name per date string!  #{desc.inspect} != #{migration[:desc].inspect}"
      end
      migration[:desc] = desc

      migration[:actions][action.to_sym] = {
        file: file
      }
    end

    raise "No migrations in directory #{migration_dir.inspect}!  Did you mean to specify a migration directory?" if @migrations_in_dir.empty?

    @migrations_in_dir
  end

  def ensure_schema_keyspace_exists(options = {})
    ks = execute_cql "SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'schema';"

    if ks.empty?
      raise "No schema keyspace in a dry run!" if options[:dry_run]

      peers = execute_cql "SELECT peer FROM system.peers;"

      @replication = [3, peers.to_a.size + 1].min
      execute_cql <<-MIGRATION, options
        CREATE KEYSPACE "schema" WITH REPLICATION =
          { 'class' : 'SimpleStrategy', 'replication_factor' : #{@replication} };
      MIGRATION
    end

    cf = execute_cql "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE columnfamily_name = 'migrations' AND keyspace_name = 'schema';"
    if cf.empty?
      raise "No migration table in a dry run!" if options[:dry_run]

      execute_cql <<-MIGRATION
        CREATE TABLE "schema"."migrations" (
          "date_string" varchar,
          "up_filename" varchar,
          "sha1" varchar,
        PRIMARY KEY ("date_string", "up_filename"));
      MIGRATION
    end
  end

  def migrations_completed(refresh = false, options = {})
    return @migrations_completed if @migrations_completed && !refresh

    ensure_schema_keyspace_exists(options)

    @migrations_completed = {}

    migrations = execute_cql 'SELECT * FROM "schema"."migrations";'
    migrations.each do |migration|
      @migrations_completed[migration["date_string"]] = migration.to_hash
    end

    @migrations_completed
  end

  def sha1(path)
    Digest::SHA1.hexdigest File.read path
  end

  def execute_migration_file(path, options)
    ensure_schema_keyspace_exists(options)

    STDERR.puts "Executing migration file: #{path.inspect}"

    components = File.basename(path).split(".")
    components.shift   # Take just the extensions

    content = File.read path

    while components.size > 1
      ext = components.pop

      if ext == "erb" || ext == "erubis"
        eruby = Erubis::Eruby.new content
        content = eruby.result :replication => @replication
      else
        raise "Unknown intermediate extension in path #{path.inspect}: #{ext.inspect}!"
      end
    end

    final_type = components.first
    if ["cql", "cqlsh"].include?(final_type)
      execute_cql content, options
    elsif ["erb", "erubis"].include?(final_type)
      raise "Can't use erb as the final extension in path #{path.inspect}!"
    else
      raise "Unknown extension #{final_type.inspect} in path #{path.inspect}!"
    end
  end

  public

  def up(date_str, options = {})
    raise "Can't apply migration #{date_str} that already happened!" if migrations_completed(false,options)[date_str]
    raise "Can't apply migration #{date_str} that has no migration files!" unless migrations_in_dir[date_str]
    raise "Can't apply migration #{date_str} with no up migration!" unless migrations_in_dir[date_str][:actions][:up]

    up_filename = migrations_in_dir[date_str][:actions][:up][:file]
    execute_migration_file up_filename, options
    execute_cql "INSERT INTO \"schema\".\"migrations\" (date_string, up_filename, sha1) VALUES ('#{date_str}', '#{up_filename}', '#{sha1 up_filename}')", options
  end

  def down(date_str, options = {})
    raise "Can't reverse migration #{date_str} that didn't happen!" unless migrations_completed(false,options)[date_str]
    raise "Can't reverse migration #{date_str} that has no migration files!" unless migrations_in_dir[date_str]
    raise "Can't reverse migration #{date_str} with no down migration!" unless migrations_in_dir[date_str][:actions][:down]

    execute_migration_file migrations_in_dir[date_str][:actions][:down][:file], options
    execute_cql "DELETE FROM \"schema\".\"migrations\" WHERE date_string = '#{date_str}';", options
  end

  def up_to(date_str, options = {})
    uncompleted_dates = migrations_in_dir.keys - migrations_completed(false,options).keys

    STDERR.puts "Uncompleted: #{uncompleted_dates.inspect}"
    migrations_to_run = uncompleted_dates.select { |d| d <= date_str }

    STDERR.puts "Run #{migrations_to_run.size} migrations, update to #{date_str}."
    migrations_to_run.each { |m| up(m, options) }
  end

  def down_to(date_str, options = {})
    migrations_to_run = migrations_completed(false,options).keys.select { |d| d > date_str }

    STDERR.puts "Run #{migrations_to_run.size} migrations, roll back to #{date_str}."
    migrations_to_run.each { |m| down(m, options) }
  end

  def current_latest(options = {})
    migrations_completed(false,options).keys.max
  end

  def latest_in_directory
    migrations_in_dir.keys.max
  end

  def to_latest(options = {})
    latest = latest_in_directory
    raise "No latest migration!" unless latest
    up_to latest, options
  end

  def to_target(date_str, options = {})
    if date_str < current_latest(options)
      down_to date_str, options
    else
      up_to date_str, options
    end
  end

  def rollback(options = {})
    down(current_latest(options), options)
  end

end
