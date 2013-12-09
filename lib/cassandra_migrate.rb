require "cassandra_migrate/version"

require "cql"

module CassandraMigrate
  attr :host
  attr :port
  attr :keyspace
  attr :migration_dir

  def cql_client
    return @cassandra_client if @cassandra_client

    @cassandra_client = Cql::Client.connect(:hosts => host, :port => port)
    @cassandra_client.use(keyspace) if keyspace
  end

  def execute_cql_file(file)
    cql = File.read file

    cql_client.execute cql
  end

  def execute_cql(cql)
    cql_client.execute cql
  end

  def migrations_in_dir(refresh = false)
    return @migrations_in_dir if @migrations_in_dir && !refresh

    @migrations_in_dir = {}
    Dir[migration_dir].each do |file|
      next unless file =~ /^(\d{14})_/

      m = /^(?<date_str>\d{14})_(?<desc>[^.]+)_(?<action>[^_.]+)(?<extensions>\..*)$/.match(file)
      @migrations_in_dir[date_str] ||= {}
      migration = @migrations_in_dir[date_str]
      migration[:actions] ||= {}

      if migration[:desc] && migration[:desc] != desc
        raise "Only one migration name per date string!  #{desc.inspect} != #{migration[:desc].inspect}"
      end
      migration[:desc] = desc

      migration[:actions][action] = {
        file: file
      }
    end

    @migrations_in_dir
  end

  def ensure_schema_keyspace_exists
    ks = execute_cql "SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'schema';"

    if ks.empty?
      peers = execute_cql "SELECT peer FROM system.peers;"

      replication = [3, peers.size + 1].max
      execute_cql <<-MIGRATION
        CREATE KEYSPACE "schema" WITH REPLICATION =
          { 'class' : 'SimpleStrategy', 'replication_factor' : #{replication} };
      MIGRATION
    end


  end

  def migrations_completed(refresh = false)
    return @migrations_completed if @migrations_completed && !refresh

    ensure_schema_keyspace_exists

    @migrations_completed = {}

    # TODO: finish
    @migrations_completed
  end

  def up(date_str, options = {})
  end

  def down(date_str, options = {})
  end

  def up_to(date_str, options = {})
  end

  def down_to(date_str, options = {})
  end

  def go_to_target(date_str, options = {})
  end

end
