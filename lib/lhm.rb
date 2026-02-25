# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/table'
require 'lhm/invoker'
require 'lhm/connection'
require 'lhm/version'

# Large hadron migrator - online schema change tool
#
# @example
#
#   Lhm.change_table(:users) do |m|
#     m.add_column(:arbitrary, "INT(12)")
#     m.add_index([:arbitrary, :created_at])
#     m.ddl("alter table %s add column flag tinyint(1)" % m.name)
#   end
#
module Lhm

  LHMA_SPLIT_REGEX = /lhma_(\d{2,4}_){7}/
  LHMN_SPLIT_REGEX = /lhmn_/
  LHMT_SPLIT_REGEX = /lhmt_(ins|upd|del)_/

  # Alters a table with the changes described in the block
  #
  # @param [String, Symbol] table_name Name of the table
  # @param [Hash] options Optional options to alter the chunk / switch behavior
  # @option options [Fixnum] :stride
  #   Size of a chunk (defaults to: 40,000)
  # @option options [Fixnum] :throttle
  #   Time to wait between chunks in milliseconds (defaults to: 100)
  # @option options [Boolean] :atomic_switch
  #   Use atomic switch to rename tables (defaults to: true)
  #   If using a version of mysql affected by atomic switch bug, LHM forces user
  #   to set this option (see SqlHelper#supports_atomic_switch?)
  # @yield [Migrator] Yielded Migrator object records the changes
  # @return [Boolean] Returns true if the migration finishes
  # @raise [Error] Raises Lhm::Error in case of a error and aborts the migration
  def self.change_table(table_name, options = {}, &block)
    Lhm.cleanup(true, table_name: table_name) if options.fetch(:cleanup, false)
    connection = Connection.new(adapter)

    origin = Table.parse(table_name, connection)
    invoker = Invoker.new(origin, connection)
    block.call(invoker.migrator)
    invoker.run(options)

    true
  ensure
    Lhm.cleanup(true, table_name: table_name, only_triggers: true)
  end

  # Cleans up LHM temporary tables and triggers
  #
  # @param [Boolean] run Actually perform the cleanup (default: false, dry run)
  # @param [Hash] options Optional options to control cleanup behavior
  # @option options [Boolean] :only_triggers
  #   Only cleanup triggers, not tables (default: false)
  # @option options [String, Symbol] :table_name
  #   Only cleanup artifacts for this specific table
  # @option options [Time] :until
  #   Only cleanup tables created before this time
  # @option options [Boolean] :batch_delete
  #   Delete rows in batches before dropping to avoid replica lag (default: false)
  # @option options [Fixnum] :stride
  #   Number of rows to delete per batch when batch_delete is true (default: 10,000)
  # @option options [Fixnum] :throttle
  #   Milliseconds to sleep between batches when batch_delete is true (default: 100)
  def self.cleanup(run = false, options = {})
    only_triggers = options.fetch(:only_triggers, false)
    batch_delete = options.fetch(:batch_delete, false)
    stride = options.fetch(:stride, 10_000)
    throttle = options.fetch(:throttle, 100)

    lhm_tables = only_triggers ? [] : connection.select_values("show tables").select { |name| name =~ /^lhm(a|n)_/ }
    if options[:until]
      lhm_tables.select!{ |table|
        table_date_time = Time.strptime(table, "lhma_%Y_%m_%d_%H_%M_%S")
        table_date_time <= options[:until]
      }
    end

    lhm_triggers = connection.select_values("show triggers").collect do |trigger|
      trigger.respond_to?(:trigger) ? trigger.trigger : trigger
    end.select { |name| name =~ /^lhmt/ }

    if options[:table_name]
      table_name = options[:table_name].to_s
      lhm_tables.select! do |table|
        stripped_table_name = table.starts_with?("lhma") ? table.split(LHMA_SPLIT_REGEX).last : table.split(LHMN_SPLIT_REGEX).last
        stripped_table_name == table_name
      end
      lhm_triggers.select! do |trigger|
        trigger.split(LHMT_SPLIT_REGEX).last == table_name
      end
    end

    if run
      lhm_triggers.each do |trigger|
        connection.execute("drop trigger if exists #{trigger}")
      end
      lhm_tables.each do |table|
        if batch_delete
          chunked_delete(table, stride, throttle)
        end
        connection.execute("drop table if exists #{table}")
      end
      true
    elsif lhm_tables.empty? && lhm_triggers.empty?
      puts "Everything is clean. Nothing to do."
      true
    else
      puts "Existing LHM backup tables: #{lhm_tables.join(", ")}."
      puts "Existing LHM triggers: #{lhm_triggers.join(", ")}."
      puts "Run Lhm.cleanup(true) to drop them all."
      puts "Run Lhm.cleanup(true, batch_delete: true) to delete in batches first (recommended for large tables)."
      false
    end
  end

  # Deletes rows from a table in chunks to avoid replica lag
  #
  # @param [String] table_name Name of the table to delete from
  # @param [Fixnum] stride Number of rows to delete per batch
  # @param [Fixnum] throttle Milliseconds to sleep between batches
  def self.chunked_delete(table_name, stride, throttle)
    puts "Batch deleting from #{table_name} (stride: #{stride}, throttle: #{throttle}ms)..."
    throttle_seconds = throttle / 1000.0
    total_deleted = 0

    loop do
      deleted = connection.execute("DELETE FROM `#{table_name}` LIMIT #{stride}").affected_rows
      break if deleted == 0

      total_deleted += deleted
      print "."
      sleep(throttle_seconds)
    end

    puts "\nDeleted #{total_deleted} rows from #{table_name}"
  end
  private_class_method :chunked_delete

  def self.setup(adapter)
    @@adapter = adapter
  end

  def self.adapter
    @@adapter =
      begin
        raise 'Please call Lhm.setup' unless defined?(ActiveRecord)
        ActiveRecord::Base.connection
      end
  end

  protected

  def self.connection
    Connection.new(adapter)
  end
end
