# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/sql_helper'

module Lhm
  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    # Copy from origin to destination in chunks of size `stride`. Sleeps for
    # `throttle` milliseconds between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @stride = options[:stride] || 40_000
      @throttle = options[:throttle] || 100
      @start = options[:start] || select_start
      @limit = options[:limit] || select_limit
      @batch_mode = options[:batch_mode] || false
    end

    # Copies chunks of size `stride`, starting from `start` up to id `limit`.
    def up_to(&block)
      1.upto(traversable_chunks_size) do |n|
        yield(bottom(n), top(n))
      end
    end

    def traversable_chunks_size
      @limit && @start ? ((@limit - @start + 1) / @stride.to_f).ceil : 0
    end

    def bottom(chunk)
      (chunk - 1) * @stride + @start
    end

    def top(chunk)
      [chunk * @stride + @start - 1, @limit].min
    end

    def copy(lowest, highest)
      "insert ignore into `#{ destination_name }` (#{ columns }) " +
      "select #{ columns } from `#{ origin_name }` " +
      "where `#{ origin_primary_key }` between #{ lowest } and #{ highest }"
    end

    def copy_batchwise(select_clause)
      "insert ignore into `#{ destination_name }` (#{ columns }) " + 
      "#{select_clause}"
    end

    def select_start
      start = connection.select_value("select min(#{origin_primary_key}) from #{ origin_name }")
      start ? start.to_i : nil
    end

    def select_limit
      limit = connection.select_value("select max(#{origin_primary_key}) from #{ origin_name }")
      limit ? limit.to_i : nil
    end

    def throttle_seconds
      @throttle / 1000.0
    end

  private

    def destination_name
      @migration.destination.name
    end

    def origin_name
      @migration.origin.name
    end

    def origin_primary_key
      @migration.origin.pk
    end

    def columns
      @columns ||= @migration.intersection.joined
    end

    def validate
      if @start && @limit && @start > @limit
        error("impossible chunk options (limit must be greater than start)")
      end
    end

    def execute
      @batch_mode ? execute_batch_mode : execute_legacy_mode
    end

    def execute_batch_mode
      start_value = @start - 1
      records = @connection.select_all(select_query(start_value))
      while records.any?
        records_size = @connection.update(copy_batchwise(select_query(start_value)))
        break if records_size.zero?
        start_value = @connection.select_last(select_query(start_value))["#{origin_primary_key}"]
      end
    end

    def select_query(offset)
      "select #{ columns } from `#{ origin_name }` where `#{origin_primary_key}` > #{offset} order by #{origin_primary_key} asc limit #{@stride}"
    end

    def execute_legacy_mode
      up_to do |lowest, highest|
        affected_rows = @connection.update(copy(lowest, highest))

        if affected_rows > 0
          sleep(throttle_seconds)
        end

        print "."
      end
      print "\n"
    end
  end
end
