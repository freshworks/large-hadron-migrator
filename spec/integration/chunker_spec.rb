# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

require 'lhm'
require 'lhm/table'
require 'lhm/migration'

describe Lhm::Chunker do
  include IntegrationHelper

  before(:each) { connect_master! }

  describe "copying" do
    before(:each) do
      @origin = table_create(:origin)
      @destination = table_create(:destination)
      @migration = Lhm::Migration.new(@origin, @destination)
    end

    it "should copy 23 rows from origin to destination" do
      23.times { |n| execute("insert into origin set id = '#{ n * n + 23 }'") }

      Lhm::Chunker.new(@migration, connection, { :stride => 100 }).run

      slave do
        count_all(@destination.name).must_equal(23)
      end
    end
  end

  describe "Batch copy test" do
    before(:each) do
      @batch_origin = table_create(:batch_origin)
      @batch_destination = table_create(:batch_destination)
      @batch_migration = Lhm::Migration.new(@batch_origin, @batch_destination)
    end

    it "should copy 100 rows from batch_origin to batch_destination" do
      100.times { |n| execute("insert into batch_origin set id = '#{ n * n + 100 }'") }

      Lhm::Chunker.new(@batch_migration, connection, { :stride => 40, :batch_mode => true }).run

      slave do
        count_all(@batch_destination.name).must_equal(100)
      end
    end
  end

  describe "when table has 0 rows" do
    it "should add an index" do
      execute("delete from batch_origin")
      count_all(:batch_origin).must_equal(0)
      index_present = index_on_columns?(:batch_origin, [:id, :origin])
      index_present.must_equal(false)

      Lhm.change_table(:batch_origin, :atomic_switch => false) do |t|
        t.add_index([:id, :origin])
      end

      index_present = index_on_columns?(:batch_origin, [:id, :origin])
      index_present.must_equal(true)
    end

    it "should add a column" do
      execute("delete from batch_origin")
      count_all(:batch_origin).must_equal(0)
      column_present = table_read(:batch_origin).columns.keys.include?('sub_batch_id')
      column_present.must_equal(false)

      Lhm.change_table(:batch_origin, :atomic_switch => false) do |t|
        t.add_column :sub_batch_id, "TINYINT(4)"
      end

      table_read(:batch_origin).columns.keys.must_include('sub_batch_id')
    end
  end
end
