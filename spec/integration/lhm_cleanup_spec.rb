require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

require 'lhm'

describe Lhm do
  include IntegrationHelper

  before(:each) { connect_master! }

  describe "cleanup" do
    it "should delete users' lhmn table when table_name is passed" do
      table_create(:lhmn_table, true)

      Lhm.cleanup(true, table_name: 'users')

      lhm_tables = @connection.select_values("show tables").select { |name| name =~ /^lhmn_/ && name.ends_with?('users') }
      lhm_tables.must_equal([])
    end

    it "should delete 10_users' lhma table when table_name is passed" do
      table_create(:lhma_table, true)

      Lhm.cleanup(true, table_name: '10_users')

      lhm_tables = @connection.select_values("show tables").select { |name| name =~ /^lhmn_/ && name.ends_with?('10_users') }
      lhm_tables.must_equal([])
    end

    it "should delete triggers on triggers_origin table when table_name is passed" do
      bulk_create(:table_with_lhmt)

      Lhm.cleanup(true, table_name: 'triggers_origin')

      lhm_triggers = @connection.select_values("show triggers").collect do |trigger|
        trigger.respond_to?(:trigger) ? trigger.trigger : trigger
      end.select { |name| name =~ /^lhmt/ && name.ends_with?('triggers_origin') }
      lhm_triggers.must_equal([])
    end
  end
end
