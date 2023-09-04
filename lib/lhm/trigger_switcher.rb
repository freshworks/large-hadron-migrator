module Lhm
  class TriggerSwitcher
    attr_reader :source_table, :destination_table_name, :connection

    def initialize(source_table, destination_table_name, connection)
      @source_table = source_table
      @destination_table_name = destination_table_name
      @connection = connection
    end

    def copy_triggers
      return unless !destination_table_name.empty? && connection.table_exists?(destination_table_name)

      source_table.triggers.each do |trigger|
        trigger_name = trigger[0]
        trigger_definition = fetch_trigger_definition(trigger_name, connection)
        # since source table is renamed to lhmn_table_name, triggers associated with the source table will now be associated with lhmn_table_name.
        # skip if the trigger isn't associated with lhmn_table_name.
        next unless trigger_definition.include? destination_table_name

        modified_definition = trigger_definition.gsub(destination_table_name, source_table.name)
        connection.execute("DROP TRIGGER #{trigger_name};")
        connection.execute(modified_definition)
      end
    end

    def fetch_trigger_definition(trigger_name, connection)
      result = connection.execute("SHOW CREATE TRIGGER #{trigger_name};").first
      # ["update_message_old_5", "NO_ENGINE_SUBSTITUTION", "CREATE DEFINER=`root`@`localhost` TRIGGER update_message_old_5 BEFORE INSERT ON `lhma_2023_09_04_14_32_54_810_logs_table` FOR EACH ROW\n    BEGIN\n      IF NEW.message = '1' THEN\n        SET NEW.message = 'true';\n      END IF;\n    END", "utf8", "utf8_general_ci", "utf8_unicode_ci", 2023-09-04 14:32:43 UTC]
      # create trigger definition will be stored in the third index in the result, ideally result will always be of length 7 if its executed successfully.
      result && result.length > 2 ? result[2] : ''
    end
  end
end
