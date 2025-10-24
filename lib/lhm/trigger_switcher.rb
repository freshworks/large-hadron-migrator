module Lhm
  class TriggerSwitcher
    def copy_triggers(source_table, destination_table_name, connection)
      return unless !destination_table_name.empty? && connection.table_exists?(destination_table_name)

      source_table.triggers.each do |trigger|
        trigger_name = trigger[0]
        trigger_definition = fetch_trigger_definition(trigger_name, connection)
        # since source table is renamed to lhma_table_name(destination table name) after migration, triggers associated with the source table will now be associated with lhma_table_name.
        # skip if the trigger isn't associated with lhma_table_name.
        next unless trigger_definition.include? destination_table_name

        old_trigger_name = trigger_name
        trigger_name = remove_timestamp_from_trigger_name_if_exists(trigger_name)
        modified_trigger_name = trigger_name + '_' + Time.now.getutc.to_s.gsub(/[:\- ]/, '_') #sample trigger name - trigger_name_2023_12_18_03_40_45_UTC
        modified_definition = trigger_definition.gsub(destination_table_name, source_table.name)
        modified_definition = modified_definition.gsub(old_trigger_name, modified_trigger_name)
        connection.execute(modified_definition)
        connection.execute("DROP TRIGGER #{old_trigger_name};")
      end
    end

    def fetch_trigger_definition(trigger_name, connection)
      result = connection.execute("SHOW CREATE TRIGGER #{trigger_name};").first
      # sample result => ["update_message_old_5", "NO_ENGINE_SUBSTITUTION", "CREATE DEFINER=`root`@`localhost` TRIGGER update_message_old_5 BEFORE INSERT ON `lhma_2023_09_04_14_32_54_810_logs_table` FOR EACH ROW\n BEGIN\n IF NEW.message = '1' THEN\n SET NEW.message = 'true';\n END IF;\n END", "utf8", "utf8_general_ci", "utf8_unicode_ci", 2023-09-04 14:32:43 UTC]
      # create trigger definition will be stored in the third index in the result, ideally result will always be of length 7 if its executed successfully.
      trigger_definition = result && result.length > 2 ? result[2] : ''

      # Replace definer if LHM_TRIGGER_DEFINER environment variable is set
      trigger_definition = update_trigger_definer(trigger_definition) if ENV['LHM_TRIGGER_DEFINER'] && !trigger_definition.empty?
      trigger_definition
    end

    def update_trigger_definer(trigger_definition)
      # Match DEFINER=`user`@ pattern and replace only the user part, keeping the original host
      trigger_definition.gsub(/DEFINER=`[^`]+`@/, "DEFINER=`#{ENV['LHM_TRIGGER_DEFINER']}`@")
    end

    def remove_timestamp_from_trigger_name_if_exists(trigger_name)
      timestamp_suffix_regex = /_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}_UTC)\z/
      match_data = trigger_name.match(/^(.*?)(?:#{timestamp_suffix_regex})$/)
      match_data ? match_data[1] : trigger_name
    end
  end
end
