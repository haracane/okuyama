module Okuyama
  module Protocol
    class Version1
      def self.init_count
        return '0'
      end
      
      def self.version
        return '1.0.0'
      end
      
      def self.set_value(key, val, tag_list=nil)
        return self.send_key_tags_value_message('1', key, tag_list, val)
      end
      
      def self.get_value(key)
        return self.send_key_message('2', key)
      end

      def self.get_tag_keys(tag, flag)
        return self.send_tag_flag_message('3', tag, flag)
      end
  
      def self.remove_value(key)
        return self.send_key_message('5', key)
      end
  
      def self.set_new_value(key, val, tag_list=nil)
        return self.send_key_tags_value_message('6', key, tag_list, val)
      end
      
      def self.get_value_version_check(key)
        return self.send_key_message('15', key)
      end
      
      def self.set_value_version_check(key, val, version, tag_list=nil)
        return self.send_key_tags_value_message('16', key, tag_list, val, version)
      end
  
      def self.incr_value(key, val)
        return self.send_key_value_message('13', key, val)
      end
      
      def self.decr_value(key, val)
        return self.send_key_value_message('14', key, val)
      end
      
      def self.get_multi_value(key_list)
        return self.send_key_message('22', key)
      end
      
      def self.get_tag_values(tag)
        return self.send_tag_message('23', tag)
      end
      
      def self.remove_tag_from_key(tag, key)
        return self.send_tag_key_message('40', tag, key)
      end

      def self.set_value_and_create_index(key, val, options=nil)
        tag_list = nil
        group = nil
        min_index_n = 1
        max_index_n = 3
        if options then
          tag_list = options[:tags] || tag_list
          group = options[:group] || group
          min_index_n = options[:min_index_n] || min_index_n
          max_index_n = options[:max_index_n] || max_index_n
        end
        return self.send_key_tags_value_group_index_message('42', key, tag_list, val, group, min_index_n, max_index_n)
      end
      
      def self.search_value(query_list, options=nil)
        condition = :and
        group = nil
        nsize = 3
        if options then
          tag_list = options[:condition] || condition
          group = options[:group] || group
          min_index_n = options[:nsize] || nsize
        end

        case condition
        when :and
          condition = 1
        else :or
          condition = 2
        end
        return self.send_key_condition_group_index_message('43', query_list, condition, group, nsize)
      end

      def self.parse_line_result(result)
        record = result.split(/,/)
        
        opcode = record.shift
        exit_code = record.shift
        
        case exit_code
        when 'false'
          return nil
        when 'error'
          raise Okuyama::ServerError, record[0]
        end
        
        case opcode
        when '0'   # init_count
          return record[0]
        when '2'   # get_value
          ret = Base64.decode64(record[0])
          return ret
        when '4'   # get_tag_keys
          return record[0].split(/:/).map{|b|Base64.decode64(b)}
        when '13'  # incr_value
          return Base64.decode64(record[0])
        when '14'  # decr_value
          return Base64.decode64(record[0])
        when '15'  # get_value_version_check
          record[0] = Base64.decode64(record[0])
          return record
        when '22'  # get_multi_value
          return Base64.decode64(record[0])
        when '23'  # get_tag_values
          record[0] = Base64.decode64(record[0])
          record[1] = Base64.decode64(record[1])
          return record
        when '43'   # search_value
          return record[0].split(/:/).map{|b|Base64.decode64(b)}
        else
          return true
        end
        return record
      end

      private
      def self.send_key_message(opcode, key)
        key_base64 = Base64.encode64(key).chomp
        return "#{opcode},#{key_base64}"
      end

      def self.send_keys_message(opcode, key_list)
        keys_base64 = key_list.map{|key|Base64.encode64(key).chomp}.join(',')
        return "#{opcode},#{keys_base64}"
      end

      def self.send_key_value_message(opcode, key, val)
        key_base64 = Base64.encode64(key).chomp
        val_base64 = Base64.encode64(val).chomp
        dlock = 0
        return "#{opcode},#{key_base64},#{dlock},#{val_base64}"
      end
      
      def self.send_key_tags_value_message(opcode, key, tag_list, val, version=nil)
        key_base64 = Base64.encode64(key).chomp
        val_base64 = Base64.encode64(val).chomp
        tags = nil
        if tag_list then
          tags = tag_list.map{|t|Base64.encode64(t).chomp}.join(':')
        end
        tags ||= "(B)"
        dlock = 0
        if version then
          return "#{opcode},#{key_base64},#{tags},#{dlock},#{val_base64},#{version}"
        else
          return "#{opcode},#{key_base64},#{tags},#{dlock},#{val_base64}"
        end
      end
      
      def self.send_key_tags_value_group_index_message(opcode, key, tag_list, val, group, min_index_n, max_index_n)
        key_base64 = Base64.encode64(key).chomp
        val_base64 = Base64.encode64(val).chomp

        tags = nil
        if tag_list then
          tags = tag_list.map{|t|Base64.encode64(t).chomp}.join(':')
        end
        tags ||= "(B)"

        dlock = 0

        group_base64 = nil
        if group then
          group_base64 = Base64.encode64(group).chomp
        end
        group_base64 ||= "(B)"

        return "#{opcode},#{key_base64},#{tags},#{dlock},#{val_base64},#{group_base64},#{min_index_n},#{max_index_n}"
      end

      def self.send_tag_message(opcode, tag)
        tag_base64 = Base64.encode64(tag).chomp
        return "#{opcode},#{tag_base64}"
      end

      def self.send_tag_flag_message(opcode, tag, flag)
        tag_base64 = Base64.encode64(tag).chomp
        return "#{opcode},#{tag_base64},#{flag}"
      end

      def self.send_tag_key_message(opcode, key, val)
        tag_base64 = Base64.encode64(tag).chomp
        key_base64 = Base64.encode64(key).chomp
        dlock = 0
        return "#{opcode},#{tag_base64},#{key_base64},#{dlock}"
      end

      def self.send_key_condition_group_index_message(opcode, query_list, condition, group, nsize)
        queries = query_list.map{|q|Base64.encode64(q).chomp}.join(':')
        
        group_base64 = nil
        if group then
          group_base64 = Base64.encode64(group).chomp
        end
        group_base64 ||= "(B)"
        return "#{opcode},#{queries},#{condition},#{group_base64},#{nsize}"
      end

    end
  end
end