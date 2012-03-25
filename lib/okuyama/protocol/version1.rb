module Okuyama
  module Protocol
    class Version1 < AbstractProtocol
      def initialize(options=nil)
        if options then
          @base64_encode_flag = options[:base64_encode_flag]
        end
        @base64_encode_flag = true if @base64_encode_flag.nil?
      end
            
      def version
        return '1.0.0'
      end
      
      def encode(text)
        return text if ! @base64_encode_flag
        return Base64.encode64(text).chomp
      end
      
      def decode(text)
        return text if ! @base64_encode_flag
        return Base64.decode64(text)
      end
      
      def init_count(socket)
        socket.puts '0'
      end
      
      def set_value(socket, key, val, tag_list=nil)
        return self.send_key_tags_value_message(socket, '1', key, tag_list, val)
      end
      
      def get_value(socket, key)
        return self.send_key_message(socket, '2', key)
      end

      def get_tag_keys(socket, tag, flag)
        return self.send_tag_flag_message(socket, '3', tag, flag)
      end
  
      def remove_value(socket, key)
        return self.send_key_dlock_message(socket, '5', key, '0')
      end
  
      def set_new_value(socket, key, val, tag_list=nil)
        return self.send_key_tags_value_message(socket, '6', key, tag_list, val)
      end
      
      def get_value_version_check(socket, key)
        return self.send_key_message(socket, '15', key)
      end
      
      def set_value_version_check(socket, key, val, version, tag_list=nil)
        return self.send_key_tags_value_message(socket, '16', key, tag_list, val, version)
      end
  
      def incr_value(socket, key, val)
        return self.send_key_value_message(socket, '13', key, val)
      end
      
      def decr_value(socket, key, val)
        return self.send_key_value_message(socket, '14', key, val)
      end
      
      def get_multi_value(socket, key_list)
        return self.send_keys_message(socket, '22', key_list)
      end
      
      def get_tag_values(socket, tag)
        return self.send_tag_message(socket, '23', tag)
      end
      
      def remove_tag_from_key(socket, tag, key)
        return self.send_tag_key_message(socket, '40', tag, key)
      end

      def set_value_and_create_index(socket, key, val, tag_list=nil, group=nil, min_index_n='1', max_index_n='3')
        return self.send_key_tags_value_group_index_message(socket, '42', key, tag_list, val, group, min_index_n, max_index_n)
      end
      
      def search_value(socket, query_list, condition='1', group=nil, nsize='3')
        return self.send_query_condition_group_index_message(socket, '43', query_list, condition, group, nsize)
      end

      def parse_line_result(result, to_i_flag=nil)
        record = result.split(/,/)
        
        opcode = record.shift
        exit_code = record.shift
        
        case exit_code
        when 'false'
          case opcode
          when '4'
            return []
          when '43'
            return []
          else
            return nil
          end
        when 'error'
          raise Okuyama::ServerError, record[0]
        end
        
        case opcode
        when '0'   # init_count
          result = record[0]
          result = result.to_i if to_i_flag
          return result
        when '2'   # get_value
          ret = record[0]
          ret = self.decode(record[0])
          return ret
        when '4'   # get_tag_keys
          return record[0].split(/:/).map{|b|self.decode(b)}
        when '13'  # incr_value
          if to_i_flag then
            result = Base64.decode64(record[0]).chomp
            result = result.to_i
          else
            result = self.decode(record[0])
          end
          return result
        when '14'  # decr_value
          if to_i_flag then
            result = Base64.decode64(record[0]).chomp
            result = result.to_i
          else
            result = self.decode(record[0])
          end
          return result
        when '15'  # get_value_version_check
          record[0] = self.decode(record[0])
          return record
        when '22'  # get_multi_value
          return self.decode(record[0])
        when '23'  # get_tag_values
          record[0] = self.decode(record[0])
          record[1] = self.decode(record[1])
          return record
        when '43'   # search_value
          return record[0].split(/:/).map{|b|self.decode(b)}
        else
          return true
        end
        return record
      end

      protected
      def send_key_message(socket, opcode, key)
        key_base64 = self.encode(key)
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.puts
      end

      def send_key_dlock_message(socket, opcode, key, dlock)
        key_base64 = self.encode(key)
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        socket.print  dlock
        socket.puts
      end

      def send_keys_message(socket, opcode, key_list)
        keys_base64 = key_list.map{|key|self.encode(key)}.join(',')
        socket.print opcode
        socket.print ","
        socket.print  keys_base64
        socket.puts
     end

      def send_key_value_message(socket, opcode, key, val)
        key_base64 = self.encode(key)
        val_base64 = self.encode(val)
        dlock = '0'
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        socket.print  dlock
        socket.print ","
        socket.print  val_base64
        socket.puts
      end
      
      def send_key_tags_value_message(socket, opcode, key, tag_list, val, version=nil)
        key_base64 = self.encode(key)
        val_base64 = self.encode(val)
        tags = nil
        if tag_list then
          tags = tag_list.map{|t|self.encode(t)}.join(':')
        end
        tags ||= "(B)"
        dlock = '0'
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        socket.print  tags
        socket.print ","
        socket.print  dlock
        socket.print ","
        socket.print  val_base64

        if version then
          socket.print ","
          socket.print  version
        end
        socket.puts
      end
      
      def send_key_tags_value_group_index_message(socket, opcode, key, tag_list, val, group, min_index_n, max_index_n)
        key_base64 = self.encode(key)
        val_base64 = self.encode(val)

        tags = nil
        if tag_list then
          tags = tag_list.map{|t|self.encode(t)}.join(':')
        end
        tags ||= "(B)"

        dlock = '0'

        group_base64 = nil
        if group then
          group_base64 = self.encode(group)
        end
        group_base64 ||= "(B)"

        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        socket.print  tags
        socket.print ","
        socket.print  dlock
        socket.print ","
        socket.print  val_base64
        socket.print ","
        socket.print  group_base64
        socket.print ","
        socket.print  min_index_n
        socket.print ","
        socket.print  max_index_n
        socket.puts
      end

      def send_tag_message(socket, opcode, tag)
        tag_base64 = self.encode(tag)
        socket.print opcode
        socket.print ","
        socket.print  tag_base64
        socket.puts
      end

      def send_tag_flag_message(socket, opcode, tag, flag)
        tag_base64 = self.encode(tag)
        socket.print opcode
        socket.print ","
        socket.print  tag_base64
        socket.print ","
        socket.print  flag
        socket.puts
      end

      def send_tag_key_message(socket, opcode, tag, key)
        tag_base64 = self.encode(tag)
        key_base64 = self.encode(key)
        dlock = '0'
        socket.print opcode
        socket.print ","
        socket.print  tag_base64
        socket.print ","
        socket.print  key_base64
        socket.print ","
        socket.print  dlock
        socket.puts
      end

      def send_query_condition_group_index_message(socket, opcode, query_list, condition, group, nsize)
        queries = query_list.map{|q|self.encode(q)}.join(':')
        
        group_base64 = nil
        if group then
          group_base64 = self.encode(group)
        end
        group_base64 ||= "(B)"
        socket.print opcode
        socket.print ","
        socket.print  queries
        socket.print ","
        socket.print  condition
        socket.print ","
        socket.print  group_base64
        socket.print ","
        socket.print  nsize
        socket.puts
      end

    end
  end
end