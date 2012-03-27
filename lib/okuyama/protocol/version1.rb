module Okuyama
  module Protocol
    class Version1 < AbstractProtocol
      attr_accessor :debug
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
      
      [
        [:init_count, 0], [:set_value, 1], [:get_value, 2], [:get_tag_keys, 3],
        [:remove_value, '5'], [:set_new_value, '6'], [:get_value_version_check, '15'], [:set_value_version_check, '16'],
        [:incr_value, 13], [:decr_value, 14], [:get_multi_value, 22], [:get_tag_values, 23], [:remove_tag_from_key, 40],
        [:set_value_and_create_index, 42], [:search_value, 43]
      ].each do |pair|
        name = pair[0]
        opcode = pair[1]
        eval <<-EOF
          def #{name}(socket, *args)
            Okuyama.logger.debug "send: #{"#"}{self.message_of_send_#{name}("#{opcode}", *args).inspect}" if @debug
            self.send_#{name}(socket, "#{opcode}", *args)
          end
        EOF
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
      def send_init_count(socket, opcode)
        socket.puts opcode
      end
      def send_set_value(socket, opcode, key, tag_list, val, version=nil)
        key_base64 = self.encode(key)
        val_base64 = self.encode(val)
        dlock = '0'
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        self.print_tag_list socket, tag_list
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
      
      def send_get_value(socket, opcode, key)
        key_base64 = self.encode(key)
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.puts
      end
      
      def send_get_tag_keys(socket, opcode, tag, flag)
        socket.print opcode
        socket.print ","
        socket.print  self.encode(tag)
        socket.print ","
        socket.print  flag
        socket.puts
      end

      def send_remove_value(socket, opcode, key, dlock='0')
        key_base64 = self.encode(key)
        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        socket.print  dlock
        socket.puts
      end

      alias :send_set_new_value :send_set_value
      alias :send_get_value_version_check :send_get_value
      alias :send_set_value_version_check :send_set_value

      def send_incr_value(socket, opcode, key, val)
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
      
      alias :send_decr_value :send_incr_value
      
      def send_get_multi_value(socket, opcode, key_list)
        keys_base64 = key_list.map{|key|self.encode(key)}.join(',')
        socket.print opcode
        socket.print ","
        socket.print  keys_base64
        socket.puts
      end

      def send_get_tag_values(socket, opcode, tag)
        tag_base64 = self.encode(tag)
        socket.print opcode
        socket.print ","
        socket.print  tag_base64
        socket.puts
      end

      def send_remove_tag_from_key(socket, opcode, tag, key)
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

      def send_set_value_and_create_index(socket, opcode, key, val, tag_list=nil, group=nil, min_index_n='1', max_index_n='3')
        key_base64 = self.encode(key)
        val_base64 = self.encode(val)

        dlock = '0'

        socket.print opcode
        socket.print ","
        socket.print  key_base64
        socket.print ","
        self.print_tag_list socket, tag_list
        socket.print ","
        socket.print  dlock
        socket.print ","
        socket.print  val_base64
        socket.print ","
        self.print_group socket, group
        socket.print ","
        socket.print  min_index_n
        socket.print ","
        socket.print  max_index_n
        socket.puts
      end

      def send_search_value(socket, opcode, query_list, condition='1', group=nil, nsize='3')
        
        socket.print opcode
        socket.print ","
        self.print_query_list(socket, query_list)
        socket.print ","
        socket.print condition
        socket.print ","
        self.print_group socket, group
        socket.print ","
        socket.print  nsize
        socket.puts
      end
      
      def print_group(socket, group)
        if group then
          socket.print self.encode(group)
        else
          socket.print  "(B)"
        end
      end
      
      def print_tag_list(socket, tag_list)
        if tag_list then
          self.print_key_list(socket, tag_list)
        else
          socket.print "(B)"
        end
      end

      def print_key_list(socket, key_list)
        lsize = key_list.size
        k = key_list[0]
        socket.print self.encode(k) if k
        (lsize-1).times do |i|
          socket.print ':'
          socket.print self.encode(key_list[i+1])
        end
      end
      alias :print_query_list :print_key_list

    end
  end
end