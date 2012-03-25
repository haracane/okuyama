module Okuyama
  class Client
    public
    attr_reader :host, :port, :timeout, :retry_max
    attr_accessor :parse_int_flag
    
    def initialize(options)
      @host = options[:host] || 'localhost'
      @port = options[:port] || 8888
      @timeout = options[:timeout] || 10
      @retry_max = options[:retry_max] || 3
      protocol_version = options[:protocol_version] || '1.0.0'
      protocol_version = '1.0.0'
      case protocol_version
      when '1.0.0'        
        @protocol = Okuyama::Protocol::Version1
      else
        raise OkuyamaError, "protocol version #{protocol_version.inspect} is invalid"
      end
      @parse_int_flag = options[:parse_int_flag]
      @parse_int_flag = true if @parse_int_flag.nil?
    end
    
    def protocol_version
      @protocol.version
    end
    
    def close(raise_exception=nil)
      if @socket then
        begin
          @socket.close
        rescue Exception=>e
          raise e if raise_exception
          Okuyama.logger.error "ERROR: #{e.message}"
        end
      end
      @socket = nil
    end
    
    def send(message, options=nil, &block)
      # Disable debug message for better performance
      # Okuyama.logger.debug "send: #{message}"
      retry_count = 0
      begin
        self.socket.puts(message)
      rescue Exception=>e
        if retry_count < @retry_max then
          Okuyama.logger.error "ERROR: #{e.message}"
          @socket.close
          @socket = nil
          retry_count += 1
          retry
        else
          raise e
        end
      end
      
      if block_given? then
        while line = socket.gets do
          yield(line)
        end
      else
        result = socket.gets
        result.chomp!
        # Disable debug message for better performance
        # Okuyama.logger.debug "recv: #{result.inspect}"
        return result
      end
    end

    def init_count(options=nil)
      message = @protocol.init_count
      result = self.request_parsed_result(message,options)
      result = result.to_i if @parse_int_flag
      return result
    end
    
    def set_value(key, val, *args)
      tag_list = nil
      options = nil
      
      argc = args.length
      if argc == 1 then
        arg = args[0]
        if arg.is_a? Array then
          tag_list = arg
        elsif arg.is_a? Hash then
          options = arg
          tag_list = options[:tags]
        else
          tag_list = [arg.to_s]
        end
      elsif 1 < argc then
        tag_list = args
      end

      # Disable debug message for better performance
      # Okuyama.logger.debug "Okuyama::Client.protocol.set_value(key=#{key.inspect},val=#{val.inspect},tag_list=#{tag_list.inspect})"

      message = @protocol.set_value(key, val, tag_list)
      return self.request_parsed_result(message,options)
    end
    
    def get_value(key, options=nil)
      message = @protocol.get_value(key)
      return self.request_parsed_result(message,options)
    end
    
    def get_tag_keys(tag, *args)
      flag = 'false'
      options = nil
      argc = args.length
      if argc == 1 && args[0].is_a?(Hash) then
          flag = options[:flag]
      elsif 0 < argc then
        flag = args[0]
      end
      message = @protocol.get_tag_keys(tag, flag)
      return self.request_parsed_result(message,options)
    end
    
    def remove_value(key, options=nil)
      message = @protocol.remove_value(key)
      return self.request_parsed_result(message,options)
    end
    
    def set_new_value(key, val, tag_list=nil, options=nil)
      message = @protocol.set_new_value(key, val, tag_list)
      return self.request_parsed_result(message,options)
    end
    
    def get_value_version_check(key, options=nil)
      message = @protocol.get_value_version_check(key)
      return self.request_parsed_result(message,options)
    end
    
    def set_value_version_check(key, val, version, tag_list=nil, options=nil)
      message = @protocol.set_value_versin_check(key, val, version, tag_list)
      return self.request_parsed_result(message,options)
    end
    
    def incr_value(key, val, options=nil)
      message = @protocol.incr_value(key, val)
      result = self.request_parsed_result(message,options)
      result = result.to_i if @parse_int_flag
      return result
    end
    
    def decr_value(key, val, options=nil)
      message = @protocol.decr_value(key, val)
      result = self.request_parsed_result(message,options)
      result = result.to_i if @parse_int_flag
      return result
    end
    
    def get_multi_value(key_list, options=nil)
      message = @protocol.get_multi_value(key_list)
      result = []
      self.send(message) { |line|
        line.chomp!
        result.push self.parse_result(line)
      }
      return result
    end
    
    def get_tag_values(tag, options=nil)
      message = @protocol.get_tag_values(tag)
      result = []
      self.send(message) { |line|
        line.chomp!
        result.push self.parse_result(line)
      }
      return result
    end

    def remove_tag_from_key(tag, key, options=nil)
      message = @protocol.remove_tag_from_key(tag, key)
      return self.request_parsed_result(message)
    end

    def set_value_and_create_index(key, value, options=nil)
      message = @protocol.set_value_and_create_index(key, val, options)
      return self.request_parsed_result(message)
    end

    def search_value(query_list, options=nil)
      if ! query_list.is_a? Array then
        query_list = [query_list.to_s]
      end
      message = @protocol.search_value(query_list, options)
      return self.request_parsed_result(message)
    end

    protected    
    def parse_result(result)
      begin
        result = @protocol.parse_line_result(result)
      rescue Okuyama::ServerError => e
        raise Okuyama::ServerError, "#{e.message}, sent message = #{message.inspect}"
      end
      return result
    end

    def request_parsed_result(message, options=nil)
      result = self.send(message)
      return self.parse_result(result)
    end
    
    def socket
      if @socket.nil? then
        begin
          @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
          sockaddr = Socket.sockaddr_in(@port, @host)
          if @timeout
            secs = Integer(@timeout)
            usecs = Integer((@timeout - secs) * 1_000_000)
            optval = [secs, usecs].pack("l_2")
            @socket.setsockopt Socket::SOL_SOCKET, Socket::SO_RCVTIMEO, optval
            @socket.setsockopt Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, optval
          end
          @socket.connect(sockaddr)
        rescue Exception => e
          @socket = nil
          raise e
        end
      end
      return @socket
    end
  end
end
