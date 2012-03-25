module Okuyama
  class FastClient
    public
    attr_reader :host, :port, :timeout, :retry_max
    attr_accessor :base64_encode_flag
    
    def initialize(options)
      @host = options[:host] || 'localhost'
      @port = options[:port] || 8888
      @timeout = options[:timeout] || 10
      @retry_max = options[:retry_max] || 3
      protocol_version = options[:protocol_version] || '1.0.0'
      protocol_version = '1.0.0'
      case protocol_version
      when '1.0.0'        
        @protocol = Okuyama::Protocol::Version1.new(:base64_encode_flag=>options[:base64_encode_flag])
      else
        raise OkuyamaError, "protocol version #{protocol_version.inspect} is invalid"
      end
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
    
    def init_count
      message = @protocol.init_count
      return self.request(message)
    end
    
    def set_value(key, val, tag_list=nil)
      message = @protocol.set_value(key, val, tag_list)
      return self.request(message)
    end
    
    def get_value(key)
      message = @protocol.get_value(key)
      return self.request(message)
    end
    
    def get_tag_keys(tag, flag='false')
      message = @protocol.get_tag_keys(tag, flag)
      return self.request(message)
    end
    
    def remove_value(key)
      message = @protocol.remove_value(key)
      return self.request(message)
    end
    
    def set_new_value(key, val, tag_list=nil)
      message = @protocol.set_new_value(key, val, tag_list)
      return self.request(message)
    end
    
    def get_value_version_check(key)
      message = @protocol.get_value_version_check(key)
      return self.request(message)
    end
    
    def set_value_version_check(key, val, version, tag_list=nil)
      message = @protocol.set_value_versin_check(key, val, version, tag_list)
      return self.request(message)
    end
    
    def incr_value(key, val)
      message = @protocol.incr_value(key, val)
      return self.request(message)
    end
    
    def decr_value(key, val)
      message = @protocol.decr_value(key, val)
      return self.request(message)
    end
    
    def get_multi_value(key_list, &block)
      message = @protocol.get_multi_value(key_list)
      return self.request_lines(message, &block)
    end
    
    def get_tag_values(tag, &block)
      message = @protocol.get_tag_values(tag)
      return self.request_lines(message, &block)
    end

    def remove_tag_from_key(tag, key)
      message = @protocol.remove_tag_from_key(tag, key)
      return self.request(message)
    end

    def set_value_and_create_index(key, value, tag_list=nil, group=nil, min_index_n=nil, max_index_n=nil)
      message = @protocol.set_value_and_create_index(key, val, tag_list, group, min_n, max_n)
      return self.request(message)
    end

    def set_value_and_create_index(key, value, options=nil)
      if options then
        tag_list = options[:tags]
        group = options[:group]
        min_n = options[:min_n]
        max_n = options[:max_n]
      end
      return self.set_value_and_create_index(key, val, tag_list, group, min_n, max_n)
    end

    def search_value(query_list, condition=nil, group=nil, nsize=nil)
      message = @protocol.search_value(query_list, condition, group, nsize)
      return self.request(message)
    end

    def search_value(query_list, options=nil)
      if ! query_list.is_a? Array then
        query_list = [query_list.to_s]
      end
      
      if options then
        tag_list = options[:condition]
        group = options[:group]
        min_index_n = options[:nsize]
      end

      case condition
      when :and
        condition = '1'
      else :or
        condition = '2'
      end
      return self.search_value(query_list, condition, group, nsize)
    end

    protected    
    def send(message)
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
    end
    
    def each(&block)
      while line = socket.gets do
        if line == "END¥n" then
          break
        else
          yield(line)
        end
      end
    end

    def readlines
      ret = []
      while line = socket.gets do
        if line == "END¥n" then
          break
        else
          ret.push line
        end
      end
      return ret
    end

    def gets
      self.socket.gets
    end

    def request(message, options=nil)
      self.send(message)
      return self.gets
    end
    
    def request_lines(message, options=nil, &block)
      self.send(message)
      if block_given? then
        return self.each(&block)
      else
        return self.readlines
      end
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
