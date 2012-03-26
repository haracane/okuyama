module Okuyama
  class FastClient
    public
    attr_reader :host, :port, :timeout, :retry_max, :protocol
    attr_accessor :debug, :base64_encode_flag, :to_i_flag, :parse_flag, :recv_flag
    
    def initialize(options)
      @host = options[:host] || 'localhost'
      @port = options[:port] || 8888
      @timeout = options[:timeout] || 10
      @retry_max = options[:retry_max] || 3
      @recv_flag = true
      @recv_flag = options[:recv_flag] || @recv_flag
      @to_i_flag = options[:to_i_flag]
      @to_i_flag = true if @to_i_flag.nil?
      @parse_flag = options[:parse_flag]
      @parse_flag = true if @parse_flag.nil?
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
          @socket = nil
          raise e if raise_exception
          Okuyama.logger.error "ERROR: #{e.message}"
        end
      end
      @socket = nil
    end
    
    def recvs
      return if ! @recv_flag
      line = self.socket.gets
      line.chomp!
      # Disable debug message for better performance
      Okuyama.logger.debug "recv: #{line.inspect}" if @debug
      return self.parse_result(line) if @parse_flag
      return line
    end
    
    def recv_lines(&block)
      if block_given? then
        return self.each(&block)
      else
        return self.readlines
      end
    end
    
    alias :recv_init_count :recvs    
    def init_count
      @protocol.init_count(self.socket)
      return self.recv_init_count
    end
    
    alias :recv_set_value :recvs
    def set_value(key, val, tag_list=nil)
      @protocol.set_value(self.socket, key, val, tag_list)
      return self.recv_set_value
    end
    
    alias :recv_get_value :recvs
    def get_value(key)
      @protocol.get_value(self.socket, key)
      return self.recv_get_value
    end
    
    alias :recv_get_tag_keys :recvs
    def get_tag_keys(tag, flag='false')
      @protocol.get_tag_keys(self.socket, tag, flag)
      return self.recv_get_tag_keys
    end
    
    alias :recv_remove_value :recvs
    def remove_value(key)
      @protocol.remove_value(self.socket, key)
      return self.recv_remove_value
    end
    
    alias :recv_set_new_value :recvs
    def set_new_value(key, val, tag_list=nil)
      @protocol.set_new_value(self.socket, key, val, tag_list)
      return self.recv_set_new_value
    end
    
    alias :recv_get_value_version_check :recvs
    def get_value_version_check(key)
      @protocol.get_value_version_check(self.socket, key)
      return self.recv_get_value_version_check
    end
    
    alias :recv_set_value_version_check :recvs
    def set_value_version_check(key, val, version, tag_list=nil)
      @protocol.set_value_version_check(self.socket, key, val, version, tag_list)
      return self.recv_set_value_version_check
    end
    
    alias :recv_incr_value :recvs
    def incr_value(key, val)
      @protocol.incr_value(self.socket, key, val.to_s)
      return self.recv_incr_value
    end
    
    alias :recv_decr_value :recvs
    def decr_value(key, val)
      @protocol.decr_value(self.socket, key, val.to_s)
      return self.recv_decr_value
    end
    
    alias :recv_get_multi_value :recv_lines
    def get_multi_value(key_list, &block)
      Okuyama.logger.debug "send: #{@protocol.message_of_get_multi_value(key_list).inspect}" if @debug
      @protocol.get_multi_value(self.socket, key_list)
      return self.recv_get_multi_value(&block)
    end
    
    alias :recv_get_tag_values :recv_lines
    def get_tag_values(tag, &block)
      @protocol.get_tag_values(self.socket, tag)
      return self.recv_get_tag_values(&block)
    end

    alias :recv_remove_tag_from_key :recvs
    def remove_tag_from_key(tag, key)
      @protocol.remove_tag_from_key(self.socket, tag, key)
      return self.recv_remove_tag_from_key
    end

    alias :recv_set_value_and_create_index :recvs
    def set_value_and_create_index(key, val, options=nil)
      if options then
        tag_list = options[:tags]
        group = options[:group]
        min_n = options[:min_n]
        max_n = options[:max_n]
        min_n = min_n.to_s if min_n
        max_n = max_n.to_s if max_n
      end
      
      @protocol.set_value_and_create_index(self.socket, key, val, tag_list, group, min_n, max_n)
      return self.recv_set_value_and_create_index
    end

    alias :recv_search_value :recvs
    def search_value(query_list, options=nil)
      if ! query_list.is_a? Array then
        query_list = [query_list.to_s]
      end
      
      if options then
        condition = options[:condition]
        group = options[:group]
        nsize = options[:nsize]
        nsize = nsize.to_s if nsize
        case condition
        when :and
          condition = '1'
        else :or
          condition = '2'
        end
      end

      @protocol.search_value(self.socket, query_list, condition, group, nsize)
      return self.recv_search_value
    end
    
    protected
    def each(&block)
      return if ! @recv_flag
      while line = socket.gets do
        line.chomp!
        if line == "END" then
          break
        else
          yield(self.parse_result(line))
        end
      end
    end

    def readlines
      return if ! @recv_flag
      ret = []
      while line = socket.gets do
        line.chomp!
        if line == "END" then
          break
        else
          ret.push self.parse_result(line)
        end
      end
      return ret
    end

    def socket
      if @socket.nil? then
        retry_count = 0
        begin
          @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
          sockaddr = Socket.sockaddr_in(@port, @host)
          if @timeout then
            secs = Integer(@timeout)
            usecs = Integer((@timeout - secs) * 1_000_000)
            optval = [secs, usecs].pack("l_2")
            @socket.setsockopt Socket::SOL_SOCKET, Socket::SO_RCVTIMEO, optval
            @socket.setsockopt Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, optval
          end
          @socket.connect(sockaddr)
        rescue Exception => e
          if retry_count < @retry_max then
            Okuyama.logger.error "ERROR: #{e.message}"
            @socket.close if @socket != nil
            @socket = nil
            retry_count += 1
            retry
          else
            raise e
          end
          @socket = nil
          raise e
        end
      end
      return @socket
    end
    
    def parse_result(result)
      begin
        result = @protocol.parse_line_result(result, @to_i_flag)
      rescue Okuyama::ServerError => e
        self.close
        raise Okuyama::ServerError, "#{e.message}, message = #{result.inspect}"
      end
      return result
    end
  end
end
