module Okuyama
  class FastClient
    public
    attr_reader :host, :port, :timeout, :retry_max, :protocol
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
      @protocol.init_count(self.socket)
      return self.recvs
    end
    
    def set_value(key, val, tag_list=nil)
      @protocol.set_value(self.socket, key, val, tag_list)
      return self.recvs
    end
    
    def get_value(key)
      @protocol.get_value(self.socket, key)
      return self.recvs
    end
    
    def get_tag_keys(tag, flag='false')
      @protocol.get_tag_keys(self.socket, tag, flag)
      return self.recvs
    end
    
    def remove_value(key)
      @protocol.remove_value(self.socket, key)
      return self.recvs
    end
    
    def set_new_value(key, val, tag_list=nil)
      @protocol.set_new_value(self.socket, key, val, tag_list)
      return self.recvs
    end
    
    def get_value_version_check(key)
      @protocol.get_value_version_check(self.socket, key)
      return self.recvs
    end
    
    def set_value_version_check(key, val, version, tag_list=nil)
      @protocol.set_value_versin_check(self.socket, key, val, version, tag_list)
      return self.recvs
    end
    
    def incr_value(key, val)
      @protocol.incr_value(self.socket, key, val.to_s)
      return self.recvs
    end
    
    def decr_value(key, val)
      @protocol.decr_value(self.socket, key, val.to_s)
      return self.recvs
    end
    
    def get_multi_value(key_list, &block)
      @protocol.get_multi_value(self.socket, key_list)
      return self.recv_lines(&block)
    end
    
    def get_tag_values(tag, &block)
      @protocol.get_tag_values(self.socket, tag)
      return self.recv_lines(&block)
    end

    def remove_tag_from_key(tag, key)
      @protocol.remove_tag_from_key(self.socket, tag, key)
      return self.recvs
    end

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
      return self.recvs
    end

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
      return self.recvs
    end
    
    def method_missiong(method_id, *args, &block)
      method_name = method_id.to_s
      if method_name =~ /^message_of_/ then
        next_method_name = $'
        result = capture(:stdout) {
          @protocol.send(method_name, $stdout, *args, &block)
        }
        return result
      else
        super
      end
    end

    protected    

    def each(&block)
      while line = socket.gets do
        line.chomp!
        if line == "END" then
          break
        else
          yield(line)
        end
      end
    end

    def readlines
      ret = []
      while line = socket.gets do
        if line == "END" then
          break
        else
          ret.push line
        end
      end
      return ret
    end

    def recvs
      line = self.socket.gets
      line.chomp!
      return line
    end
    
    def recv_lines(&block)
      if block_given? then
        return self.each(&block)
      else
        return self.readlines
      end
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
  end
end
