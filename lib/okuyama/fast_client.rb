module Okuyama
  class FastClient
    public
    attr_reader :host, :port, :timeout, :retry_max, :protocol
    attr_accessor :base64_encode_flag, :to_i_flag, :parse_flag
    
    def initialize(options)
      @host = options[:host] || 'localhost'
      @port = options[:port] || 8888
      @timeout = options[:timeout] || 10
      @retry_max = options[:retry_max] || 3
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
    
    def debug=(d)
      @debug = d
      @protocol.debug = d
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
    alias :recv_set_value :recvs    
    alias :recv_get_value :recvs
    alias :recv_get_tag_keys :recvs
    alias :recv_remove_value :recvs
    alias :recv_set_new_value :recvs
    alias :recv_get_value_version_check :recvs
    alias :recv_set_value_version_check :recvs
    alias :recv_incr_value :recvs
    alias :recv_decr_value :recvs
    alias :recv_get_multi_value :recv_lines
    alias :recv_get_tag_values :recv_lines
    alias :recv_remove_tag_from_key :recvs
    alias :recv_set_value_and_create_index :recvs
    alias :recv_search_value :recvs

    [ :init_count, :set_value, :get_value, :get_tag_keys,
      :remove_value, :set_new_value, :get_value_version_check, :set_value_version_check,
      :incr_value, :decr_value, :get_multi_value, :get_tag_values, :remove_tag_from_key,
      :set_value_and_create_index, :search_value].each do |name|
      eval <<-EOF
        def send_#{name}(*args)
          @protocol.#{name}(self.socket, *args)
        end
        def #{name}(*args, &block)
          self.send_#{name}(*args)
          self.recv_#{name}(&block)
        end
      EOF
    end

    
    protected
    def each(&block)
      while result = socket.gets do
        result.chomp!
        if result == "END" then
          break
        else
          result = self.parse_result(result) if @parse_flag
          yield(result)
        end
      end
    end

    def readlines
      ret = []
      while result = socket.gets do
        result.chomp!
        if result == "END" then
          break
        else
          result = self.parse_result(result) if @parse_flag
          ret.push result
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
