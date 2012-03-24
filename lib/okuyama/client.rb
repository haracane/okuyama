module Okuyama
  class Client
    def initialize(options)
      @host = options[:host] || 'localhost'
      @port = options[:port] || 8888
    end
    
    def socket
      if @socket.nil? then
        @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
        sockaddr = Socket.sockaddr_in(@port, @host)
        @socket.connect(sockaddr)
      end
      return @socket
    end
    
    def close
      @socket.close if @socket
      @socket = nil
    end
    
    def get_value(key, options=nil)
      message = Okuyama::Protocol.get_value(key,options)
      result = self.send_msg(message,options)
      return result[1]
    end
    
    def send_msg(msg, options=nil)
      client = self.socket
      Okuyama.logger.debug "send: #{msg}"
      self.socket.puts(msg)
      result = socket.gets
      result.chomp!
      Okuyama.logger.debug "recv: #{result}"
      ret = self.parse_result(result,options)
    end
    
    def set_value(key, val, options=nil)
      tag_list = options[:tags] if options
      message = Okuyama::Protocol.set_value(key, tag_list, val, options)
      result = self.send_msg(message)
      return result
    end
    
    def parse_result(result,options=nil)
      record = result.split(/,/)
      code = record.shift
      case code
      when '2'
        record[1] = Base64.decode64(record[1])
      end
      return record
    end
  end
end
