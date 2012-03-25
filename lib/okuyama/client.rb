module Okuyama
  class Client < FastClient
    public
    attr_accessor :to_i_flag, :parse_flag
    
    def initialize(options)
      super
      @to_i_flag = options[:to_i_flag]
      @to_i_flag = true if @to_i_flag.nil?
      @parse_flag = options[:parse_flag]
      @parse_flag = true if @parse_flag.nil?
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
      return self.request(message,options)
    end
    
    def set_new_value(key, val, *args)
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

      message = @protocol.set_new_value(key, val, tag_list)
      return self.request(message,options)
    end
    
    def set_value_version_check(key, val, version, *args)
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

      message = @protocol.set_value_version_check(key, val, version, tag_list)
      return self.request(message,options)
    end
    
    def get_tag_keys(tag, *args, &block)
      flag = 'false'
      options = nil
      argc = args.length
      if argc == 1 && args[0].is_a?(Hash) then
          flag = options[:flag]
      elsif 0 < argc then
        flag = args[0]
      end
      message = @protocol.get_tag_keys(tag, flag)
      return self.request(message,options,&block)
    end

    protected    
    def each(&block)
      return super(&block) if ! @prase_flag
      if block_given? then
        while result = socket.gets do
          if result == "END¥n" then
            break
          else
            result = self.parse_result(result)
            yield(result)
          end
        end
      end
    end

    def readlines
      return super if ! @parse_flag
      ret = []
      self.each { |record|
        STDERR.puts record.inspect
        ret.push record
      }
      return record
    end
    
    def gets
      line = super
      return self.parse_result(line.chomp) if @parse_flag
      return line
    end

    def parse_result(result)
      begin
        result = @protocol.parse_line_result(result, @to_i_flag)
      rescue Okuyama::ServerError => e
        raise Okuyama::ServerError, "#{e.message}, sent message = #{message.inspect}"
      end
      return result
    end

  end
end
