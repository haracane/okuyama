module Okuyama
  class Client < FastClient
    public
    
    def initialize(options)
      super(options)
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

      Okuyama.logger.debug "Okuyama::FastClient.set_value(key=#{key.inspect},val=#{val.inspect},tag_list=#{tag_list.inspect})" if @debug

      super(key, val, tag_list)
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

      Okuyama.logger.debug "Okuyama::FastClient.set_value(key=#{key.inspect},val=#{val.inspect},tag_list=#{tag_list.inspect})" if @debug

      return super(key, val, tag_list)
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

      Okuyama.logger.debug "Okuyama::FastClient.set_value(key=#{key.inspect},val=#{val.inspect},tag_list=#{tag_list.inspect})" if @debug

      return super(key, val, version, tag_list)
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
      return super(tag, flag)
    end

  end
end
