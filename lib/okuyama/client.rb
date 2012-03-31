module Okuyama
  class Client < FastClient
    public
    
    def initialize(options)
      super(options)
    end
    
    
    def send_set_value(key, val, *args)
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

      Okuyama.logger.debug "Okuyama::Client.send_set_value(key=#{key.inspect},tag_list=#{tag_list.inspect},val=#{val.inspect})" if @debug

      super(key, tag_list, val)
    end
    
    def send_set_new_value(key, val, *args)
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

      Okuyama.logger.debug "Okuyama::Client.send_set_value(key=#{key.inspect},tag_list=#{tag_list.inspect},val=#{val.inspect})" if @debug

      return super(key, tag_list, val)
    end
    
    def send_set_value_version_check(key, val, version, *args)
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

      return super(key, tag_list, val, version)
    end
    
    def send_get_tag_keys(tag, *args, &block)
      flag = 'false'
      options = nil
      argc = args.length
      if argc == 1 && args[0].is_a?(Hash) then
          flag = options[:flag]
      elsif 0 < argc then
        flag = args[0]
      end
      return super(tag, flag, &block)
    end

    def send_incr_value(key, val)
      super(key, val.to_s)
    end
    
    def send_decr_value(key, val)
      super(key, val.to_s)
    end

    def send_set_value_and_create_index(key, val, options=nil)
      if options then
        tag_list = options[:tags]
        group = options[:group]
        min_n = options[:min_n]
        max_n = options[:max_n]
        min_n = min_n.to_s if min_n
        max_n = max_n.to_s if max_n
      end
      
      super(key, val, tag_list, group, min_n, max_n)
    end
    
    def send_search_value(query_list, options=nil)
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
        when :or
          condition = '2'
        end
      end
      condition ||= '1'

      super(query_list, condition, group, nsize)
    end
  end
end
