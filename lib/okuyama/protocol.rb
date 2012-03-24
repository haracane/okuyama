module Okuyama
  module Protocol
    def self.set_value(key, tag_list, val, options=nil)
      key_base64 = Base64.encode64(key).chomp
      val_base64 = Base64.encode64(val).chomp
      tags = nil
      if tag_list then
        tags = tag_list.map{|t|Base64.encode64(t).chomp}.join(':')
      end
      tags ||= "(B)"
      dlock = 0
      return "1,#{key_base64},#{tags},#{dlock},#{val_base64}"
    end

    def self.get_value(key, options=nil)
      key_base64 = Base64.encode64(key).chomp
      return "2,#{key_base64}"
    end

  end
end