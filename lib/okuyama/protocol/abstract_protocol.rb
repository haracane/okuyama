module Okuyama
  module Protocol
    class AbstractProtocol
      def method_missing(method_id, *args, &block)
        method_name = method_id.to_s
        if method_name =~ /^message_of_/ then
          next_method_name = $'
          result = self.capture(:stdout) {
            self.send(next_method_name, $stdout, *args, &block)
          }
          return result
        else
          super
        end
      end
      
      protected
      def capture(*streams)
        streams.map! { |stream| stream.to_s }
        begin
          result = StringIO.new
          streams.each { |stream| eval "$#{stream} = result" }
          yield
        ensure
          streams.each { |stream| eval("$#{stream} = #{stream.upcase}") }
        end
        result.string
      end
    end
  end
end