require 'spec_helper'

describe Okuyama::Client do
  
  [true,true].each do |base64_encode_flag|
    describe "when base64_encode_flag = #{base64_encode_flag}" do
        
      before :each do
        @testnumval_int = 10

        @testnumkey = 'testnumkey'
        @testnumval = @testnumval_int.to_s
        @testkey1 = 'testkey1'
        @testval1 = 'testval1'
        @testkey2 = 'testkey2'
        @testval2 = 'testval2'
        @testnewkey = 'testnewkey'
        @testnewval = 'testnewval'
        @testnewval1 = 'testnewval1'
        @testtag = 'testtag'
        @testnewtag = 'testnewtag'
        @testgroup = 'testgroup'
        @testnewgroup = 'testnewgroup'
        @testquery1 = 'testval'
        @testquery2 = 'val1'
        @testnewquery = 'testnewval'
        
        if base64_encode_flag == false then 
          @testnumkey = Base64.encode64(@testnumkey).chomp
          @testnumval = Base64.encode64(@testnumval).chomp
          @testkey1 = Base64.encode64(@testkey1).chomp
          @testval1 = Base64.encode64(@testval1).chomp
          @testkey2 = Base64.encode64(@testkey2).chomp
          @testval2 = Base64.encode64(@testval2).chomp
          @testnewkey = Base64.encode64(@testnewkey).chomp
          @testnewval = Base64.encode64(@testnewval).chomp
          @testnewval1 = Base64.encode64(@testnewval1).chomp
          @testtag = Base64.encode64(@testtag).chomp
          @testnewtag = Base64.encode64(@testnewtag).chomp
          @testgroup = Base64.encode64(@testgroup).chomp
          @testnewgroup = Base64.encode64(@testnewgroup).chomp
          @testquery1 = Base64.encode64(@testquery1).chomp
          @testquery2 = Base64.encode64(@testquery2).chomp
          @testnewquery = Base64.encode64(@testnewquery).chomp
        end
        
        @client = Okuyama::Client.new(:host=>'localhost', :port=>8888, :base64_encode_flag=>base64_encode_flag)
        @client.debug = true
        @client.remove_value(@testnumkey)
        @client.remove_value(@testkey1)
        @client.remove_value(@testkey2)
        @client.remove_value(@testnewkey)
        @client.remove_tag_from_key(@testnewtag, @testkey1)
        @client.set_value(@testnumkey, @testnumval)
        @client.set_value_and_create_index(@testkey1, @testval1, :tags=>[@testtag], :group=>@testgroup, :min_n=>1, :max_n=>3)
        @client.set_value(@testkey2, @testval2, @testtag)

      end
    
      after :each do 
        @client.close
      end
    
      describe "incr_value(key, val)" do
        describe "when key exists," do
          describe "when value is a number" do
            it "should return incremented value(integer)" do
              Okuyama.logger.debug("send: #{@client.protocol.message_of_incr_value(@testnumkey, '1').inspect}")
              result = @client.incr_value(@testnumkey, 1)
              result.should == (@testnumval_int + 1)
            end
            it "should return incremented value(text)" do
              @client.to_i_flag = false
              result = @client.incr_value(@testnumkey, 1)
              expected = (@testnumval_int + 1).to_s
              expected = Base64.encode64(expected).chomp if base64_encode_flag == false
              result.should == expected
            end
          end
          describe "when value is not a number" do
            it "should return 1(integer)" do
              result = @client.incr_value(@testkey1, 1)
              result.should == 1
            end
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            Okuyama.logger.debug("send: #{@client.protocol.message_of_incr_value(@testnewkey, '1').inspect}")
            result = @client.incr_value(@testnewkey, 1)
            result.should be_nil
          end
        end
      end
    
    end
  end
end

