require 'spec_helper'

describe Okuyama::Client do
  
  [true, false].each do |base64_encode_flag|
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
    
      describe "set_value_and_create_index(key, val, :tags=>tags, :group=>group, :min_n=>min_n, :max_n=>max_n)" do
        describe "when key exists," do
          it "should success" do
            Okuyama.logger.debug("send: #{@client.protocol.message_of_set_value_and_create_index(@testkey1, @testval1, @testtag, @testgroup, '1', '3').inspect}")
            result = @client.set_value_and_create_index(@testkey1, @testval1, :tags=>[@testtag], :group=>@testgroup, :min_n=>1, :max_n=>3)
            result.should be_true
          end
        end
        describe "when key does not exist," do
          it "should success" do
            Okuyama.logger.debug("send: #{@client.protocol.message_of_set_value_and_create_index(@testnewkey, @testnewval, @testnewtag, @testnewgroup, '1', '3').inspect}")
            result = @client.set_value_and_create_index(@testnewkey, @testnewval, :tags=>[@testnewtag], :group=>@testnewgroup, :min_n=>1, :max_n=>3)
            result.should be_true
          end
        end
      end

      describe "search_value(query)" do
        describe "when key exists," do
          it "should return keys" do
            Okuyama.logger.debug("send: #{@client.protocol.message_of_search_value([@testquery1]).inspect}")
            result = @client.search_value(@testquery1)
            result.sort!
            result.should == [@testkey1]
          end
        end
        describe "when key does not exist," do
          it "should return []" do
            result = @client.search_value(@testnewquery)
            result.should == []
          end
        end
      end

      describe "search_value(query, :condition=>:and, :group=>group, :nsize=>nsize)" do
        if false then
          describe "when key exists," do
            it "should return keys" do
              Okuyama.logger.debug("send: #{@client.protocol.message_of_search_value([@testquery1, @testquery2], '1', @testgroup, '3').inspect}")
              result = @client.search_value([@testquery1, @testquery2], :condition=>:and, :group=>@testgroup, :nsize=>3)
              result.sort!
              result.should == ['testkey1']
            end
          end
        end
        describe "when key does not exist," do
          it "should return []" do
            result = @client.search_value([@testquery1, @testnewquery], :condition=>:and, :group=>@testgroup, :nsize=>3)
            result.should == []
          end
        end
      end

      describe "search_value(query, :condition=>:or, :group=>group, :nsize=>nsize)" do
        if false then
          describe "when key exists," do
            it "should return keys" do
              Okuyama.logger.debug("send: #{@client.protocol.message_of_search_value([@testquery1, @testquery2], '2', @testgroup, '3').inspect}")
              result = @client.search_value([@testquery1, @testquery2], :condition=>:or, :group=>@testgroup, :nsize=>3)
              result.sort!
              result.should == ['testkey1']
            end
          end
        end
        describe "when key does not exist," do
          it "should return []" do
            result = @client.search_value([@testnewquery, @testnewquery], :condition=>:or, :group=>@testgroup, :nsize=>3)
            result.should == []
          end
        end
      end

    end
  end
end

