require 'spec_helper'

describe Okuyama::Client do
  
  [nil, true, false].each do |base64_encode_flag|
    describe "when base64_encode_flag = #{base64_encode_flag}" do
        
      before :each do
        @testnumval_int = 10
        @testnumval = @testnumval_int.to_s

        @testnumkey = 'testnumkey'
        @testkey1 = 'testkey1_0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz'
        @testval1 = 'testval1_0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz'
        @testkey2 = 'testkey2'
        @testval2 = 'testval2'
        @testnewkey = 'testnewkey'
        @testnewval = 'testnewval'
        @testnewval1 = 'testnewval1'
        @testtag = 'testtag_0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz'
        @testnewtag = 'testnewtag'
        @testgroup = 'testgroup'
        @testnewgroup = 'testnewgroup'
        @testquery1 = 'testval'
        @testquery2 = 'val1'
        @testnewquery = 'testnewval'
        
        if base64_encode_flag == false then 
          protocol = Okuyama::Protocol::Version1.new 
          @testnumkey = protocol.encode(@testnumkey)
          @testnumval = protocol.encode(@testnumval)
          @testkey1 = protocol.encode(@testkey1)
          @testval1 = protocol.encode(@testval1)
          @testkey2 = protocol.encode(@testkey2)
          @testval2 = protocol.encode(@testval2)
          @testnewkey = protocol.encode(@testnewkey)
          @testnewval = protocol.encode(@testnewval)
          @testnewval1 = protocol.encode(@testnewval1)
          @testtag = protocol.encode(@testtag)
          @testnewtag = protocol.encode(@testnewtag)
          @testgroup = protocol.encode(@testgroup)
          @testnewgroup = protocol.encode(@testnewgroup)
          @testquery1 = protocol.encode(@testquery1)
          @testquery2 = protocol.encode(@testquery2)
          @testnewquery = protocol.encode(@testnewquery)
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
    
      describe "protocol_version" do
        it "should return protocol version" do
          result = @client.protocol_version
          result.should == '1.0.0'
        end
      end
      
      describe "init_count" do
        it "should return init count(integer)" do
          result = @client.init_count
          result.should > 0
        end
        
        it "should return init count(text)" do
          @client.to_i_flag = false
          result = @client.init_count
          result.should =~ /[0-9]+/
        end
      end
    
      describe "set_value(key, value)" do
        describe "when key exists," do
          it "should set value" do
            result = @client.set_value(@testkey1, @testnewval1)
            result.should == true
            val = @client.get_value(@testkey1)
            val.should == @testnewval1
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_value(@testnewkey, @testnewval)
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
          end
        end
      end
    
      describe "set_value(key, value, tag): tag is a String" do
        describe "when key exists," do
          it "should set value" do
            result = @client.set_value(@testkey1, @testnewval1, @testnewtag)
            result.should == true
            val = @client.get_value(@testkey1)
            val.should == @testnewval1
            key_list = @client.get_tag_keys(@testtag)
            key_list.include?(@testkey1).should be_true
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testkey1).should be_true
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_value(@testnewkey, @testnewval, @testnewtag)
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testnewkey).should be_true
          end
        end
      end
    
      describe "set_value(key, value, tags): 'tags' is a Array of String" do
        describe "when key exists," do
          it "should set value" do
            result = @client.set_value(@testkey1, @testnewval1, [@testtag])
            result.should == true
            val = @client.get_value(@testkey1)
            val.should == @testnewval1
            key_list = @client.get_tag_keys(@testtag)
            key_list.include?(@testkey1).should be_true
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testkey1).should be_false
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_value(@testnewkey, @testnewval, [@testtag])
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testnewkey).should be_true
          end
        end
      end
    
      describe "set_value(key, value, :tags=>tags): 'tags' is a Array of String" do
        describe "when key exists," do
          it "should set value" do
            result = @client.set_value(@testkey1, @testnewval1, :tags=>[@testtag])
            result.should == true
            val = @client.get_value(@testkey1)
            val.should == @testnewval1
            key_list = @client.get_tag_keys(@testtag)
            key_list.include?(@testkey1).should be_true
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testkey1).should be_false
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_value(@testnewkey, @testnewval, :tags=>[@testnewtag])
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testnewkey).should be_true
          end
        end
      end
    
      describe "get_value(key)" do
        describe "when key exists," do
          it "should return value" do
            result = @client.get_value(@testkey1)
            result.should == @testval1
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            result = @client.get_value(@testnewkey)
            result.should be_nil
          end
        end
      end
    
      describe "get_tag_keys(tag)" do
        describe "when tag exists," do
          it "should return keys" do
            result = @client.get_tag_keys(@testtag)
            result.sort!
            result.shift == @testkey1
            result.shift == @testkey2
            result == []
          end
        end
        describe "when tag does not exist," do
          it "should return []" do
            result = @client.get_tag_keys(@testnewtag)
            result.should == []
          end
        end
      end
    
      describe "remove_value(key)" do
        describe "when key does not exist," do
          it "should fail" do
            result = @client.remove_value(@testnewkey)
            result.should be_nil
          end
        end
        describe "when key exists," do
          it "should success" do
            result = @client.remove_value(@testkey1)
            result.should == true
          end
        end
      end
      
      describe "set_new_value(key, value)" do
        describe "when key exists," do
          it "should fail" do
            result = @client.set_new_value(@testkey1, @testnewval1)
            result.should be_nil
            val = @client.get_value(@testkey1)
            val.should == @testval1
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_new_value(@testnewkey, @testnewval)
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
          end
        end
      end
    
      describe "set_new_value(key, value, tag): 'tag' is a String" do
        describe "when key exists," do
          it "should fail" do
            result = @client.set_new_value(@testkey1, @testnewval1, @testnewtag)
            result.should be_nil
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_new_value(@testnewkey, @testnewval, @testnewtag)
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testnewkey).should be_true
          end
        end
      end
    
      describe "set_new_value(key, value, tags): 'tags' is a Array of String" do
        describe "when key exists," do
          it "should fail" do
            result = @client.set_new_value(@testkey1, @testnewval1, [@testtag])
            result.should be_nil
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_new_value(@testnewkey, @testnewval, [@testtag])
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testnewkey).should be_true
          end
        end
      end
    
      describe "set_new_value(key, value, :tags=>tags): 'tags' is a Array of String" do
        describe "when key exists," do
          it "should fail" do
            result = @client.set_new_value(@testkey1, @testnewval1, :tags=>[@testtag])
            result.should be_nil
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.set_new_value(@testnewkey, @testnewval, :tags=>[@testnewtag])
            result.should == true
            val = @client.get_value(@testnewkey)
            val.should == @testnewval
            key_list = @client.get_tag_keys(@testnewtag)
            key_list.include?(@testnewkey).should be_true
          end
        end
      end
    
      describe "get_value_version_check(key)" do
        describe "when key exists," do
          it "should return value and version number" do
            result = @client.get_value_version_check(@testkey1)
            result.size.should == 2
            result[0].should == @testval1
            result[1].length.should > 0
          end
        end
        describe "when key does not exist," do
          it "should set value" do
            result = @client.get_value_version_check(@testnewkey)
            result.should be_nil
          end
        end
      end
    
      describe "set_value_version_check(key, value, version)" do
        describe "when key exists," do
          describe "when version number is same" do
            it "should set value" do
              result = @client.get_value_version_check(@testkey1)
              version = result[1]
              result = @client.set_value_version_check(@testkey1, @testnewval1, result[1])
              result.should == true
              val = @client.get_value(@testkey1)
              val.should == @testnewval1
            end
          end
          describe "when version number is different" do
            it "should fail" do
              result = @client.set_value_version_check(@testkey1, @testnewval1, 'differentversion')
              result.should be_nil
            end
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            result = @client.set_value_version_check(@testnewkey, @testnewval, 'newversion')
            result.should be_nil
          end
        end
      end
    
      describe "set_value_version_check(key, value, version, tag): tag is a String" do
        describe "when key exists," do
          describe "when version number is same" do
            it "should set value" do
              result = @client.get_value_version_check(@testkey1)
              version = result[1]
              result = @client.set_value_version_check(@testkey1, @testnewval1, version, @testnewtag)
              result.should == true
              val = @client.get_value(@testkey1)
              val.should == @testnewval1
              key_list = @client.get_tag_keys(@testnewtag)
              key_list.include?(@testkey1).should be_true
            end
          end
          describe "when version number is different" do
            it "should fail" do
              result = @client.set_value_version_check(@testkey1, @testnewval1, 'differentversion')
              result.should be_nil
            end
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            result = @client.set_value_version_check(@testnewkey, @testnewval, 'newversion')
            result.should be_nil
          end
        end
      end
    
      describe "set_value_version_check(key, value, version, tags): 'tags' is a Array of String" do
        describe "when key exists," do
          describe "when version number is same" do
            it "should set value" do
              result = @client.get_value_version_check(@testkey1)
              version = result[1]
              result = @client.set_value_version_check(@testkey1, @testnewval1, version, [@testnewtag])
              result.should == true
              val = @client.get_value(@testkey1)
              val.should == @testnewval1
              key_list = @client.get_tag_keys(@testnewtag)
              key_list.include?(@testkey1).should be_true
            end
          end
          describe "when version number is different" do
            it "should fail" do
              result = @client.set_value_version_check(@testkey1, @testnewval1, 'differentversion')
              result.should be_nil
            end
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            result = @client.set_value_version_check(@testnewkey, @testnewval, 'newversion')
            result.should be_nil
          end
        end
      end
    
      describe "set_value_version_check(key, value, version, :tags=>tags): 'tags' is a Array of String" do
        describe "when key exists," do
          describe "when version number is same" do
            it "should set value" do
              result = @client.get_value_version_check(@testkey1)
              version = result[1]
              result = @client.set_value_version_check(@testkey1, @testnewval1, version, :tags=>[@testnewtag])
              result.should == true
              val = @client.get_value(@testkey1)
              val.should == @testnewval1
              key_list = @client.get_tag_keys(@testnewtag)
              key_list.include?(@testkey1).should be_true
            end
          end
          describe "when version number is different" do
            it "should fail" do
              result = @client.set_value_version_check(@testkey1, @testnewval1, 'differentno')
              result.should be_nil
            end
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            result = @client.set_value_version_check(@testnewkey, @testnewval, 'newversion')
            result.should be_nil
          end
        end
      end
    
      describe "get_multi_value(key)" do
        describe "when all keys exist," do
          it "should return values" do
            result = @client.get_multi_value([@testkey1, @testkey2])
            result.should == [@testval1, @testval2]
          end
        end
        describe "when keys exist or not," do
          it "should return existing values" do
            result = @client.get_multi_value([@testkey1, @testnewkey])
            result.should == [@testval1, nil]
          end
        end
        describe "when no keys exist," do
          it "should return nils" do
            result = @client.get_multi_value([@testnewkey, @testnewkey])
            result.should == [nil, nil]
          end
        end
      end

      describe "get_tag_values(tag)" do
        describe "when keys exist," do
          it "should return key-values" do
            result = @client.get_tag_values(@testtag)
            result.sort!{|a,b|a[0]<=>b[0]}
            result.shift.should == [@testkey1, @testval1]
            result.shift.should == [@testkey2, @testval2]
            result.should == []
          end
        end
        describe "when no keys exist," do
          it "should return []" do
            result = @client.get_tag_values(@testnewtag)
            result.should == []
          end
        end
      end

      describe "remove_tag_from_key(tag, key)" do
        describe "when tag&key exist," do
          it "should remove tag from key" do
            result = @client.remove_tag_from_key(@testtag, @testkey1)
            result.should be_true
            key_list = @client.get_tag_keys(@testtag)
            key_list.include?(@testkey1).should be_false
          end
        end
        describe "when tag&key do not exist," do
          it "should fail" do
            result = @client.remove_tag_from_key(@testnewtag, @testkey1)
            result.should be_nil
          end
        end
      end

      describe "set_value_and_create_index(key, val)" do
        describe "when key exists," do
          it "should success" do
            result = @client.set_value_and_create_index(@testkey1, @testval1)
            result.should be_true
          end
        end
        describe "when key does not exist," do
          it "should success" do
            result = @client.set_value_and_create_index(@testnewkey, @testnewval)
            result.should be_true
          end
        end
      end

      describe "set_value_and_create_index(key, val, :tags=>tags, :group=>group, :min_n=>min_n, :max_n=>max_n)" do
        describe "when key exists," do
          it "should success" do
            result = @client.set_value_and_create_index(@testkey1, @testval1, :tags=>[@testtag], :group=>@testgroup, :min_n=>1, :max_n=>3)
            result.should be_true
          end
        end
        describe "when key does not exist," do
          it "should success" do
            result = @client.set_value_and_create_index(@testnewkey, @testnewval, :tags=>[@testnewtag], :group=>@testnewgroup, :min_n=>1, :max_n=>3)
            result.should be_true
          end
        end
      end

      describe "search_value(query)" do
        describe "when key exists," do
          it "should return keys" do
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
            # result = @client.incr_value(@testnewkey, 1)
            # result.should be_nil
          end
        end
      end
      
      describe "decr_value(key)" do
        describe "when key exists," do
          describe "when value is a number" do
            it "should return decremented value(integer)" do
              result = @client.decr_value(@testnumkey, 1)
              result.should == (@testnumval_int - 1)
            end
            it "should return decremented value(text)" do
              @client.to_i_flag = false
              val = @client.get_value(@testnumkey)
              result = @client.decr_value(@testnumkey, 1)
              expected = (@testnumval_int - 1).to_s
              expected = Base64.encode64(expected).chomp if base64_encode_flag == false
              result.should == expected
            end
          end
          describe "when value is not a number" do
            it "should return 1(integer)" do
              result = @client.decr_value(@testkey1, 1)
              result.should == 0
            end
          end
        end
        describe "when key does not exist," do
          it "should fail" do
            # result = @client.decr_value(@testnewkey, 1)
            # result.should be_nil
          end
        end
      end
      
    end
  end
end

