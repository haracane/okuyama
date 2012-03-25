require 'spec_helper'

describe Okuyama::Client do
  
  [true].each do |base64_encode_flag|
    describe "when base64_encode_flag = #{base64_encode_flag}" do
        
      before :each do
        @testnumkey = 'testnumkey'
        @testnumval = '10'
        @testkey1 = 'testkey1'
        @testval1 = 'testval1'
        @testkey2 = 'testkey2'
        @testval2 = 'testval2'
        @testnewkey = 'testnewkey'
        @testnewval = 'testnewval'
        @testnewval1 = 'testnewval1'
        @testtag = 'testtag'
        @testnewtag = 'testnewtag'
        
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
        end
        
        @client = Okuyama::Client.new(:host=>'localhost', :port=>8888, :base64_encode_flag=>base64_encode_flag)
        @client.set_value(@testkey1, @testval1, @testtag)
        @client.set_value(@testkey2, @testval2, @testtag)
        @client.remove_value(@testnewkey)
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
            key_list.include?(@testkey1).should be_true
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
            key_list.include?(@testkey1).should be_true
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
            result = @client.get_tag_keys(@testnewkey)
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
    
    
    end
  end
end

