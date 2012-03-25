require 'spec_helper'

describe Okuyama::Client do
  before :each do 
    @client = Okuyama::Client.new(:host=>'localhost', :port=>8888)
  end

  after :each do 
    @client.close
  end

  it "should return protocol version" do
    result = @client.protocol_version
    result.should == '1.0.0'
  end
  
  it "should get init count" do
    result = @client.init_count
    result.should > 0
    @client.parse_int_flag = false
    result = @client.init_count
    result.should =~ /[0-9]+/
  end

  it "should set value without tags" do
    result = @client.set_value('testkey', 'testval')
    result.should == true
  end

  it "should set value with tags" do
    result = @client.set_value('testkey', 'testval', 'testtag')
    result.should == true
    result = @client.set_value('testkey', 'testval', ['testtag'])
    result.should == true
    result = @client.set_value('testkey', 'testval', :tags=>['testtag'])
    result.should == true
  end

  it "should get value" do
    result = @client.set_value('testkey', 'testval')
    result = @client.get_value('testkey')
    result.should == 'testval'
  end

  it "should not get value with new key" do
    result = @client.get_value('no_exist_key')
    result.should be_nil
  end

  it "should get value" do
    result = @client.set_value('testkey', 'testval')
    result = @client.get_value('testkey')
    result.should == 'testval'
  end

  it "should get keys from tag" do
    result = @client.set_value('testkey', 'testval', ['testtag'])
    result = @client.get_tag_keys('testtag')
    result.should == ['testkey']
  end

end

