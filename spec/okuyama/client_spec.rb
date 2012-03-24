require 'spec_helper'

describe Okuyama::Client do
  before :each do 
    @client = Okuyama::Client.new(:host=>'localhost', :port=>8888)
  end

  after :each do 
    @client.close
  end
  
  it "should set value without tags" do
    result = @client.set_value('testkey', 'testval')
    result.should == ["true", "OK"]
  end

  it "should set value with tags" do
    result = @client.set_value('testkey', 'testval', :tags=>['testtag'])
    result.should == ["true", "OK"]
  end

  it "should get value with tags" do
    result = @client.set_value('testkey', 'testval')
    result = @client.get_value('testkey')
    result.should == 'testval'
  end

end

