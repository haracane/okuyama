require 'spec_helper'

describe Okuyama::Protocol::Version1 do
  before :each do 
    @client = Okuyama::Client.new(:host=>'localhost', :port=>8888)
  end

  after :each do 
    @client.close
  end
  
  it "should set value without tags" do
  end

  it "should set value with tags" do
  end

  it "should get value with tags" do
  end

end

