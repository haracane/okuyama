require 'spec_helper'

describe Okuyama::Protocol::Version1 do
  before :each do 
    @protocol = Okuyama::Protocol::Version1.new
  end

  it "should write incr_value message" do
    result = @protocol.message_of_incr_value('testnumkey', '1')
    result.chomp.should == "13,dGVzdG51bWtleQ==,0,MQ=="
  end

  it "should write set_value_and_create_index message" do
    result = @protocol.message_of_set_value_and_create_index('key', 'val', ['tag'], 'group', '1', '3')
    result.chomp.should == "42,a2V5,dGFn,0,dmFs,Z3JvdXA=,1,3"
  end

  it "should write search_value message" do
    result = @protocol.message_of_search_value(['query1', 'query2'], '1', 'group', '3')
    result.chomp.should == "43,cXVlcnkx:cXVlcnky,1,Z3JvdXA=,3"
  end

end

