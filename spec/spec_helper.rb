$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'rspec'
require 'okuyama'
require 'stringio'

Okuyama.logger = Logger.new(STDERR)
Okuyama.logger.level = Logger::DEBUG
# Okuyama.logger.level = Logger::ERROR

# Requires supporting files with custom matchers and macros, etc,
# in ./support/ and its subdirectories.
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

RSpec.configure do |config|
  
end

module IOUtil
end
