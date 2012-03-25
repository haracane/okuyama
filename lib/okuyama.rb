require 'base64'
require 'socket'

autoload :Logger, 'logger'

module Okuyama
  autoload :Client, 'okuyama/client'
  autoload :FastClient, 'okuyama/fast_client'
  module Protocol
    autoload :AbstractProtocol, 'okuyama/protocol/abstract_protocol'
    autoload :Version1, 'okuyama/protocol/version1'
  end

  # generic error
  class OkuyamaError < RuntimeError; end
  # socket/server communication error
  class NetworkError < OkuyamaError; end
  # server error
  class ServerError < OkuyamaError; end

  def self.logger
    @logger ||= (rails_logger || default_logger)
  end

  def self.rails_logger
    (defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger) ||
    (defined?(RAILS_DEFAULT_LOGGER) && RAILS_DEFAULT_LOGGER.respond_to?(:debug) && RAILS_DEFAULT_LOGGER)
  end

  def self.default_logger
    require 'logger'
    l = Logger.new(STDOUT)
    l.level = Logger::INFO
    l
  end

  def self.logger=(logger)
    @logger = logger
  end

end
