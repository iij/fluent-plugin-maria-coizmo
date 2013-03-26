$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'fluent-plugin'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'test/unit'
require 'fluent/test'

unless ENV.has_key?('VERBOSE')
  nulllogger = Object.new
  nulllogger.instance_eval {|obj|
    def method_missing(method, *args)
      # pass
    end
  }
  $log = nulllogger
end
