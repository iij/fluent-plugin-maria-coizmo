require './helper'
require 'out_maria_coizmo'
require 'time'
require 'msgpack'
require 'mysql2'
require 'mocha'

class MariaCoizmoOutputTest < Test::Unit::TestCase

  def setup
    Fluent::Test.setup
    @obj = Fluent::MariaCoizmoOutput.new
  end

  CONFIG = %[
host localhost
database coizmo
username root
flush_interval 10s
]

  def create_driver(conf = CONFIG, tag = "test")
    d = Fluent::Test::BufferedOutputTestDriver.new(Fluent::MariaCoizmoOutput, tag).configure(conf)

    init = {
      :host => d.instance.host,
      :port => d.instance.port,
      :username => d.instance.username,
      :database => d.instance.database
    }

    obj = Object.new
    obj.instance_eval {
      def escape(v) return v; end
      def query(*args); [1]; end
      def close; true; end
    }
    d.instance.handler = obj

    @handler = Mysql2::Client.new(init)
    return d
  end

  def test_format
    d = create_driver
    time = Time.parse("2012/07/31 13:13:13").to_i
    d.emit({"a"=>1},time)
    d.emit({"a"=>2},time)

    d.expect_format ['test', time, {"a" => 1}].to_msgpack    
    d.expect_format ['test', time, {"a" => 2}].to_msgpack    
  end

  def test_write
    d = create_driver
    @handler.stubs(:query).returns("success")

    time = Time.parse("2012/12/01 10:10:10").to_i
    record = {
      "host"=>"hogehoge",
      "device"=>"utrh0",
      "value"=> 50.34
    }

    chunk = ["test", time, record].to_msgpack
    # DBに書き込まれるので注意 data_201212
    #d.emit(record,time)
    #d.run
  end

  def test_get_sensor_id
    d = create_driver
    assert_equal 2, @obj.__send__(:get_sensor_id,@handler,"hogehoge","utrh1")
  end

  def test_get_host_id
    d = create_driver
    assert_equal 2,  @obj.__send__(:get_host_id,@handler,"hogehoge")
  end

  def test_set_sensor
    d = create_driver
    @handler.stubs(:query).returns("success")
    assert_equal "success", @obj.__send__(:set_sensor,@handler,"hogehoge","test","1")
  end

end

