require 'mysql2'

module Fluent
  class MariaCoizmoOutput < BufferedOutput
    Plugin.register_output('maria_coizmo', self)

    config_param :host, :string
    config_param :database, :string
    config_param :username, :string
    config_param :password, :string, :default => ''
    
    def initialize
      super
      @sensor_id_cache = Hash.new
    end

    def configure(conf)
      super
      @init = {
        :host => @host,
        :username => @username,
        :password => @password,
        :database => @database
      }
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      handler = Mysql2::Client.new(@init)

      chunk.msgpack_each do |tag,time,data|
        sensor_id = @sensor_id_cache[data["device"]]
        if sensor_id.nil?
          sensor_id = get_sensor_id(handler,data["host"],data["device"],data["unit"])
          $log.info "cached sensor_id: #{sensor_id}"
          @sensor_id_cache[data["device"]] = sensor_id
        end

        sql = "INSERT INTO data (sensor_id,time,value,leverage) VALUES (#{sensor_id},from_unixtime(#{time}),#{data["value"]},#{data["leverage"]})"
        handler.query(sql)
      end

      handler.close
    end

    def get_sensor_id(handler,host_name,device,unit)
      sql = "SELECT sensor_id FROM sensor JOIN host ON sensor.host_id=host.host_id WHERE host.host_name='#{host_name}' AND device='#{device}'" 
      $log.info sql
      result = handler.query(sql).each do |sensor_id|
        sensor_id.each do |key, val|
          return val
        end
      end
      if result.empty?
        host_id = get_host_id(handler,host_name)
        set_sensor(handler, device, host_id, unit)
        get_sensor_id(handler, host_name, device, unit)
      end
    end

    def get_host_id(handler,host_name)
      sql = "SELECT host_id FROM host WHERE host_name='#{host_name}'"
      $log.info sql
      sleep 0.5
      handler.query(sql).each do |host_id|
        host_id.each do |key, val|
          return val
        end
      end
    end

    def set_sensor(handler,device,host_id,unit)
      sql = "INSERT INTO sensor (device, unit, host_id) VALUES ('#{device}', '#{unit}', #{host_id})"
      $log.info sql
      sleep 0.5
      handler.query(sql)
    end

  end
end
