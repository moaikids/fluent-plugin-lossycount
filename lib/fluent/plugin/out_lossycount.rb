module Fluent
  class LossyCountOutput < BufferedOutput
    Fluent::Plugin.register_output('lossycount', self)
    attr_reader :size, :key_name

    def initialize
      super
      require 'msgpack'
    end

    def configure(conf)
      super

      @size = conf.has_key?('size') ? conf['size'] : -1
      @key_name = conf.has_key?('key_name') ? conf['key_name'] : nil
    end

    def start
      super

      @redis = Redis.new(:host => @host, :port => @port, :timeout => @timeout,
                         :thread_safe => true, :db => @db_number)
    end

    def shutdown
      @redis.quit
    end

    def format(tag, time, record)
      identifier = [tag, time].join(".")
      [identifier, record].to_msgpack
    end

    def write(chunk)
      @redis.pipelined {
        chunk.open { |io|
          begin
            MessagePack::Unpacker.new(io).each { |message|
              begin
                (tag, record) = message
                if @store_type == 'zset'
                  operation_for_zset(record)
                elsif @store_type == 'set'
                  operation_for_set(record)
                elsif @store_type == 'list'
                  operation_for_list(record)
                elsif @store_type == 'string'
                  operation_for_string(record)
                end
              rescue NoMethodError => e
                puts e
              end
            }
          rescue EOFError
            # EOFError always occured when reached end of chunk.
          end
        }
      }
    end

    def operation_for_zset(record)
      now = Time.now.to_i
      k = traverse(record, @key_name).to_s
      if @score_name
        s = traverse(record, @score_name)
      else
        s = now
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
      
      @redis.multi do
        @redis.zadd sk , s, v
        if @key_expire > 0
          @redis.expire sk , @key_expire
        end
        if @value_expire > 0
          @redis.zremrangebyscore sk , '-inf' , (now - @value_expire)
        end
      end
      if @value_length > 0
        script = generate_zremrangebyrank_script(sk, @value_length)
        @redis.eval script
      end
    end

    def operation_for_set(record)
      k = traverse(record, @key_name).to_s
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
              
      @redis.sadd sk, v
      if @key_expire > 0
        @redis.expire sk, @key_expire
      end
    end

    def operation_for_list(record)
      k = traverse(record, @key_name).to_s
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix

      @redis.multi do
        if @list_order == 'asc'
          @redis.rpush sk, v
        else
          @redis.lpush sk, v
        end             
        if @key_expire > 0
          @redis.expire sk, @key_expire
        end
      end
      if @value_length > 0
        script = generate_ltrim_script(sk, @value_length, @list_order)
        @redis.eval script
      end 
    end

    def operation_for_string(record)
      k = traverse(record, @key_name).to_s
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
         
      @redis.multi do        
        @redis.set sk, v
        if @key_expire > 0
          @redis.expire sk, @key_expire
        end
      end
    end

    def generate_zremrangebyrank_script(key, maxlen)
      script  = "local key = '" + key.to_s + "'\n"
      script += "local maxlen = " + maxlen.to_s + "\n"
      script += "local len = tonumber(redis.call('ZCOUNT', key, '-inf', '+inf'))\n"
      script += "if len > maxlen then\n"
      script += "    local l = len - maxlen\n"
      script += "    if l >= 0 then\n"
      script += "        return redis.call('ZREMRANGEBYRANK', key, 0, l)\n"
      script += "    end\n"
      script += "end\n"
      return script
    end

    def generate_ltrim_script(key, maxlen, order)
      script  = "local key = '" + key.to_s + "'\n"
      script += "local maxlen = " + maxlen.to_s + "\n"
      script += "local order ='" + order.to_s + "'\n"
      script += "local len = tonumber(redis.call('LLEN', key))\n"
      script += "if len > maxlen then\n"
      script += "    if order == 'asc' then\n"
      script += "        local l = len - maxlen\n"
      script += "        return redis.call('LTRIM', key, l, -1)\n"
      script += "    else\n"
      script += "        return redis.call('LTRIM', key, 0, maxlen - 1)\n"
      script += "    end\n"
      script += "end\n"
      return script
    end

    def traverse(data, key)
      val = data
      key.split('.').each{ |k|
        if val.has_key?(k)
          val = val[k]
        else
          return nil
        end
      }
      return val
    end
  end
end
