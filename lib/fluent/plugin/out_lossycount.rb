require_relative '../../counter/lossy_counter'

class Fluent::LossyCountOutput < Fluent::Output
    Fluent::Plugin.register_output('lossycount', self)

    config_param :key_name, :string, :default => nil
    config_param :time_windows, :time, :default => 60
    config_param :output_tag, :string, :default => nil
    config_param :output_key_name, :string, :default => 'key'
    config_param :output_timestamp_name, :string, :default => 'timestamp'
    config_param :output_value_name, :string, :default => 'value'
    config_param :gamma, :float, :default => 0.005
    config_param :epsilon, :float, :default => 0.004
    config_param :enable_metrics, :bool, :default => false
    config_param :metrics_tag, :string, :default => 'counter.metrics'
    config_param :verbose, :bool, :default => false

    def configure(config)
        super
        unless config.has_key?('key_name')
            raise Fluent::ConfigError, "you must set 'key_name'"
        end
        unless config.has_key?('output_tag')
            raise Fluent::ConfigError, "you must set 'output_tag'"
        end

        @mutex = Mutex.new
        @sleep_wait = 0.4
        init_counter()
    end

    def start
        super
        start_watch
    end

    def shutdown
        super
        @watcher.terminate
        @watcher.join
    end

    def init_counter()
        @counter = Counter::LossyCounter.new({:gamma => @gamma, :epsilon => @epsilon})
    end

    def flush_counter(step)
        freq_counter = {}
        metrics = {}
        @mutex.synchronize {
            freq_counter = @counter.get()
            if @enable_metrics
                metrics = @counter.get_metrics()
            end 
            init_counter()
        }
        flush_time = Fluent::Engine.now.to_i
        if @verbose
            $log.info "flushtime : " + flush_time.to_s
            $log.info "{ "
        end
        freq_counter.each_pair { |key, value|
            map = {@output_key_name => key, @output_timestamp_name => flush_time, @output_value_name => value}
            if @verbose
                $log.info map
            end
            Fluent::Engine.emit(@output_tag, Fluent::Engine.now, map)
        }
        if @verbose
            $log.info "}"
        end

        if @enable_metrics
            if @verbose
                $log.info "metrics : " + metrics.to_s
            end
            Fluent::Engine.emit(@metrics_tag, Fluent::Engine.now, metrics)
        end
    end

    def start_watch
        @watcher = Thread.new{
            @last_checked = Fluent::Engine.now.to_i
            while true
                sleep @sleep_wait
                now = Fluent::Engine.now.to_i
                if now - @last_checked >= @time_windows
                    flush_counter(now - @last_checked)
                    @last_checked = now
                end
            end
        }
    end

    def emit(tag, es, chain)
        es.each {|time,record|
            k = traverse(record, @key_name)
            if k
                if k.is_a?(Array)
                    k.each{ |v|
                        @counter.add(v.to_s)
                    }
                else
                    @counter.add(k.to_s)
                end
            end
        }
        chain.next
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
