# -*- coding: utf-8 -*-

module Counter
    class LossyCounter
        def initialize(config)
            @gamma = config.has_key?(:gamma) ? config[:gamma].to_f : 0.005
            @epsilon = config.has_key?(:epsilon) ? config[:epsilon].to_f : 0.001
            @current = 1
            @freq_counter = {}
            @delta_counter = {}
            @num = 0
            @max_size = -1
        end

        def add(key)
            if @freq_counter.has_key?(key)
                @freq_counter[key] += 1
            else
                @freq_counter[key] = 1
                @delta_counter[key] = @current - 1
            end
            @num += 1
            if @num % (1 / @epsilon).to_i == 0
                sweep()
            end
        end

        def sweep()
            length = @freq_counter.length
            if @max_size < length
                @max_size = length
            end

            @freq_counter.each_pair { |key, value|
                if value <= (@current - @delta_counter[key])
                    @freq_counter.delete(key)
                    @delta_counter.delete(key)
                end
            }
            @current += 1
        end

        def get()
            buf = {}
            @freq_counter.each_pair { |key, value|
                if value > (@num * (@gamma - @epsilon) ).to_i
                    buf[key] = value
                end
            }
            return buf
        end
        
        def get_num()
            return @num
        end 

        def get_current_max_size()
            return @max_size
        end

        def get_metrics()
            return {'num' => get_num(), 'max_size' => get_current_max_size(), 'current_size' => @freq_counter.size() , 'reduced_size' => get().size(), "gamma" => @gamma , "epsilon" => @epsilon , "n_x_gamma" => (@num.to_f * @gamma), "n_x_gamma-epsilon" => (@num.to_f * (@gamma - @epsilon)) }
        end

    end
end
