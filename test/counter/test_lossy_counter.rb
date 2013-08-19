require 'helper'

class LossyCounterTest < Test::Unit::TestCase
    def setup
        @gamma   = 0.005
        @epsilon = 0.0045
    end

    def test_add_naive()
        counter = Counter::LossyCounter.new({:gamma => @gamma, :epsilon => @epsilon})
        counter.add('a')
        counter.add('b')
        counter.add('c')
        counter.add('b')
        counter.add('a')
        counter.add('b')

        freq_counter = counter.get()

        p freq_counter
        assert_equal freq_counter.has_key?('a') , true
        assert_equal freq_counter.has_key?('b') , true
        assert_equal freq_counter.has_key?('c') , true
        assert_equal freq_counter.has_key?('d') , false
        assert_equal freq_counter['a'] , 2
        assert_equal freq_counter['b'] , 3
        assert_equal freq_counter['c'] , 1
        assert_equal freq_counter['d'] , nil

        p counter.get_metrics()
    end

    def test_add_and_sweep()
        counter = Counter::LossyCounter.new({:gamma => @gamma, :epsilon => @epsilon})
        buf = {}
        radix = [10000000, 1000000, 100000, 10000, 1000, 100, 10]
        l = 100000

        (0..(l - 1)).step(1) do |i|
            now = ((Time.now.to_f * 10000000) % 10000000).to_i
            for r in radix
                key = now & r
                counter.add(key.to_s)
                if buf.has_key?(key.to_s)
                    buf[key] += 1
                else
                    buf[key] = 1
                end
            end
        end

        n = counter.get_num()
        g = counter.get_num_x_gamma().to_i
        e = (n * @epsilon).to_i
        ge = counter.get_num_x_gamma_d_epsilon().to_i
        freq_count = counter.get()

        p counter.get_metrics()

        buf.each_pair { |key, value|
            if freq_count.has_key?(key)
                if freq_count[key] >= g.to_i
                    assert_equal value , freq_count[key]
                else
                    assert_operator e.to_i , :>= , (freq_count[key] - value).abs
                end
            else 
                assert_operator e.to_i , :>= , value
            end
        }

    end
end
