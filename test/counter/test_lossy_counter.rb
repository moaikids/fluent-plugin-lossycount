require 'helper'

class LossyCounterTest < Test::Unit::TestCase
    def setup
        @gamma   = 0.01
        @epsilon = 0.001
        @counter = Counter::LossyCounter.new({:gamma => @gamma, :epsilon => @epsilon})
    end

    def test_add_naive()
        @counter.add('a')
        @counter.add('b')
        @counter.add('c')
        @counter.add('b')
        @counter.add('a')
        @counter.add('b')

        freq_counter = @counter.get()

        p freq_counter
        assert_equal freq_counter.has_key?('a') , true
        assert_equal freq_counter.has_key?('b') , true
        assert_equal freq_counter.has_key?('c') , true
        assert_equal freq_counter.has_key?('d') , false
        assert_equal freq_counter['a'] , 2
        assert_equal freq_counter['b'] , 3
        assert_equal freq_counter['c'] , 1
        assert_equal freq_counter['d'] , nil
    end

    def test_add_and_sweep()
        buf = {}
        radix = [10000000, 1000000, 100000, 10000, 1000, 100, 10]
        l = 100000
        n = (l * radix.size).to_i
        g = (n * @gamma).to_i
        e = (n * @epsilon).to_i
        p 'n : ' + n.to_s
        p 'n x gamma : ' + g.to_s
        p 'n x epsilon : ' + e.to_s

        (0..l).step(1) do |i|
            now = ((Time.now.to_f * 10000000) % 10000000).to_i
            radix = [10000000, 1000000, 100000, 10000, 1000, 100, 10]
            for r in radix
                key = now & r
                @counter.add(key)
                if buf.has_key?(key)
                    buf[key] += 1
                else
                    buf[key] = 1
                end
            end
        end

        freq_counter = @counter.get()
        p "freq couont : " + buf.size().to_s
        p "lossy couont : " + freq_counter.size().to_s
        p "max size : " + @counter.get_current_max_size().to_s

        buf.each_pair { |key, value|
            if value >= g.to_i
                assert_equal freq_counter.has_key?(key), true
                assert_equal value , freq_counter[key]
            else
                if freq_counter.has_key?(key)
                    assert_operator e.to_i , :>= , (freq_counter[key] - value).abs
                end
            end
        }

    end
   
end
