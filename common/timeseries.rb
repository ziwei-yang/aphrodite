# Example: TimeSeriesBucket.new(10, 6) -> maintain latest 6 buckets for each 10ms
class TimeSeriesBucket
	def initialize(time_unit_ms, units)
		@time_unit_ms = time_unit_ms.to_i
		raise "time_unit_ms #{time_unit_ms} is 0" if @time_unit_ms == 0
		@bucket_num = units
		# Initializing buckets with bucket_num empty ones.
		@buckets = units.times.map { [] }
		@latest_bucket_id = 0
		@latest_bucket = @buckets[@bucket_num-1]
		@latest_bucket_ct = 0
		@useless_bucket = []
	end

	def shift
		@buckets[0].shift
	end

	# Regroup buckets.
	# Append data into last bucket.
	def append(t, data) # t in ms
		id = t.to_i / @time_unit_ms
		# puts ['incoming', t] # Debug
		# Put into current bucket
		if id == @latest_bucket_id
			return if data.nil?
			@latest_bucket_ct += 1
			return(@latest_bucket.push(data))
		end
		# Fill gap between latest_bucket_id and id
		gap = [id-@latest_bucket_id, @bucket_num].min

		# Method 1, no shift() needed
		# push(): 19-20K, is faster than +=, and N.times {}
		if gap == 1
			@buckets.push(@buckets.shift.clear)
		elsif gap == 2
			@buckets.push(@buckets.shift.clear)
			@buckets.push(@buckets.shift.clear)
		else
			gap.times { @buckets.push(@buckets.shift.clear) }
		end
		
		# Method 2, no shift() needed
# 		gap.times {
# 			@buckets.push(@buckets.shift.clear)
# 		}

		# Method 3
# 		if gap == 1
# 			@buckets.push([])
# 		elsif gap == 2
# 			@buckets.push(@useless_bucket)
# 			@buckets.push([])
# 		else
# 			(gap-1).times { @buckets.push(@useless_bucket) }
# 			@buckets.push([])
# 		end
# 		@buckets.shift(gap)

		# @buckets += ([[]] * ([id-@latest_bucket_id, @bucket_num].min)) # 13~17K in backtesting
		# @buckets.concat([[]] * gap) # concat() 17K

		# Put into latest bucket
		@latest_bucket = @buckets[@bucket_num-1]
		@latest_bucket_ct = 0
		if data != nil
			@latest_bucket_ct = 1
			@latest_bucket.push(data)
		end
		@latest_bucket_id = id # Update latest_bucket_id
	end

	def each(&block) # Faster than all_data().each
		@buckets.each { |b| b.each(&block) }
	end

	def each_bucket_top(&block)
		@buckets.each { |b| block.call(b.first) }
	end

	def last_bucket_ct
		@latest_bucket_ct
	end

	def all_ct
		@buckets.map { |b| b.size }.sum
	end

	def all_data
		# @buckets.reduce(:+) # 28K -> 18K
		all = []
		@buckets.each { |d| all.concat(d) } # 28L -> 22K
		all
	end

	def print
		@buckets.reverse.each_with_index do |bkt, i|
			puts "Bucket ID #{@latest_bucket_id - i}"
			bkt.each { |data| puts "\t\t\tdata: #{data}" }
		end
	end
end

class CandleBars
  class << self
    def merge_candles(candles)
      return {} if candles.empty?
      return candles[0] if candles.size == 1
      candles = candles.sort_by { |c| c[:ms] }.reverse # From latest to old.
      {
        :open => candles.last[:open],
        :close=> candles.first[:close],
        :high => candles.map { |c| c[:high] }.max,
        :low  => candles.map { |c| c[:low] }.min,
        :vol  => candles.map { |c| c[:vol] }.sum,
        :ms   => candles.last[:ms]
      }
    end
  end
	def initialize(time_unit_m, max_candles, &stat_lambda) # Time unit in minute
    @time_unit_ms = time_unit_m * 60_000
    @history = [] # From latest to oldest, does NOT include the current candle.
    @max_candles = max_candles
    @current_candles = 0
    @latest_candle = {}
		@latest_bucket_id = nil
    @latest_tick_ms = 0
    @_default_lambda = lambda { |candle, last_candle, data|
      if data.nil?  # Init candle from last_candle
        candle[:open] = candle[:high] = candle[:low] = candle[:close] = last_candle[:close]
        candle[:vol] = 0
      else # Update candle from tick data: { 'p'=>, 's'=> }
        last = candle[:close] = data['p']
        candle[:open] ||= last
        candle[:high] ||= last
        candle[:low] ||= last
        candle[:vol] ||= 0
        candle[:high] = last if last > candle[:high]
        candle[:low] = last if last < candle[:low]
        candle[:vol] += data['s']
      end
    }
    if block_given?
      @stat_lambda = stat_lambda
    else
      @stat_lambda = @_default_lambda
    end
  end

  def to_json(state={}) # Compatiable with JSON lib
    [
      @time_unit_ms,
      @history,
      @max_candles,
      @current_candles,
      @latest_candle,
      @latest_bucket_id,
      @latest_tick_ms
    ].to_json(state)
  end

  def restore_from_json(j)
    @time_unit_ms, @history, @max_candles, @current_candles, @latest_candle, @latest_bucket_id, @latest_tick_ms = j
    @latest_candle.keys.each { |k|
      @latest_candle[k.to_sym] = @latest_candle.delete(k)
    }
    @history.each { |h|
      h.keys.each { |k| h[k.to_sym] = h.delete(k) }
    }
  end

	def append(t, data) # t in ms, discard non-latest data
    return if @latest_tick_ms > t # Could not distinct same trades here.
    # puts "APPEND #{data}"
    @latest_tick_ms = t
		id = t.to_i / @time_unit_ms
    @latest_bucket_id ||= id
		if id == @latest_bucket_id
      @stat_lambda.call(@latest_candle, nil, data) # Modify latest_candle only.
    else
      gap = id - @latest_bucket_id
      last_candle = @latest_candle
      #  puts "INSERT #{gap} candles"
      gap.times { @history.unshift(last_candle) } # Clone candle for missing timerange, put at first
      @current_candles += gap
      @history = @history[0..(@max_candles-1)] if @current_candles > @max_candles
      @latest_bucket_id = id

      @latest_candle = {}
      @stat_lambda.call(@latest_candle, last_candle, nil) # Init latest_candle
    end
    @latest_candle[:ms] = id * @time_unit_ms
  end

  # Trades: [{ 't'=>, 'p'=>, 's'=> }...]
  def on_trades(trades)
    return if trades.empty?
    trades.each { |t|
      time = t['t'].to_i
      # Discard when trades time equals last tick ms. Update at last to record all trades at same time.
      next if @latest_tick_ms >= time
      append(time, t)
    }
    @latest_tick_ms = [@latest_tick_ms, trades.map { |t| t['t'].to_i }.max].max
  end

  def latest(n=1)
    return @latest_candle if n <= 1
    return [@latest_candle] + @history[0..(n-1)]
  end
end
