#################################################
# Meta-programming utils placed first.
#################################################
module LockUtil
	def self.included(clazz)
		super
		# add instance methods: _apd_method_locs, _apd_method_lock_get
		clazz.class_eval do
			define_method(:_apd_method_locs) { instance_variable_get :@_apd_method_locs }
			define_method(:_apd_method_lock_get) { |m| send("_apd_get_lock_for_#{m}".to_sym) }
		end
		# add feature DSL 'thread_safe'
		clazz.singleton_class.class_eval do
			define_method(:thread_safe) do |*methods|
				methods.each do |method|
					method = method.to_sym
					# method -> method_without_threadsafe
					old_method_sym = "#{method}_without_threadsafe".to_sym
					if clazz.method_defined? old_method_sym
# 					puts "#{clazz}: #{old_method_sym} alread exists, skip wrapping".red
					else
						alias_method old_method_sym, method
# 					puts "#{clazz}: #{method} -> #{old_method_sym}".red
					end
					clazz.class_eval do
						# add instance methods: _apd_get_lock_for_(method_name)
						# All target methods share one mutex.
						define_method("_apd_get_lock_for_#{method}".to_sym) do
							instance_variable_set(:@_apd_method_locs, {}) if _apd_method_locs.nil?
							return _apd_method_locs[method] unless _apd_method_locs[method].nil?
							# Init mutex for all methods.
							mutex = Mutex.new
							methods.each { |m| _apd_method_locs[m] = mutex }
							mutex
						end
						# Wrap old method with lock.
						define_method(method) do |*args, &block|
							ret = nil
# 							puts "#{clazz}\##{self.object_id} call thread_safe method #{method}".red
							_apd_method_lock_get(method).synchronize do
# 								puts "#{clazz}\##{self.object_id} call internal method #{old_method_sym}".blue
								ret = send old_method_sym, *args, &block
							end
# 							puts "#{clazz}\##{self.object_id} end thread_safe method #{method}".green
							ret
						end
					end
				end
			end
		end
	end
end

class ExpireResultFailed < Exception
end
module ExpireResult
	# Add feature DSL: expire_every T, query_method
	# The result will be stored and expired every T seconds.
	def self.included(clazz)
		super
		clazz.singleton_class.class_eval do
			define_method(:expire_every) do |*args|
				raise "DSL expire_every usage: T(seconds), method_name" unless args.size == 2
				expire_t = args[0].to_f
				method = args[1].to_sym
				raise "DSL expire_every usage: T(seconds), method_name" unless expire_t > 0
				old_method_sym = "__#{method}_without_expire_warpping".to_sym
				if clazz.method_defined? old_method_sym
#  					puts "#{clazz}: #{old_method_sym} alread exists, skip wrapping".red
				else
					alias_method old_method_sym, method
#  					puts "#{clazz}: #{method} -> #{old_method_sym}".red
				end
				clazz.class_eval do
					# Overwrite instance method:
					# 	if @__method_result_time[args] not expired
					# 		return @__method_result[args]
					# 	ret = __method__without_expire_wrapping()
					#   @__method_result[args] = ret
					#   @__method_result_time[args] = now
					define_method(method) do |*m_args, &block|
						r_key = "@__#{method}_result".gsub('?', '_qmark_').to_sym
						t_key = "@__#{method}_result_time".gsub('?', '_qmark_').to_sym
						# m_args will never be nil, it would be [] if no argument given.
						# But this might be changed in side old_method_sym(), so make a shallow clone first.
						args_key = m_args.clone
						# Initial result records.
						instance_variable_set(r_key, {}) if instance_variable_get(r_key).nil?
						instance_variable_set(t_key, {}) if instance_variable_get(t_key).nil?
						last_time = instance_variable_get(t_key)[args_key]
						last_result = instance_variable_get(r_key)[args_key]
						now = Time.now.to_i
						if last_time != nil && now - last_time < expire_t
							return last_result
						end
						ret = nil
						begin
							ret = send old_method_sym, *m_args, &block
						rescue ExpireResultFailed => e
							puts "ExpireResultFailed, use last result: #{method}(#{args_key})"
							return last_result
						end
						instance_variable_get(r_key)[args_key] = ret
						instance_variable_get(t_key)[args_key] = Time.now.to_i
						# puts "Cache #{method} result for #{args_key} in #{expire_t} seconds."
						return ret
					end
				end
			end
		end
	end
end

module CacheUtil
	def cache_client
		redis
	end

	# Should add a redis_db function here.
	def redis
		@@redis ||= Redis.new :host => REDIS_HOST, port:REDIS_PORT, db:redis_db, password:REDIS_PSWD, timeout:20.0, connect_timeout:20.0, reconnect_attempts:10
	end

	def redis_new
		Redis.new :host => REDIS_HOST, port:REDIS_PORT, db:redis_db, password:REDIS_PSWD, timeout:20.0, connect_timeout:20.0, reconnect_attempts:10
	end

	def clear_redis_by_prefix(prefix)
		return if prefix.nil?
		cmd = "local keys = redis.call('keys', ARGV[1]) for i=1,#keys,5000 do redis.call('del', unpack(keys, i, math.min(i+4999, #keys))) end return keys";
		Logger.debug "Clearing redis by prefix:[#{prefix}]"
		redis.eval(cmd, [], ["#{prefix}*"])
	end

	def clear_redis_by_table(table)
		clear_redis_by_prefix "SQL_BUFFER:#{table}:"
	end

	def clear_redis_by_path(prefix)
		cmd = "local keys = redis.call('keys', ARGV[1]) for i=1,#keys,5000 do redis.call('del', unpack(keys, i, math.min(i+4999, #keys))) end return keys";
		redis.eval(cmd, [], ["#{prefix}:*"])
	end

	def redis_lock_manager
		@@redis_lock_manager ||= Redlock::Client.new([redis()])
	end
end

module Cacheable
	include CacheUtil

	def self.included(clazz)
		super
		# add feature DSL 'cache_by'
		clazz.singleton_class.class_eval do
			define_method(:cache_by) do |*args|
				raise "cache_by need a basename and a key array." if args.size <= 1
				raise "cache_by need a basename and a key array." unless args[1].is_a?(Array)
				opt = args[2] || {}
				type_sorted_set = opt[:type] == :sorted_set
				value_method = opt[:value] || :to_json
				# Find an avaiable method name slot.
				cache_method_key,	cache_method, decache_method = '@__cacheable_key', '__cacheable_cache', '__cacheable_decache'
				index = 0
				while true
					break unless clazz.method_defined?("#{cache_method}_#{index}".to_sym)
					index += 1
				end
				# Define method (de)cacheable_cache_method_#NUM
				target_cache_method_sym = "#{cache_method}_#{index}".to_sym
				target_decache_method_sym = "#{decache_method}_#{index}".to_sym
				clazz.class_eval do
					define_method(target_cache_method_sym) do
						# Compute K and V.
						cache_key = args[0].to_s
						keys = args[1]
						middle_keys, score = keys, 0
						if type_sorted_set
							# Last key is score of sorted set.
							last_key, middle_keys = keys[-1], keys[0..-2]
							score = send(last_key).to_i
						end
						middle_keys.each { |k| cache_key = "#{cache_key}:#{send(k).to_s}" }
						# Remove old data if key changed.
						old_kv = instance_variable_get "#{cache_method_key}_#{index}".to_sym
						if old_kv != nil && old_kv[0] != cache_key
							if type_sorted_set
								cache_client.zrem old_kv[0], old_kv[1]
							else
								cache_client.del old_kv[0]
							end
						end
						value = send value_method
						instance_variable_set "#{cache_method_key}_#{index}".to_sym, [cache_key, value, score]
						# Put data.
						if type_sorted_set
							# puts "Cache into #{cache_key}, score #{send(last_key)} #{score}\n#{value}"
							cache_client.zadd cache_key, score, value
						else
							cache_client.set cache_key, value
						end
					end
					define_method(target_decache_method_sym) do
						# ZREM old data.
						old_kv = instance_variable_get "#{cache_method_key}_#{index}".to_sym
						if old_kv != nil
							if type_sorted_set
								# [cache_key, value, score]
								cache_client.zrem old_kv[0], old_kv[2]
							else
								cache_client.del old_kv[0]
							end
						end
					end
					define_method(:cache) do
						index = 0
						# Combine all cache methods.
						while true
							method_sym = "#{cache_method}_#{index}".to_sym
							break unless clazz.method_defined?(method_sym)
							send method_sym
							index += 1
						end
					end
					define_method(:decache) do
						index = 0
						# Combine all decache methods.
						while true
							method_sym = "#{decache_method}_#{index}".to_sym
							break unless clazz.method_defined?(method_sym)
							send method_sym
							index += 1
						end
					end
				end
			end
		end
	end
end

#################################################
# Utility modules below
#################################################

module FileUtil
	def tail(file, opt={})
		verbose = opt[:verbose] == true
		sleep_interval = opt[:interval] || 0.1
	
		f = nil
		begin
			f = File.open(file,"r")
			# seek to the end of the most recent entry
	# 		f.seek(0,IO::SEEK_END)
		rescue Errno::ENOENT
			sleep sleep_interval
			retry
		end
	
		ct = 0
		loop do
			select([f])
			line = f.gets
			if line.nil? || line.size == 0
				sleep sleep_interval
				next
			end
			puts "#{ct.to_s.ljust(5)}: #{line}" if verbose
			if block_given?
				ret = yield line.strip
				break if ret == false
			end
			ct += 1
		end
	end
end

module SleepUtil
	def graphic_sleep(time)
		maxSleepCount = time
		sleepCount = 0
		statusBarLength = 70
		step = 1
		step = 0.1 if time < 60
		while sleepCount < maxSleepCount
			elapsedLength = statusBarLength * sleepCount / maxSleepCount
			remainedLength = statusBarLength - elapsedLength
			statusStr = "|#{'=' * elapsedLength}>#{'.' * remainedLength}"
			print "\rSleep #{(maxSleepCount - sleepCount).to_i.to_s.ljust(10)}#{statusStr}"
			sleep step
			sleepCount += step
		end
		print "\r#{' '.ljust('Sleep '.length + 10 + statusBarLength + 2)}\r"
	end
end

module CycleWorker
	def _cycle_init(opt={})
		@cycle_roundtime = 60
		@cycle_roundtime = opt[:roundtime] unless opt[:roundtime].nil?
		@cycle_roundct = 0
		cycle_init(opt)
	end

	def cycle_endless(opt={})
		_cycle_init if @cycle_roundtime.nil?
		verbose = opt[:verbose]
		while true
			@cycle_roundct += 1
			Logger.debug "CycleWorker round##{@cycle_roundct} start." if verbose
			start_t = Time.now
			cycle_work
			end_t = Time.now
			Logger.debug "CycleWorker round##{@cycle_roundct} finished." if verbose
			sleep_time = @cycle_roundtime - (end_t - start_t)
			sleep sleep_time if sleep_time > 0
		end
	end

	def cycle_init(opt={}); end
	def cycle_work; end
end

module ProfilingUtil
	def profile_on
		@_apd_profiling_status = true
	end

	def profile_off
		@_apd_profiling_status = false
		profile_record_clear()
	end

	def profile_timing(name)
		if @_apd_profiling_status != true
			return yield() if block_given?
			return
		end
		start_time = Time.new.to_f
		ret = yield() if block_given?
		end_time = Time.new.to_f
		elapsed_s = (end_time - start_time)
		Logger.log "timing #{name}: #{elapsed_s}"
		ret
	end

	def profile_record(name)
		if @_apd_profiling_status != true
			return yield() if block_given?
			return
		end
		name = name.to_s
		@_apd_profiling_data ||= {}
		@_apd_profiling_data[name] ||= 0
		start_time = Time.new.to_f
		ret = yield() if block_given?
		end_time = Time.new.to_f
		elapsed_s = (end_time - start_time)
		if @_apd_profiling_data[name] != nil # In case of cache cleared.
			@_apd_profiling_data[name] += elapsed_s
		end
		ret
	end

	def profile_record_clear
		@_apd_profiling_data = {}
		@_apd_profiling_cache = {}
	end

	def profile_record_start(name)
		return if @_apd_profiling_status != true
		name = name.to_s
		@_apd_profiling_cache ||= {}
		@_apd_profiling_cache[name] ||= Time.new.to_f
	end

	def profile_record_end(name, opt={})
		return if @_apd_profiling_status != true
		name = name.to_s
		@_apd_profiling_data ||= {}
		@_apd_profiling_cache ||= {}
		if opt[:prefix] == true
			end_time = Time.new.to_f
			@_apd_profiling_cache.keys.each do |k|
				next unless k.start_with?(name)
				start_time = @_apd_profiling_cache.delete(k)
				next if start_time.nil?
				elapsed_s = (end_time - start_time)
				@_apd_profiling_data[k] ||= 0
				@_apd_profiling_data[k] += elapsed_s
			end
			return
		end
		start_time = @_apd_profiling_cache.delete(name)
		if opt[:allow_null] == false
			raise "Profiling #{name} is not started yet" if start_time.nil?
		else
			return if start_time.nil?
		end
		end_time = Time.new.to_f
		elapsed_s = (end_time - start_time)
		@_apd_profiling_data[name] ||= 0
		@_apd_profiling_data[name] += elapsed_s
	end

	def profile_print
		@_apd_profiling_data ||= {}
		data = @_apd_profiling_data.to_a
		puts "---- #{data.size} profiling entries ----"
		data.sort_by { |kv| kv[1] }.reverse.each do |name, time|
			puts "#{name.ljust(32)} #{time.round(3)}"
		end
	end
end

module LogicControl
	def endless_retry(opt={}, &block)
		opt_c = opt.clone
		opt_c[:retry_ct] = -1
		opt_c[:log_level] = 4
		limit_retry(opt_c, &block)
	end
	def no_complain(opt={})
		begin
			return yield()
		rescue => e
			Logger.error e if opt[:silent] != true
			return nil
		end
	end
	def limit_retry(opt={})
		max_ct = opt[:retry_ct] || 3
		sleep_s = opt[:sleep] || 0
		log_level = opt[:log_level] || 3
		ct = 0
		begin
			return yield(ct)
		rescue StandardError => e
			ct += 1
			# Exception has a number of subclasses; some are recoverable 
			# while others are not. All recoverable errors inherit from the 
			# StandardError class, which itself inherits directly from Exception.
			raise e if max_ct == 0
			raise e if max_ct > 0 && ct >= max_ct
			puts "#{e.class} #{e.message}\nRetry #{ct}/#{max_ct} after sleep #{sleep_s}s", level:log_level
			sleep(sleep_s) if sleep_s > 0
			retry
		end
	end
  # Keep sleeping even always waked up by other threads.
  def keep_sleep(seconds)
    until_t = Time.now.to_f + seconds
    loop {
      remained_t = until_t - Time.now.to_f
      break if remained_t <= 0
      sleep remained_t # Always waked up by other threads.
    }
  end
end

module CLI
  def terminal_width
    # IO.console.winsize
    # io-console does not support JRuby
    GLI::Terminal.new.size[0] || 80
  end
  def terminal_height
    GLI::Terminal.new.size[1] || 24
  end

  def get_input(opt={})
    puts(opt[:prompt].white.on_black, level:2) unless opt[:prompt].nil?
    timeout = opt[:timeout]
    if timeout.nil?
      r = STDIN.gets
			return nil if r.nil?
      return r.chomp
    elsif timeout == 0
      return 'Y'
    else
      ret = nil
      begin
        Timeout::timeout(timeout) do
          ret = STDIN.gets.chomp
        end
      rescue Timeout::Error
        ret = nil
      end
      return ret
    end
  end

	def progressive_string(text, progress, width, opt={})
		text = text.uncolorize
		progress = 1 if progress > 1
		progress = 0 if progress < 0
		width = width.to_i
		raise "width #{width} should > 0" unless width > 0
		score_width = (width*progress.to_f).to_i
		score_width = [width, score_width].min
		if opt[:side] != 'right'
			# text = 0123456, progress = 0.4, width = 10
			# text1 = 0123, text2 = '456----'
			text_1 = '' if score_width == 0
			text_1 = text.ljust(width)[0..(score_width-1)] if score_width > 0
			text_2 = text.ljust(width)[score_width..-1]
		else
			# text = 0123456, progress = 0.4, width = 10
			# text1 = 3456, text2 = '----012'
			text_1 = '' if score_width == 0
			text_1 = text.rjust(width)[-score_width..-1] if score_width > 0
			text_2 = text.rjust(width)[0..(width-score_width-1)]
			text_2 = '' if score_width >= width
		end
		text_2 = text_2.gsub('  ', '░░').gsub(/\s$/, '░').gsub(/░\s/, '░░').gsub(/\s░/, '░░')
		if opt[:color].nil? # Adaptive color
			if score_width.to_f/width < 0.50
				text = [text_1.light_white.on_green, text_2.green]
			elsif score_width.to_f/width < 0.75
				text = [text_1.light_white.on_yellow, text_2.yellow]
			else
				text = [text_1.light_white.on_red, text_2.red]
			end
		else
			color = opt[:color].to_sym
			if color == :default || color == :uncolorize
				text = [text_1, text_2]
			else
				on_color = "on_#{opt[:color]}".to_sym
				text = [text_1.light_white.send(on_color), text_2.send(color)]
			end
		end
		text = text.reverse if opt[:side] == 'right'
		return text.join
	end
end

module ExecUtil
	def exec_command(command, opt={})
		log_prefix = opt[:log_prefix] || ''
		use_thread = opt[:thread] == true
		verbose = opt[:verbose] == true
		quiet = (opt[:quiet] == true || opt[:silent] == true)
		verbose = false if quiet
		status = opt[:status] || {}
		status['output'] ||= []
		status_lambda = opt[:status_cb] || lambda {|l| }
		read, io = IO.pipe

		Logger.info "Exec: #{command}" unless quiet

		# Start a new thread to execute command while collecting realtime logs.
		logthread = Thread.new do
			begin
				Logger.debug "CMD #{log_prefix} Log thread started." if verbose
				line_ct = 0
				read.each_line do |line|
					line = line[0..-2]
					line_ct += 1
					Logger.debug "CMD #{log_prefix} Log: #{line}" if verbose
					status['progress'] = "CMD #{log_prefix} \##{line_ct}: #{line}"
					status['output'].push line
					status_lambda.call(status)
				end
			rescue => e
				Logger.info "CMD #{log_prefix} Log: error occurred:" unless quiet
				Logger.error e
			end
			Logger.debug "CMD #{log_prefix} Log thread end." if verbose
		end

		exec_lambda = lambda do
			begin
				Logger.info "CMD #{log_prefix} thread started." if use_thread
				ret = system(command, out:io, err:io)
				status['ret'] = ret
				io.close
			rescue => e
				Logger.info "CMD #{log_prefix} error occurred:" unless quiet
				Logger.error e
				status['error'] = e.message
			end
			status['exit'] = true
			Logger.info "CMD #{log_prefix} thread end." if use_thread
			status_lambda.call(status)
		end

		if use_thread
			t = Thread.new { exec_lambda.call }
			return t
		else
			exec_lambda.call
			logthread.join
			return status
		end
	end
end
