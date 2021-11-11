# A genernic greedy connection pool
#
# It always keeps available connections for instant usage.
# Used connections would be recycled/closed.
# Broken connection would be discarded.
#
# pool = GreedyConnectionPool.new(10, opt) {
#   HTTP.persistent(host, timeout:2147483647).timeout(read: 3, write:2, connection:1)
# }
# pool.with { |http| http.get/put/post }
# # Use http.get/put/post directly.
# # Use http.default_options.to_hash to confirm timeout settings.
# # !! http.timeout(new_value) would lead to a new connection creation.
class GreedyConnectionPool
	attr_reader :name
	attr_accessor :keep_avail_size, :debug, :warn_time
	def initialize(name, keep_avail_size, opt={}, &block)
		@name = name
		@debug = opt[:debug] == true
		@warn_time = opt[:warn_time]
		@_warn_stack = opt[:warn_stack] || 4
		@_conn_create_block = block if block_given?
		@_avail_conn = Concurrent::Array.new
		@_occupied_conn = Concurrent::Array.new
		@keep_avail_size = keep_avail_size
		raise "keep_avail_size should >= 0" unless keep_avail_size >= 0
		thread_name = "#{self.class.name} #{name} maintain"
		@_maintain_thread = Thread.new(abort_on_exception:true) {
			Thread.current[:name] = thread_name
			maintain_loop()
		}
		@_maintain_thread.priority = -3
	end

	def create_conn
		t = Time.now.to_f
		conn = @_conn_create_block.call
		t = (Time.now.to_f - t)*1000
		puts [@name, "Create new conn", t.round(4).to_s.ljust(8), 'ms', status].join(' ')
		conn
	end

	def with(&block)
		return nil if block.nil?
		conn = @_avail_conn.delete_at(0) || create_conn()
		if @_maintain_thread != nil
			# https://ruby-doc.org/core-2.6.5/Thread.html#method-i-status
			if @_maintain_thread.status
				@_maintain_thread.wakeup
			else
				maintain()
			end
		end

		@_occupied_conn.push(conn)

		t = Time.now.to_f
		begin
			ret = block.call(conn)
		rescue HTTP::ConnectionError => e
			puts [@name, "with() http connection error", e.message] if @debug
			@_occupied_conn.delete(conn)
			conn.close
			raise e
		end
		t = (Time.now.to_f - t)
		warn = (@warn_time != nil && @warn_time <= t)
		t *= 1000

		@_occupied_conn.delete(conn)
		@_avail_conn.push(conn)

		if warn
			puts [
				@name, "with()", t.round(4).to_s.ljust(8), 'ms',
				'thr.p', Thread.current.priority,
				status.to_json
			].join(' ').red, level:@_warn_stack
		elsif @debug
			puts [@name, "with()", t.round(4).to_s.ljust(8), 'ms', status]
		end

		ret
	end

	def maintain
		(@keep_avail_size-@_avail_conn.size).times {
			@_avail_conn.push(create_conn())
		}
	end

	def maintain_loop
		loop {
			begin
				next if @_avail_conn.size >= @keep_avail_size
				maintain()
			rescue => e
				APD::Logger.error e
			ensure
				sleep
			end
		}
	end

	def status
		{
			:avail => @_avail_conn.size,
			:using => @_occupied_conn.size
		}
	end
end

# An example.
class GreedyRedisPool < GreedyConnectionPool
	def initialize(keep_avail_size, opt={})
		redis_db = opt[:redis_db] || raise("redis_db should be specified in opt")
		super('redis', keep_avail_size, opt) { 
			# puts "New redis client"
			Redis.new :host => REDIS_HOST, port:REDIS_PORT, db:redis_db, password:REDIS_PSWD, timeout:20.0, connect_timeout:20.0, reconnect_attempts:10
		}
	end
end

# A drop-in proxy for any type of connections
# It would proxy any missing method to a connection warpped in pool.with()
# Example:
# proxy = TransparentGreedyPoolProxy.new( GreedyConnectionPool.new(...) )
# # Proxy could be used as a single redis client.
# proxy.get(...)
# proxy.hset(...)
# proxy.subscribe(...)
class TransparentGreedyPoolProxy
	include LockUtil
	attr_reader :pool
	def initialize(pool)
		@pool = pool
		@dynamic_methods = {}
	end

  def with
    ret = nil
    @pool.with() { |conn|
      ret = yield conn
    }
    ret
  end

	def method_missing(method, *args, &block)
		return unless @dynamic_methods[method].nil?

		puts "TransparentGreedyPoolProxy #{@pool.name} adding #{method}() on-the-fly"
		self.define_singleton_method(method) do |*method_args, &method_block|
			ret = nil
			@pool.with() { |conn| ret = conn.send(method, *method_args, &method_block) }
			ret
		end
		@dynamic_methods[method] = true

		# Call this new method after creating.
		send(method, *args, &block)
	end

	def respond_to?(method)
    return true if method == :with
    ret = false
    @pool.with() { |conn| ret = conn.respond_to?(method) }
    ret
	end
end
