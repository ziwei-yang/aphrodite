require 'concurrent'
class Logger
	class << self
		def exception_desc(e)
			return "[#{e.class.name}]\nMSG:[#{e.message}]\n#{stacktrace(e.backtrace)}"
		end
	
		def debug(str)
			str = exception_desc(str) if str.is_a?(Exception)
			log_int str
		end
	
		def log(str, additional_stack=0, opt={})
			str = exception_desc(str) if str.is_a?(Exception)
			log_int str, additional_stack, opt
		end
	
		def info(str)
			str = exception_desc(str) if str.is_a?(Exception)
			log_int str, nil, color: :blue
		end
	
		def highlight(str)
			str = exception_desc(str) if str.is_a?(Exception)
			log_int str, nil, color: :red
		end
	
		def warn(str)
			if str.is_a?(Exception)
				log_int exception_desc(str), nil, color: :light_magenta
			else
				log_int str, nil, color: :light_magenta
			end
		end
	
		def error(str)
			if str.is_a?(Exception)
				log_int exception_desc(str), nil, color: :light_red
			else
				log_int (str.to_s + "\n" +  stacktrace(caller)), nil, color: :light_red
			end
		end
	
		def fatal(str)
			if str.is_a?(Exception)
				log_int exception_desc(str), nil, color: :red
			else
				log_int (str.to_s + "\n" +  stacktrace(caller)), nil, color: :red
			end
		end
		@@_apd_logger_max_head_len = 0
		@@_apd_logger_async_tasks = Concurrent::Array.new
		@@_apd_logger_file = nil
		@@_apd_logger_file_writer = nil
		def async?
			@@_apd_logger_file != nil
		end
		def global_output_file
			@@_apd_logger_file
		end
		def global_output_file=(f)
			raise "Logger:global_output_file exists #{@@_apd_logger_file}" unless @@_apd_logger_file.nil?
			print "Logger:global_output_file -> #{f}\n"
			print "Logger: Async mode ON\n"
			self.singleton_class.class_eval { alias_method :log_int, :log_int_async }
			@@_apd_logger_file = f
			@@_apd_logger_file_writer = File.open(f, 'a')
			@@_apd_logger_file_w_thread = Thread.new(abort_on_exception:true) {
				Thread.current[:name] = "APD::Logger async worker"
				fputs_ct = 0
				loop {
					begin
						sleep() # Wait for _log_async() is called
						print_msg = []
						flush_msg = []
						loop { # Once waked up, process all tasks in batch
							task = @@_apd_logger_async_tasks.delete_at(0)
							break if task.nil?
							head, msg, opt = task
							head = head.split(":in")[0].split('/').last.gsub('.rb', '')
							head = ".#{head[-11..-1]}" if head.size >= 12
							head = "#{opt[:time].strftime("%m/%d-%H:%M:%S.%4N")} #{head} "
							@@_apd_logger_max_head_len = [head.size, @@_apd_logger_max_head_len].max
							unless opt[:nohead]
								msg = "#{head.ljust(@@_apd_logger_max_head_len)}#{msg}"
							else
								msg = msg.to_s
							end
							msg = msg.send(opt[:color]) unless opt[:color].nil?

							msg_t = nil
							if opt[:nohead] != true && opt[:t_name] != nil
								name = opt[:t_name]
								priority = opt[:t_priority].to_s.rjust(2)
								msg_t = "\r#{head.ljust(@@_apd_logger_max_head_len)}#{priority} #{name}"
								print_msg.push((msg_t+"\n").yellow)
							end

							print_msg.push(opt[:inline] ? "\r#{msg}" : "\r#{msg}\n")
							if opt[:nofile] != true && @@_apd_logger_file_writer != nil
								fputs_ct += 1
								flush_msg.push(msg_t.yellow) if msg_t != nil
								flush_msg.push(msg)
							end
						}
						# Print & flush together
						if fputs_ct > 0
							flush_msg.each { |m| @@_apd_logger_file_writer.puts(m) }
							@@_apd_logger_file_writer.flush
						end
						print print_msg.join
					rescue => e
						print "#{e.to_s}\n"
						e.backtrace.each { |s| print "#{s}\n" }
					end
				}
			}
			print "Logger: @@_apd_logger_file_w_thread is running\n"
			@@_apd_logger_file_w_thread.priority = -3
		end
	
		private
	
		def stacktrace(backtrace)
			info = "StackTrace:\n"
			backtrace.each { |line| info += line + "\n" }
			return info
		end

		# This does not work in Parallel.each(in_processes)
		# Should set to sync mode to make it work.
		def log_int_async(o, additional_stack=0, opt={})
			if @@_apd_logger_file.nil?
				self.singleton_class.class_eval { alias_method :log_int, :log_int_sync }
				return log_int_sync(o, additional_stack, opt)
			end
			opt[:time] ||= Time.now
			if Thread.current != Thread.main
				opt[:t_priority] = Thread.current.priority
				opt[:t_name] = Thread.current[:name]
			end
			additional_stack ||= 0
			head = caller(2 + additional_stack).first
			@@_apd_logger_async_tasks.push([head, o, opt])
			@@_apd_logger_file_w_thread.wakeup unless @@_apd_logger_file_w_thread.nil?
		end

		def log_int_sync(o, additional_stack=0, opt={})
			additional_stack ||= 0
			o = o.to_s
			head = caller(2 + additional_stack).first.split(":in")[0]
			head = head.split('/').last.gsub('.rb', '')
			head = ".#{head[-11..-1]}" if head.size >= 12
			if opt[:time].nil?
				head = "#{Time.now.strftime("%m/%d-%H:%M:%S.%4N")} #{head} "
			else
				head = "#{opt[:time].strftime("%m/%d-%H:%M:%S.%4N")} #{head} "
			end
			@@_apd_logger_max_head_len = head.size if head.size > @@_apd_logger_max_head_len

			if opt[:nohead]
				msg = "\r#{o}"
			else
				msg = "\r#{head.ljust(@@_apd_logger_max_head_len)}#{o}"
			end
			msg << "\n" if opt[:inline] != true

			# Add thread head.
			if Thread.current != Thread.main && opt[:nohead] != true
				t = Thread.current
				name = t[:name] || t.to_s
				priority = t.priority.to_s.rjust(2)
				msg_t = "\r#{head.ljust(@@_apd_logger_max_head_len)}#{priority} #{name}\n"
				print msg_t.yellow
			end

			begin
				msg = msg.send(opt[:color]) unless opt[:color].nil?
			rescue => e
				msg << "\nBTX::Logger: Failed to set color #{opt[:color]}, error:#{e.message}\n"
			end
			print msg
		end

		alias_method :log_int, :log_int_sync
	end
end

# Override default puts with additional info.
Kernel.module_eval do
	alias :original_puts :puts
	# Use original puts in below cases:
	# puts msg, false
	# puts msg, info:false
	def puts(o, opt={})
		opt = { :info => false } if opt == false
		return original_puts(o) if (opt[:info] == false)
		level = opt[:level] || 1
		Logger.log(o, level, opt)
	end
end
