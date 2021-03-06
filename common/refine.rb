# Encoding: utf-8

######################################
# Global monkey patch for all versions.
######################################
if 1.class == Integer
	class ::Integer
		def strftime(format='%FT%T%:z')
			self.to_time.strftime(format)
		end

		def to_time
			if self > 9999999999
				return DateTime.strptime(self.to_s, '%Q')
			end
			DateTime.strptime(self.to_s, '%s')
		end

		def to_s_with_delimiter(opt={})
			delimiter = opt[:delimiter] || ','
			self.to_s.reverse.gsub(/(\d{3})(?=\d)/, "\\1#{delimiter}").reverse
		end
	end
else
	class ::Fixnum
		def strftime(format='%FT%T%:z')
			self.to_time.strftime(format)
		end

		def to_time
			if self > 9999999999
				return DateTime.strptime(self.to_s, '%Q')
			end
			DateTime.strptime(self.to_s, '%s')
		end

		def to_s_with_delimiter(opt={})
			delimiter = opt[:delimiter] || ','
			self.to_s.reverse.gsub(/(\d{3})(?=\d)/, "\\1#{delimiter}").reverse
		end
	end
end
class ::Date
	def to_full_str
		self.strftime "%Y-%m-%d"
	end

	def to_mysql_time
		self.strftime "%Y-%m-%d 00:00:00"
	end

	def to_yyyymmdd
		self.strftime "%Y%m%d"
	end

	def to_yyyymm
		self.strftime "%Y%m"
	end

	def to_i
		self.strftime('%Q').to_i
	end
end

class ::DateTime
	def to_full_str
		self.strftime "%Y-%m-%d %H:%M:%S.%L"
	end

	def to_mysql_time
		self.strftime "%Y-%m-%d %H:%M:%S"
	end

	def to_yyyymmdd
		self.strftime "%Y%m%d"
	end

	def to_yyyymm
		self.strftime "%Y%m"
	end

	def to_i
		self.strftime('%Q').to_i
	end
end

######################################
# Selective refinements.
######################################
if defined?(using) == 'method'
	module NokogiriRefine
		refine ::Nokogiri::XML::Element do
			def select_nodes(name_path=[], attrs=[], filter={})
				name_path = [name_path] unless name_path.is_a?(Array)
				if attrs.is_a?(Hash)
					filter = attrs
					attrs = []
				end
				attrs = [attrs] unless attrs.is_a?(Array)
				nodes = [self]
				# Dig by name_path
				name_path.each do |n|
					nodes = nodes.
						map { |c| c.children }.
						reduce(:+).
						select { |c| c.name == n }
				end
				# All attrs should be contained.
				unless attrs.empty?
					nodes = nodes.select do |c|
						attrs.select { |a| c.attr(a) != nil }.size == attrs.size
					end
				end
				# All filters should be matched.
				filter.each do |k, v|
					nodes = nodes.select do |c|
						c.attr(k.to_s) == v
					end
				end
				nodes
			end
		end
	end
	module EncodeRefine
		refine ::Date do
			def to_full_str
				self.strftime "%Y-%m-%d"
			end
	
			def to_mysql_time
				self.strftime "%Y-%m-%d 00:00:00"
			end
	
			def to_yyyymmdd
				self.strftime "%Y%m%d"
			end
	
			def to_yyyymm
				self.strftime "%Y%m"
			end

			def to_i
				self.strftime('%Q').to_i
			end
		end

		refine ::DateTime do
			def to_full_str
				self.strftime "%Y-%m-%d %H:%M:%S.%L"
			end
	
			def to_mysql_time
				self.strftime "%Y-%m-%d %H:%M:%S"
			end
	
			def to_yyyymmdd
				self.strftime "%Y%m%d"
			end
	
			def to_yyyymm
				self.strftime "%Y%m"
			end

			def to_i
				self.strftime('%Q').to_i
			end
		end
	
		refine 1.class do
			def strftime(format='%FT%T%:z')
				self.to_time.strftime(format)
			end

			def to_time
				if self > 9999999999
					return DateTime.strptime(self.to_s, '%Q')
				end
				DateTime.strptime(self.to_s, '%s')
			end

			def to_s_with_delimiter(opt={})
				delimiter = opt[:delimiter] || ','
				self.to_s.reverse.gsub(/(\d{3})(?=\d)/, "\\1#{delimiter}").reverse
			end
		end
	
		refine ::String do
			def strftime(format='%FT%T%:z')
				DateTime.parse(self).strftime format
			end

			def extract_useragent
				res = ''
				# Platform
				res << 'Android ' if include? 'Android '
				res << 'iPad ' if include? 'iPad; '
				res << 'iPhone ' if include? 'iPhone; '
				res << 'Win' if include? 'Windows NT '
				res << 'Mac' if include? 'Macintosh'
				res << 'Mac' if include? 'Darwin'
				# Browser
				{'MicroMessenger'=>'微信', 'QQ'=>'QQ', 'Firefox'=>'firefox', 'NetType'=>'网络'}.each do |attr, display_name|
					res << display_name << (split(attr)[1].split(' ')[0]) << ' ' if include? attr
				end
				return self if res.empty?
				res
			end
		end
	end
else
	# Monkey patch for those ruby versions with no using feature.
	module EncodeRefine
	end

	Kernel.module_eval do
		def using(module_name)
			puts "WARNING!!! Current ruby engine [#{RUBY_ENGINE}] does not support keyword[using], use monkey patch instead."
		end
	end

	class ::String
		def strftime(format='%FT%T%:z')
			DateTime.parse(self).strftime format
		end

		def extract_useragent
			res = ''
			# Platform
			res << 'Android ' if include? 'Android '
			res << 'iPad ' if include? 'iPad; '
			res << 'iPhone ' if include? 'iPhone; '
			res << 'Win' if include? 'Windows NT '
			res << 'Mac' if include? 'Macintosh'
			res << 'Mac' if include? 'Darwin'
			# Browser
			{'MicroMessenger'=>'微信', 'QQ'=>'QQ', 'Firefox'=>'firefox', 'NetType'=>'网络'}.each do |attr, display_name|
				res << display_name << (split(attr)[1].split(' ')[0]) << ' ' if include? attr
			end
			return self if res.empty?
			res
		end
	end
end
