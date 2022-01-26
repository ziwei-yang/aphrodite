# require_relative './conf/config'
# Should load config before this file.

require 'rubygems'
require 'bundler/setup'

# All Aphrodite methods and classes will be put in this namespace,
# which could be renamed easily.
module APD
	def self.require_try(lib)
		begin
			require lib
		rescue
			puts "Fail to load [#{lib}], skip."
		end
	end
	
	def self.require_anyof(*libs)
		success = false
		error = nil
		libs.each do |lib|
			begin
				require lib
				success = true
				break
			rescue RuntimeError, LoadError => e
				error = e
				puts "Fail to load [#{lib}], try optional choice."
				next
			end
		end
		raise error unless success
	end
end
target_module = APD

target_module.require_anyof 'bunny', 'march_hare'
target_module.require_try 'execjs'
if RUBY_ENGINE == 'ruby'
	# Could not compile mysql2 on ubuntu 1804
	target_module.require_try 'mysql2'
end

require 'cgi'
require 'uri'
require 'open-uri'
require 'date'
require 'redis'
require 'nokogiri'
require "mysql"
require 'logger'
require 'colorize'
require 'json'
require 'base64'
require 'mail'
require 'optparse'
require 'optparse/time'
require 'ostruct'
require 'redlock'
require 'concurrent'
# require 'typhoeus'
require 'http'

# Load all script in given namespace.
module APD
	APD_COMMON_PATH ||= File.dirname(File.absolute_path(__FILE__))
	dir = APD_COMMON_PATH
	# Load refinement and utility before regular files.
	processed_file = ["#{dir}/bootstrap.rb"]
	first_batch = ['refine', 'util', 'encode', 'spider'].map { |f| "#{dir}/#{f}.rb" }
	first_batch.each do |f|
 		eval File.read(f), binding, File.basename(f)
	end
	processed_file += first_batch

	# Load regular files, with some files under /bin
	batch = Dir["#{dir}/*.rb"] - processed_file + ["#{dir}/../bin/mail_task.rb"]
	batch.each do |f|
 		eval File.read(f), binding, File.basename(f)
	end

	# Load bin/
	f = "#{dir}/../bin/mail_task.rb"
	eval File.read(f), binding, File.basename(f)
end
