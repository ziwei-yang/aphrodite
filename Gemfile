gem 'rest-client'
gem 'ruby-mysql'
gem 'redis'
gem 'nokogiri'
gem 'logger'
gem 'colorize'
gem 'execjs'
gem 'connection_pool'
gem 'ruby-progressbar'
gem 'concurrent-ruby', require: 'concurrent'
gem 'mail'
gem 'mailgun-ruby'
gem 'redlock'

if RUBY_ENGINE == 'jruby'
	gem "march_hare", "~> 2.21.0"
	gem 'twitter'
elsif RUBY_ENGINE == 'truffleruby'
	gem 'bunny', '>= 2.6.3'
	# Could not compile mysql2 on ubuntu 1804
	# Could not compile twitter on macOS
else
	gem 'twitter'
	gem 'bunny', '>= 2.6.3'
	gem 'mysql2', '~>0.4.0'
end
