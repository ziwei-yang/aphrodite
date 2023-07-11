gem 'rest-client', '~> 2.1.0'
gem 'ruby-mysql'
gem 'redis', '~> 4.7.1' # blpop(list, timeout) does not work when >= 5.0
gem 'nokogiri'
gem 'logger'
gem 'colorize'
gem 'execjs'
gem 'ruby-progressbar'
gem 'mail'
gem 'mailgun-ruby'
gem 'redlock'
# gem 'rmagick'

# For twitter and connection pool
# gem 'typhoeus'

# Enable below line if wish to use http with socks proxy and keep-alive.
# gem 'http', '5.0.0.pre'
# Enable below two lines if wish to use twitter v1
# gem 'twitter', '~>7.0'
# gem 'http'

# For firefox headless html render
if RUBY_ENGINE == 'ruby' && RUBY_VERSION >= '2.5'
	gem 'selenium-webdriver', '~> 4.0.0.beta1' # requires ruby >= 2.5
else
	gem 'selenium-webdriver', '>= 3.142.7'
end

gem 'concurrent-ruby', require: 'concurrent'
# Potential performance improvements may be achieved under MRI 
# by installing optional C extensions.
gem 'concurrent-ruby-ext' if RUBY_ENGINE == 'ruby'

if RUBY_ENGINE == 'jruby'
	gem "march_hare", "~> 2.21.0"
elsif RUBY_ENGINE == 'truffleruby'
	gem 'bunny', '>= 2.6.3'
	# Could not compile mysql2 on ubuntu 1804
else
	gem 'bunny', '>= 2.6.3'
  if RUBY_PLATFORM.include?('arm64-darwin')
    # auto compiling failed on M1 CPU for mysql2
	elsif RUBY_ENGINE == 'ruby' && RUBY_VERSION >= '2.5'
		gem 'mysql2', '~>0.5'
	else
		# 0.4.x Does not work with BigDecimal
		# 0.4.0 works with Rails 4
		gem 'mysql2', '~>0.4.0'
	end
end
