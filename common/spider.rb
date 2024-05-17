module SpiderUtil
	include EncodeUtil

	USER_AGENTS = {
		'PHONE'		=> 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1',
		'TABLET'	=> 'Mozilla/5.0 (iPad; CPU OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1',
		'DESKTOP'	=> 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
	}

	def post_web(host, path, data, opt={})
		header = {
			'User-Agent'			=> 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:51.0) Gecko/20100101 Firefox/51.0',
			'Accept'					=> '*/*',
			'Accept-Language'	=> 'en-US,en;q=0.5',
			'Accept-Encoding'	=> 'gzip, deflate',
			'Connection'			=> 'keep-alive',
			'Pragma'					=> 'no-cache',
			'Cache-Control'		=> 'no-cache'
		}
		header = opt[:header] unless opt[:header].nil?
		header['Host'] ||= host

		verbose = opt[:verbose] == true
		connect = opt[:connect] ||= Net::HTTP.start(host, (opt[:port] || 80))
		data = map_to_poststr(data) if data.is_a?(Hash)

		header['Content-Type'] = 'application/x-www-form-urlencoded'
		header['Content-Length'] = data.size.to_s
		if verbose
			puts "SpiderUtil.post: host: #{host}"
			puts "SpiderUtil.post: path: #{path}"
			puts "SpiderUtil.post: header: #{header.to_json}"
			puts "SpiderUtil.post: data: #{data.to_json}"
		end
		resp = connect.post path, data, header
		raise "SpiderUtil.post: HTTP CODE #{resp.code}" if resp.code != '200'
		body = resp.body
		# Deflat gzip if possible.
		if body[0..2].unpack('H*') == ["1f8b08"]
			size = body.size
			gz = Zlib::GzipReader.new(StringIO.new(body))    
			body = gz.read
			puts "SpiderUtil.post: deflat from gzip #{size} -> #{body.size}" if verbose
		end
		puts "SpiderUtil.post: response.body [#{body.size}]:#{body[0..300]}" if verbose
		body
	end

	def parse_html(html, encoding=nil, opt={})
		if encoding.is_a?(Hash)
			opt = encoding
			encoding = opt[:encoding]
		end
		return Nokogiri::HTML(html, nil, encoding)
	end

	def parse_web(url, encoding = nil, max_ct = -1, opt = {})
		if encoding.is_a?(Hash)
			opt = encoding
			max_ct = opt[:max_ct]
		end
		if max_ct != nil && max_ct > 0
			opt[:max_time] ||= (max_ct * 60)
		end

		doc = nil
		if opt[:render] == true
			doc = render_html url, opt
		else
			doc = curl url, opt
		end
		return Nokogiri::HTML(doc)
	end
	
	def curl_native(url, opt={})
		filename = opt[:file]
		max_ct = opt[:retry] || -1
		retry_delay = opt[:retry_delay] || 1
		doc = nil
		ct = 0
		while true
			begin
				open(filename, 'wb') do |file|
					file << open(url).read
				end
				return doc
			rescue => e
				Logger.debug "error in downloading #{url}: #{e.message}"
				ct += 1
				raise e if max_ct > 0 && ct >= max_ct
				sleep retry_delay
			end
		end
	end

	# Form the native curl command to invoke.
	# Yield its file in block if given.
	# Otherwise return its content in UTF-8.
	def curl(url, opt={})
		file = opt[:file]
		use_cache = opt[:use_cache] == true
		agent = opt[:agent]
		retry_delay = opt[:retry_delay] || 1
		encoding = opt[:encoding]
		header = opt[:header] || {}
		post_data = opt[:post]
		post_data = map_to_poststr(post_data) if post_data.is_a?(Hash)

		tmp_file_use = file.nil?
		if tmp_file_use
			rand = Random.rand(10000).to_s.rjust(4, '0')
			file = "curl_#{hash_str(url)}_#{rand}.html"
		end
		# Directly return from cache file if use_cache=true
		if file != nil && File.file?(file) && use_cache == true
			Logger.debug("#{url} --> directly return cache:#{file}") if opt[:verbose] == true
			result = File.open(file, "rb").read
			return result
		end
		cmd = "curl --compressed --output '#{file}' -L " # -L to Follow 301 redirection.
		cmd += " --data \"#{post_data}\"" unless post_data.nil?
		cmd += " --fail" unless opt[:allow_http_error]
		if opt[:verbose]
			cmd += " -v"
		else
			cmd += " --silent"
		end
		cmd += " -A '#{agent}'" unless agent.nil?
		cmd += " --cookie #{opt[:cookie]}" unless opt[:cookie].nil?
		cmd += " --retry #{opt[:retry]}" unless opt[:retry].nil?
		cmd += " --retry-delay #{retry_delay}"
		cmd += " --max-time #{opt[:max_time]}" unless opt[:max_time].nil?
		header.each do |k, v|
			cmd += " --header '#{k}: #{v}'"
		end
		cmd += " '#{url}'"
		Logger.debug(cmd) if opt[:verbose]
		ret = system(cmd)
		return nil if File.exist?(file) == false
		# Let block to handle file or return its content in UTF-8
		result = nil
		if block_given?
			result = yield(file)
		else
			unless encoding.nil?
				cmd = "iconv -f #{encoding} -t utf-8//IGNORE '#{file}' -o '#{file}.utf8'"
				Logger.debug(cmd) if opt[:verbose]
				system(cmd)
				cmd = "mv '#{file}.utf8' '#{file}'"
				Logger.debug(cmd) if opt[:verbose]
				system(cmd)
			end
			result = File.open(file, "rb").read
			result = result.force_encoding('utf-8') unless encoding.nil?
		end
		begin # Clean tmp file.
			File.delete(file) if tmp_file_use
		rescue => e
			Logger.error e
		end
		return result
	end

	def map_to_poststr(map)
		str = ""
		(map || {}).each do |k, v|
			v = v.to_s
			str = "#{CGI::escape(k)}=#{CGI::escape(v)}&#{str}"
		end
		str
	end

	def href_url(homeurl, href)
		return nil if href.nil?
		return href if (href =~ /^[a-zA-Z]*\:\/\// ) == 0
		raise "#{homeurl} is not a URI" unless (homeurl =~ /^[a-zA-Z]*\:\/\// ) == 0
		protocol = homeurl.split('://')[0]
		segs = homeurl.split('/')
		base_domain = segs[0..2].join('/')
		if segs.size > 3
			base_dir = homeurl.split('/')[0..-2].join('/')
		else
			base_dir = base_domain
		end
		return "#{protocol}:#{href}" if href[0..1] == '//'
		return "#{base_domain}#{href}" if href[0] == '/'
		return "#{base_dir}/#{href}"
	end

	########################################
	# Phantomjs task proxy.
	########################################
	include ExecUtil
	include LogicControl
	def render_html(url, opt={}, &block)
		method = opt[:with] || 'phantomjs'
		retry_ct = opt[:retry_ct] || 3
		puts "\tRender #{url} with #{method}" if opt[:verbose] == true
		if method == 'phantomjs' || method == :phantomjs
			limit_retry(retry_ct:retry_ct, sleep: 60) {
				return render_with_phantomjs(url, opt)
			}
		elsif method == 'firefox' || method == :firefox
			# Block with carrying vars would not work with retry
			limit_retry(retry_ct:retry_ct, sleep: 60) {
				return render_with_firefox(url, opt, &block)
			}
		end
	end
	def render_with_phantomjs(url, opt={})
		rand = Random.rand(10000).to_s.rjust(4, '0')
		task_file = "/tmp/phantomjs_#{hash_str(url)}_#{rand}.task"
		html_file = "/tmp/phantomjs_#{hash_str(url)}_#{rand}.html"
		timeout = opt[:timeout] || 300
		task = {
			'url'			=>	url,
			'settings'=>	opt[:settings],
			'html'		=>	opt[:html] || html_file,
			'timeout'	=>	(timeout/2)*1000,
			'image'		=>	opt[:image],
			'loadimage'		=>	opt[:loadimage],
			'switch_device_after_fail' => (opt[:switch_device_after_fail] == true),
			'action'			=> opt[:action],
			'action_time' => (opt[:action_time] || 15),
			'post_render_wait_time' => (opt[:post_render_wait_time] || 0)
		}
		task.keys.each do |k|
			task.delete k if task[k].nil?
			opt.delete k
		end
		File.open(task_file, 'w') { |f| f.write(JSON.pretty_generate(task)) }
		command = "timeout #{timeout}s phantomjs --ignore-ssl-errors=true #{APD_COMMON_PATH}/html_render.js -f #{task_file}"
		# Force do not use thread, pass other options to exec_command().
		opt[:thread] = false
		status = exec_command(command, opt)
		raise JSON.pretty_generate(status) unless status['ret'] == true
		html = File.read(html_file)
		begin
			FileUtils.rm task_file
			FileUtils.rm html_file
		rescue => e
			Logger.error e
		end
		return html
	end

	def prepare_firefox_webdriver(opt={})
		verbose = (opt[:verbose] != false)
		options = Selenium::WebDriver::Firefox::Options.new
		options.headless! unless opt[:headless] == false # Default: headless
		options.set_preference('general.useragent.override', opt[:agent]) if opt[:agent].is_a?(String)
		Logger.highlight "width and height options omit" if opt[:width] != nil || opt[:height] != nil
# 		width = opt[:width] || 1400
# 		height = opt[:height] || 900
# 		puts "New firefox #{width}x#{height}" if verbose
# 		options.add_argument("--window-size=#{width},#{height}")
		# Disable firefox built-in JSON viewer
		options.add_preference("devtools.jsonview.enabled", false)
		if Selenium::WebDriver::VERSION >= '4.0'
			driver = Selenium::WebDriver.for :firefox, capabilities: [options]
		else
			driver = Selenium::WebDriver.for :firefox, options: options
		end
		driver.manage.window.maximize
# 		driver.execute_script("window.resizeTo(#{width},#{height})")
		driver
	end

	def selenium_click(webdriver, btn)
		x = btn.rect.x+btn.rect.width/2
		y = btn.rect.y+btn.rect.height/2
		begin
			webdriver.action.move_to_location(x, y).perform
			webdriver.action.click_and_hold(btn).perform
			webdriver.action.release.perform
		rescue
			# Selenium::WebDriver::Error::MoveTargetOutOfBoundsError happens sometimes.
			btn.click
		end
	end

	def selenium_moveto(webdriver, btn)
		x = btn.rect.x+btn.rect.width/2
		y = btn.rect.y+btn.rect.height/2
		webdriver.action.move_to_location(x, y).perform
	end

	def render_with_firefox(url, opt={})
		verbose = (opt[:verbose] != false)
		driver = opt[:firefox] || opt[:webdriver]
		driver_new = false
		if driver.nil?
			driver = prepare_firefox_webdriver(opt)
			driver_new = true
		end
		begin
			puts "\tfirefox #{url}", level:2 if verbose
			driver.navigate.to(url)

			render_t = opt[:render_t] || opt[:post_render_wait_time] || 10
			puts "\tRender #{render_t} seconds for: #{url}", level:2 if verbose
			sleep render_t

			if block_given?
				loop {
					break if yield(driver) == true
					puts "\tRender #{render_t} seconds for: #{url}", level:2  if verbose
					sleep render_t
				}
			end

			html = driver.page_source
		ensure
			driver.quit if driver_new
		end
		return html
	end

	def curl_with_selenium_cookies(firefox, url, opt={})
		cookies = firefox.manage.all_cookies
		# See: https://curl.se/docs/http-cookies.html
		# The cookie file format is text based and stores one cookie per line.
		# Lines that start with # are treated as comments.
		# Each line that each specifies a single cookie consists of seven text fields
		# separated with TAB characters.
		# A valid line must end with a newline character.
		cookie_lines = cookies.map { |ck|
			# puts ck.inspect
			# Fields in the file:
			# Field number, what type and example data and the meaning of it:
			#     string example.com - the domain name
			#     boolean FALSE - include subdomains
			#     string /foobar/ - path
			#     boolean TRUE - send/receive over HTTPS only
			#     number 1462299217 - expires at - seconds since Jan 1st 1970, or 0
			#     string person - name of the cookie
			#     string daniel - value of the cookie
			segs = [
				ck[:domain],
				(ck[:same_site] != nil ? 'TRUE' : 'FALSE'),
				ck[:path],
				(ck[:http_only] == false ? 'TRUE' : 'FALSE'),
				ck[:expires].strftime('%Q').to_i/1000,
				ck[:name],
				ck[:value]
			]
			# puts segs.join("\t")
			next segs.join("\t")
		}
		cookies_f = "/tmp/selenium.#{Time.now.to_i}_#{Random.rand(9999)}.cookies"
		puts "firefox #{cookie_lines.size} cookies -> #{cookies_f}\n--> #{url}"
		File.open(cookies_f, 'w') { |f| f.write(cookie_lines.join("\n")) }
		opt = opt.clone
		opt[:cookie] = cookies_f
		ret = curl(url, opt)
		FileUtils.rm(cookies_f) if File.file?(cookies_f)
		ret
	end
end
