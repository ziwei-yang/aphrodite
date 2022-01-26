# require 'rmagick' when using this module
module OCR
	def ocr_png(png_fname, opt={})
		require 'rmagick' unless Object.const_defined?('Magick')
		verbose = (opt[:verbose] != false)
		puts "OCR PNG #{png_fname}" if verbose
		b64_str = nil
		begin
			img = Magick::Image.read(png_fname).first
			img.format = 'JPEG'
			jpg_fname = png_fname + '.jpg'
			img.write(jpg_fname)
			b64_str = Base64.strict_encode64(File.read(jpg_fname))
			FileUtils.rm jpg_fname
		rescue => e
			APD::Logger.highlight "Could not load PNG #{png_fname}"
			APD::Logger.error e
		end

		puts "POST OCR Request with #{b64_str.size} b64 string" if verbose
		data = {
			"username": ENV['TTSHITU_USER'] || raise("No TTSHITU_USER in ENV"),
			"password": ENV['TTSHITU_PSWD'] || raise("No TTSHITU_PSWD in ENV"),
			"image": b64_str
		}
		headers = { :"Content-Type" => 'application/json;charset=UTF-8' }
		resp = RestClient::Request.execute(
			method: :post,
			url: "http://api.ttshitu.com/base64",
			headers: headers,
			payload:data,
			timeout:60
		)
		resp = JSON.parse(resp)
		raise "Response failed #{resp} at OCR of #{png_fname}" unless resp["success"] == true
		res = resp.dig("data", "result")
		raise "Response failed #{resp} at OCR of #{png_fname}" if res.nil?
		puts "OCR result [#{res}]" if verbose
		res
	end
end
