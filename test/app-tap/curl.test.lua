#!/usr/bin/env tarantool

local tap = require('tap')
local curl = require('curl')
local json = require('json')
local test = tap.test("curl")
local log = require('log')
local fiber = require('fiber')
-- Supress console log messages
log.level(4)
local TNT_BUILD_DIR = os.getenv("TARANTOOL_SRC_DIR")
-- create ev_loop
box.cfg{logger='tarantool.log'}

test:test("basic http post/get", function(test)
	test:plan(8)

	local http = curl.http()

	test:ok(http ~= nil, "client is created")
	local url = "http://httpbin.org/"
	local headers = { my_header = "1", my_header2 = "2" }
	local my_body = { key = "value" }
	local json_body = json.encode(my_body)
	local responses = {}
	local data = {a = 'b'}
	headers['Content-Type'] = 'application/json'
	local fibers = 2
	local ch = fiber.channel(fibers)

	fiber.create(function()
		responses.good_get = http:get(url .. "get", {headers = headers})
		ch:put(1)
	end)
	fiber.create(function()
		responses.bad_get = http:get(url .. 'this/page/not/exists',
				   {headers = headers})
		ch:put(1)
	end)
	for i = 1, fibers
		do
			ch:get()
		end
	local r = responses.good_get
	test:is(r.http_code, 200, "GET: http code page exists")
	test:isnt(r.body:len(), 0,"GET: not empty body page exists")
	local body = json.decode(r.body)
	test:is(body.url, url .. "get", "GET: right body")

	r = responses.bad_get
	test:is(r.http_code, 404, "GET: http_code page not exists")
	test:isnt(r.body:len(), 0, "GET: not empty body page not exists")
	test:ok(string.find(r.body, "Not Found"),
				"GET: right body page not exists")

	local st = http:stat()
	test:ok(st.sockets_added == st.sockets_deleted and
		st.active_requests == 0,
		"stats checking")
end)

test:test("Errors checks", function(test)
	test:plan(6)
	local http = curl:http()
	test:ok(not pcall(http.get, http, "htp://ya.ru"),
		"GET: curl exception on bad protocol")
	test:ok(not pcall(http.post, http, "htp://ya.ru", {body="{val=1}"}),
		"POST: curl exception on bad protocol")
	test:ok(not pcall(http.post, http, "ya.ru"),
		"POST: sent: curl error on empty body")
	test:ok(not pcall(http.put, http, "ya.ru"),
		"PUT: sent: curl error on empty body")
	test:ok(not pcall(http.get, http, "http://yaru"),
		"GET: curl exception on bad url")
	local status, err = pcall(http.request, http, "NOMETHOD", "ya.ru")
	test:ok(not status and string.find(json.encode(err), "undefined method"),
				"Exception on method, that doesn't exist")
end)

test:test("special methods", function(test)
	test:plan(4)
	local http = curl.http()
	local url = "https://httpbin.org/"
	local responses = {}
	local ch = fiber.channel(3)

	fiber.create(function()
		responses.delete_data = http:delete(url .. "delete")
		ch:put(1)
	end)
	fiber.create(function()
		responses.options_data = http:http_options(url)
		ch:put(1)
	end)
	fiber.create(function()
		responses.head_data = http:head(url)
		ch:put(1)
	end)

	for i = 1, 3
		do
			ch:get()
		end
	local some_header = "HTTP/1.1 200 OK"
	test:is(responses.delete_data.http_code, 200, "HTTP:DELETE request")
	test:is(responses.options_data.http_code, 200, "HTTP:OPTIONS request")
	test:is(responses.head_data.http_code, 200, "HTTP:HEAD request code")
	test:ok(string.find(responses.head_data.headers, some_header),
		"HTTP:HEAD request content")
end)



test:test("tests with server", function(test)
	test:plan(9)


	os.execute(TNT_BUILD_DIR .. "/test/app-tap/server.js & echo $! > ./nd.pid")

	local http = curl.http()
	local host	  = '127.0.0.1:10000'
	local headers = { my_header = "1", my_header2 = "2" }
	local my_body = { key = "value" }
	local json_body = json.encode(my_body)

	fiber.sleep(1)

	local trace_data = http:http_trace(host)
	test:is(trace_data.http_code, 200, "HTTP:TRACE request")


	local connect_data = http:http_connect(host)
	test:is(connect_data.http_code, 200, "HTTP:CONNECT request")

	local put_data = http:put(host,	{headers = headers,
					 body = json_body,
					 })

	local body = json.decode(put_data.body)
	test:ok( put_data.http_code == 200 and
		body.headers['my_header'] == headers.my_header and
		body.headers['my_header2'] == headers.my_header2,
		"PUT: headers")
	test:is(body["data"], json_body, "PUT: body")


	local post_data = http:post(host, {headers = headers,
					 body = json_body,
					 keepalive_idle = 30,
					 keepalive_interval = 60,})

	body = json.decode(post_data.body)
	test:ok( post_data.http_code == 200 and
		body.headers['my_header'] == headers.my_header and
		body.headers['my_header2'] == headers.my_header2,
		"POST: headers")
	test:is(body["data"], json_body, "POST: body")

	local num = 10
	local curls   = { }
	local headers = { }

	-- Init [[
	for i = 1, num do
	  headers["My-header" .. i] = "my-value"
	end

	for i = 1, num do
	  table.insert(curls, {url = host .. '/',
		http = curl.http(),
		body = json.encode({stat = box.stat(),
		info = box.info() }),
		headers = headers,
		connect_timeout = 5,
		read_timeout = 5,
		dns_cache_timeout = 1,
		} )
	end
	-- ]]

	-- Start test
	for i = 1, num do

	  local obj = curls[i]

	  for j = 1, 10 do
		  fiber.create(function()
			  obj.http:post(obj.url,
				{headers = obj.headers,
				body = obj.body,
				keepalive_idle = 30,
				keepalive_interval = 60,
				connect_timeout = obj.connect_timeout,
				read_timeout = obj.read_timeout,
				dns_cache_timeout = obj.dns_cache_timeout, })
		  end )
		  fiber.create(function()
			  obj.http:get(obj.url,
				{headers = obj.headers,
				keepalive_idle = 30,
				keepalive_interval = 60,
				connect_timeout = obj.connect_timeout,
				read_timeout = obj.read_timeout,
				dns_cache_timeout = obj.dns_cache_timeout, })
		  end )
	  end
	end
	local ok_sockets_added = true
	local ok_active = true
	local ok_timeout = true
	-- Join test
	fiber.create( function()
	local os	 = require('os')
	local yaml = require('yaml')
	local rest = num
	local ticks = 0

	while true do

		fiber.sleep(1)

		for i = 1, num do

			local obj = curls[i]

		  	if obj.http ~= nil and obj.http:stat().active_requests == 0 then
				local st = obj.http:stat()
				if st.sockets_added ~= st.sockets_deleted then
					ok_sockets_added = false
					rest = 0
				end
				if st.active_requests ~= 0 then
					ok_active = false
					rest = 0
				end
			rest = rest - 1
			curls[i].http = nil
			end
		end

		if rest <= 0 then
			break
		end

		ticks = ticks + 1

		-- Test failed
		if ticks > 80 then
			ok_timeout = false
			rest = 0
		end

	  end
	end)
	fiber.sleep(1)
	test:ok(ok_sockets_added, "Concurrent.Test free sockets")
	test:ok(ok_active, "Concurrent.Test no active requests")
	test:ok(ok_timeout, "Concurrent.Test timeout not expired")

	os.execute("cat ./nd.pid | xargs kill -s TERM; rm nd.pid")
end)
os.exit(0)
