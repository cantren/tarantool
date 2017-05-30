--
--  Copyright (C) 2016-2017 Tarantool AUTHORS: please see AUTHORS file.
--
--  Redistribution and use in source and binary forms, with or
--  without modification, are permitted provided that the following
--  conditions are met:
--
--  1. Redistributions of source code must retain the above
--   copyright notice, this list of conditions and the
--   following disclaimer.
--
--  2. Redistributions in binary form must reproduce the above
--   copyright notice, this list of conditions and the following
--   disclaimer in the documentation and/or other materials
--   provided with the distribution.
--
--  THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
--  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
--  TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
--  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
--  <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
--  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
--  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
--  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
--  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
--  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
--  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
--  THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
--  SUCH DAMAGE.
--

local fiber = require('fiber')
local driver = require('curl.driver')

local curl_mt

--
--  <http> - create a new curl instance.
--
--  Parameters:
--
--  pipeline - set to true to enable pipelining for this multi handle */
--  max_conns -  Maximum number of entries in the connection cache */
--
--  Returns:
--  curl object or raise error()
--

local http = function(opts)

    opts = opts or {}

    opts.pipeline = opts.pipeline or 0
    opts.max_conns = opts.max_conns or 5

    local ok, version = driver.version()
    local curl = driver.new(opts.pipeline, opts.max_conns)
    return setmetatable({   VERSION = version,
    curl = curl, },
    curl_mt )
end


--
--  <request> This function does HTTP request
--
--  Parameters:
--
--  method  - HTTP method, like GET, POST, PUT and so on
--  url     - HTTP url, like https://tarantool.org/doc
--  options - this is a table of options.
--      body    - this parameter is optional, you may use it for passing
--       data to a server. Like 'My text string!'
--
--      ca_path - a path to ssl certificate dir;
--
--      ca_file - a path to ssl certificate file;
--
--      headers - a table of HTTP headers;
--
--      max_conns - max amount of cached alive connections;
--
--      keepalive_idle & keepalive_interval -
--          non-universal keepalive knobs (Linux, AIX, HP-UX, more);
--
--      low_speed_time & low_speed_limit -
--          If the download receives less than
--          "low speed limit" bytes/second
--          during "low speed time" seconds,
--          the operations is aborted.
--          You could i.e if you have
--          a pretty high speed connection, abort if
--          it is less than 2000 bytes/sec
--          during 20 seconds;
--
--      read_timeout - Time-out the read operation
--          after this amount of seconds;
--
--      connect_timeout - Time-out connect operations
--          after this amount of seconds,
--          if connects are OK within this time,
--          then fine...
--          This only aborts the connect phase;
--
--      dns_cache_timeout - DNS cache timeout;
--
--  Returns:
--      {http_code=NUMBER,
--      body=STRING,
--      headers=STRING,
--      errmsg=STRING} or error()
--


curl_mt = {
    __index = {
        --
        --  <request> see above <request>
        --
        request = function(self, method, url, opts)

            if not method or not url then
                error('request(method, url [, options]])')
            end

            return self.curl:request(method, url, opts or {})
        end,

        --
        -- <get> - see <request>
        --
        get = function(self, url, options)
            return self:request('GET', url, options)
        end,

        --
        -- <post> - see <request>
        --
        post = function(self, url, options)
            return self:request('POST', url, options)
        end,

        --
        -- <put> - see <request>
        --
        put = function(self, url, options)
            return self:request('PUT', url, options)
        end,

        --
        -- <http_options> see <request>
        --
        http_options = function(self, url, options)
            return self:request('OPTIONS', url, options)
        end,

        --
        -- <head> see <request>
        --
        head = function(self, url, options)
            return self:request('HEAD', url, options)
        end,
        --
        -- <delete> see <request>
        --
        delete = function(self, url, options)
            return self:request('DELETE', url, options)
        end,

        --
        -- <trace> see <request>
        --
        http_trace = function(self, url, options)
            return self:request('TRACE', url, options)
        end,

        --
        -- <http_connect> see <request>
        --
        http_connect = function(self, url, options)
            return self:request('CONNECT', url, options)
        end,

        --
        -- <stat> - this function returns a table with many values of statistic.
        --
        -- Returns {
        --
        --  active_requests - this is number of currently executing requests
        --
        --  sockets_added -
        --  this is a total number of added sockets into libev loop
        --
        --  sockets_deleted -
        --  this is a total number of deleted sockets from libev
        --                      loop
        --
        --  total_requests - this is a total number of requests
        --
        --  http_200_responses -
        --  this is a total number of requests which have
        --              returned a code HTTP 200
        --
        --  http_other_responses -
        --      this is a total number of requests which have
        --      requests not a HTTP 200
        --
        --  failed_requests - this is a total number of requests which have
        --      failed (included systeme erros, curl errors, HTTP
        --      errors and so on)
        --  }
        --  or error()
        --
        stat = function(self)
            return self.curl:stat()
        end,

    },
}

--
-- Export
--
return {
    -- <see http>
    http = http,
}
