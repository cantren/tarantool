/*
 * Copyright (C) 2016-2017 Tarantool AUTHORS: please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *	  copyright notice, this list of conditions and the
 *	  following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *	  copyright notice, this list of conditions and the following
 *	  disclaimer in the documentation and/or other materials
 *	  provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#ifndef CURL_LUA_C_INCLUDED
#define CURL_LUA_C_INCLUDED 1

/**
 * Unique name for userdata metatables
 */
#define DRIVER_LUA_UDATA_NAME	"__tnt_curl"

#include "src/curl.h"
#include "curl.h"
#include "say.h"
#include "lua/utils.h"
#include "box/errcode.h"

/** Internal util functions
 * {{{
 */
static int
curl_make_result(lua_State *L, struct curl_response *r);

static inline struct curl_ctx*
ctx_get(lua_State *L)
{
	return (struct curl_ctx *)
			luaL_checkudata(L, 1, DRIVER_LUA_UDATA_NAME);
}

static inline void
lua_add_key_u64(lua_State *L, const char *key, uint64_t value)
{
	lua_pushstring(L, key);
	lua_pushinteger(L, value);
	lua_settable(L, -3);
}
/* }}}
 */

/** lib Lua API {{{
 */

static int
luaT_curl_request(lua_State *L)
{
	struct curl_ctx *ctx = ctx_get(L);
	if (ctx == NULL)
		return luaL_error(L, "can't get lib ctx");

	struct curl_request *req = curl_request_new(ctx);
	if (!req) {
		return luaL_error(L, "can't get new request");
	}

	const char *method = luaL_checkstring(L, 2);

	const char *url    = luaL_checkstring(L, 3);

	/** Set Options {{{
	 */
	if (lua_istable(L, 4)) {

		const int top = lua_gettop(L);

		lua_pushstring(L, "body");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1)) {
			size_t len = 0;
			const char* body = lua_tolstring(L, top + 1, &len);

			if (curl_set_body(req, body, len) < 0) {
				goto error_exit;
			}
		}
		lua_pop(L, 1);

		/** Http headers */
		lua_pushstring(L, "headers");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1)) {
			lua_pushnil(L);
			while (lua_next(L, -2) != 0) {
				if (curl_set_headers(req, lua_tostring(L, -2),
						lua_tostring(L, -1)) < 0) {
					goto error_exit;
				}
				lua_pop(L, 1);
			}
		}
		lua_pop(L, 1);

		lua_pushstring(L, "ca_path");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_ca_path(req,
					lua_tostring(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "ca_file");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_ca_file(req,
					lua_tostring(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "max_conns");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_max_conns(req,
					(long) lua_tointeger(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "keepalive_idle");
		lua_gettable(L, 4);
		long keepalive_idle = 0;
		if (!lua_isnil(L, top + 1))
			keepalive_idle = (long) lua_tointeger(L, top + 1);
		lua_pop(L, 1);

		lua_pushstring(L, "keepalive_interval");
		lua_gettable(L, 4);
		long keepalive_interval = 0;
		if (!lua_isnil(L, top + 1))
			keepalive_interval = (long) lua_tointeger(L, top + 1);
		lua_pop(L, 1);

		if (curl_set_keepalive(req, keepalive_idle,
					keepalive_interval) < 0)
			goto error_exit;
		lua_pushstring(L, "low_speed_limit");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_low_speed_limit(req,
					(long) lua_tointeger(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "low_speed_time");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_low_speed_time(req,
					(long) lua_tointeger(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "read_timeout");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_read_timeout(req,
					(long) lua_tointeger(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "connect_timeout");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_connect_timeout(req,
					(long) lua_tointeger(L, top + 1));
		lua_pop(L, 1);

		lua_pushstring(L, "dns_cache_timeout");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1))
			curl_set_dns_cache_timeout(req,
					(long) lua_tointeger(L, top + 1));
		lua_pop(L, 1);

		/* Debug- / Internal- options */
		lua_pushstring(L, "curl_verbose");
		lua_gettable(L, 4);
		if (!lua_isnil(L, top + 1) && lua_isboolean(L, top + 1))
			curl_set_verbose(req, true);
		lua_pop(L, 1);
	} else {
		diag_set(ClientError, ER_ILLEGAL_PARAMS,
				"third argument have to be a table");
		goto error_exit;
	}
	/* }}} */

	struct curl_response *resp =
		curl_request_execute(req, method, url);

	if (!resp) {
		goto error_exit;
	}

	int result = curl_make_result(L, resp);
	curl_request_delete(req);
	curl_response_delete(resp);
	return	result;
error_exit:
	curl_request_delete(req);
	luaT_error(L);
	return 0;
}

static int
luaT_curl_get_stat(lua_State *L)
{
	struct curl_ctx *ctx = ctx_get(L);
	if (ctx == NULL)
		return luaL_error(L, "can't get curl ctx");

	lua_newtable(L);

	lua_add_key_u64(L, "active_requests",
			(uint64_t) ctx->stat.active_requests);
	lua_add_key_u64(L, "sockets_added",
			(uint64_t) ctx->stat.sockets_added);
	lua_add_key_u64(L, "sockets_deleted",
			(uint64_t) ctx->stat.sockets_deleted);
	lua_add_key_u64(L, "total_requests",
			ctx->stat.total_requests);
	lua_add_key_u64(L, "http_200_responses",
			ctx->stat.http_200_responses);
	lua_add_key_u64(L, "http_other_responses",
			ctx->stat.http_other_responses);
	lua_add_key_u64(L, "failed_requests",
			(uint64_t) ctx->stat.failed_requests);

	return 1;
}

static int
curl_make_result(lua_State *L, struct curl_response *resp)
{
	assert(resp);
	lua_newtable(L);

	lua_pushstring(L, "http_code");
	lua_pushinteger(L, resp->http_code);
	lua_settable(L, -3);

	lua_pushstring(L, "error_message");
	lua_pushstring(L, resp->errmsg);
	lua_settable(L, -3);

	if (curl_response_headers(resp)) {
		lua_pushstring(L, "headers");
		lua_pushstring(L, curl_response_headers(resp));
		lua_settable(L, -3);
	}

	if (curl_response_body(resp)) {
		lua_pushstring(L, "body");
		lua_pushstring(L, curl_response_body(resp));
		lua_settable(L, -3);
	}

	return 1;
}

static int
luaT_version(lua_State *L)
{
	char version[sizeof("curl: xxx.xxx.xxx,") + sizeof("libev: xxx.xxx") ];
	snprintf(version, sizeof(version) - 1,
		"curl: %i.%i.%i, libev: %i.%i",
		LIBCURL_VERSION_MAJOR,
		LIBCURL_VERSION_MINOR,
		LIBCURL_VERSION_PATCH,
		EV_VERSION_MAJOR,
		EV_VERSION_MINOR );
	lua_pushboolean(L, true);
	lua_pushstring(L, version);
	return 2;
}

static int
luaT_curl_new_ctx(lua_State *L)
{

	struct curl_ctx *ctx = (struct curl_ctx *)
			lua_newuserdata(L, sizeof(struct curl_ctx));
	if (ctx == NULL)
		return luaL_error(L, "lua_newuserdata failed: curl_ctx");

	/* pipeline: 1 - on, 0 - off */
	bool pipeline  = (bool) luaL_checkint(L, 1);
	long max_conns = luaL_checklong(L, 2);
	ctx = curl_ctx_create(ctx, pipeline, max_conns);
	if (ctx == NULL)
		return luaL_error(L, "curl_create failed;\
					can't create multi_handler");

	luaL_getmetatable(L, DRIVER_LUA_UDATA_NAME);
	lua_setmetatable(L, -2);

	return 1;
}

static int
luaT_curl_cleanup(lua_State *L)
{
	curl_ctx_destroy(ctx_get(L));

	/* remove all methods operating on ctx */
	lua_newtable(L);
	lua_setmetatable(L, -2);

	lua_pushboolean(L, true);
	lua_pushinteger(L, 0);
	return 2;
}

/*
 * }}}
 */

/*
 * Lists of exporting: object and/or functions to the Lua
 */

static const struct luaL_Reg R[] = {
	{"version", luaT_version},
	{"new", luaT_curl_new_ctx},
	{NULL, NULL}
};

static const struct luaL_Reg M[] = {
	{"request", luaT_curl_request},
	{"stat", luaT_curl_get_stat},
	{"__gc", luaT_curl_cleanup},
	{NULL, NULL}
};

/*
 * Lib initializer
 */
LUA_API int
luaopen_curl_driver(lua_State *L)
{
	luaL_register_type(L, DRIVER_LUA_UDATA_NAME, M);
	luaL_register(L, "curl.driver", R);
	return 1;
}

#endif /* CURL_LUA_C_INCLUDED */
