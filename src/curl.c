/*
 * Copyright (C) 2016-2017 Tarantool AUTHORS: please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *	copyright notice, this list of conditions and the
 *	following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *	copyright notice, this list of conditions and the following
 *	disclaimer in the documentation and/or other materials
 *	provided with the distribution.
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

#include <assert.h>
#include "fiber.h"
#include "curl.h"
#include "lua/utils.h"
#include "small/mempool.h"
#include "box/errcode.h"

/** request and request pool API (Internal)
 * {{{
 */
static inline int
curl_request_add_header(struct curl_request *c, const char *http_header)
{
	assert(c);
	assert(http_header);
	struct curl_slist *l = curl_slist_append(c->headers, http_header);
	if (l == NULL) {
		diag_set(OutOfMemory, sizeof(http_header),
				"curl_slist_append", "curl");
		return -1;
	}
	c->headers = l;
	return 0;
}

static inline int
curl_request_add_header_content_length(struct curl_request *req)
{
	assert(req);
	char buf[38];
	snprintf(buf, sizeof(buf) - 1, "%s: %zu", "Content-Length", req->read);
	return curl_request_add_header(req, buf);
}


/* }}}
 */

/* Internal structures
 * {{{
 */

struct curl_sock {
	/* Curl easy handler */
	CURL *easy;

	/* Reference to contextt*/
	struct curl_ctx *curl_ctx;

	/* libev watcher */
	struct ev_io ev;

	/* Descriptor to curl_socket */
	curl_socket_t sockfd;

	/* Action came from socket*/
	int action;

	/* Timeout for watcher */
	long timeout;

	/* Flag on socket callbacks */
	int evset;
};


/* }}}
 */


static void curl_timer_cb(EV_P_ struct ev_timer *w, int revents);

/** Update the event timer after curl_multi library calls
 */
static int
curl_multi_timer_cb(CURLM *multi __attribute__((unused)),
				long timeout_ms,
				void *data)
{
	struct curl_ctx *ctx = (struct curl_ctx *) data;

	ev_timer_stop(loop(), &ctx->timer_event);
	if (timeout_ms > 0) {
		ev_timer_init(&ctx->timer_event,
				curl_timer_cb,
				(double) (timeout_ms / 1000), 0.);
		ev_timer_start(loop(), &ctx->timer_event);
	}
	else
		curl_timer_cb(loop(), &ctx->timer_event, 0);
	return 0;
}


/** Check for completed transfers, and remove their easy handles
 */
static void
curl_check_multi_info(struct curl_ctx *ctx)
{
	char *eff_url;
	CURLMsg	 *msg;
	int msgs_left;
	struct curl_response *resp;
	long http_code;

	while ((msg = curl_multi_info_read(ctx->multi, &msgs_left))) {

		if (msg->msg != CURLMSG_DONE)
			continue;

		CURL *easy = msg->easy_handle;
		CURLcode curl_code = msg->data.result;

		curl_easy_getinfo(easy, CURLINFO_PRIVATE, (void *) &resp);
		curl_easy_getinfo(easy, CURLINFO_EFFECTIVE_URL, &eff_url);
		curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);

		say_debug("DONE: url = %s, curl_code = %d, http_code = %d",
				eff_url, curl_code, (int) http_code);

		if (curl_code != CURLE_OK)
			++ctx->stat.failed_requests;

		if (http_code == 200)
			++ctx->stat.http_200_responses;
		else
			++ctx->stat.http_other_responses;

		resp->curl_code = (int) curl_code;
		resp->http_code = (int) http_code;
		ipc_cond_signal(&resp->cond);
	} /* while */
}


/** Called by libevent when we get action on a multi socket
 */
static void
curl_event_cb(EV_P_ struct ev_io *watcher, int revents)
{
	(void) loop;

	struct curl_ctx *ctx = (struct curl_ctx *) watcher->data;

	const int action = ( (revents & EV_READ ? CURL_POLL_IN : 0) |
				(revents & EV_WRITE ? CURL_POLL_OUT : 0) );
	say_debug("event_cb: w = %p, revents = %i", (void *) watcher, revents);
	int still_running = 0;
	CURLMcode code = curl_multi_socket_action(ctx->multi, watcher->fd,
						action, &still_running);

	if (code != CURLM_OK &&  code != CURLM_BAD_SOCKET)
		++ctx->stat.failed_requests;

	curl_check_multi_info(ctx);

	if (still_running <= 0) {
		say_debug("last transfer done, kill timeout");
		ev_timer_stop(loop(), &ctx->timer_event);
	}
}

/** Called by libevent when our timeout expires
 */
static void
curl_timer_cb(EV_P_ struct ev_timer *watcher,
		int revents __attribute__((unused)))
{
	(void) loop;

	say_debug("timer_cb: w = %p, revents = %i", (void *) watcher, revents);

	struct curl_ctx *ctx = (struct curl_ctx *) watcher->data;
	int still_running = 0;
	CURLMcode code = curl_multi_socket_action(ctx->multi,
			CURL_SOCKET_TIMEOUT, 0, &still_running);
	if (code != CURLM_OK &&  code != CURLM_BAD_SOCKET)
		++ctx->stat.failed_requests;

	curl_check_multi_info(ctx);
}

/** Clean up the curl_sock structure
 */
static inline void
curl_remove_sock(struct curl_sock *f, struct curl_ctx *ctx)
{
	say_debug("removing socket");

	if (f == NULL)
		return;

	if (f->evset)
		ev_io_stop(loop(), &f->ev);

	++ctx->stat.sockets_deleted;
	mempool_free(&ctx->sock_pool, f);
}


/** Assign information to a curl_sock structure
 */
static inline void
curl_set_sock(struct curl_sock *f,
		curl_socket_t s,
		CURL *e,
		int act,
		struct curl_ctx *ctx)
{
	say_debug("set new socket");

	const int kind = ( (act & CURL_POLL_IN ? EV_READ : 0) |
					(act & CURL_POLL_OUT ? EV_WRITE : 0) );

	f->sockfd  = s;
	f->action  = act;
	f->easy	 = e;
	f->ev.data = ctx;
	f->evset = 1;

	if (f->evset)
		ev_io_stop(loop(), &f->ev);

	ev_io_init(&f->ev, curl_event_cb, f->sockfd, kind);
	ev_io_start(loop(), &f->ev);
}


/** Initialize a new curl_sock structure
 */
static int
curl_add_sock(curl_socket_t s, CURL *easy, int action, struct curl_ctx *ctx)
{
	struct curl_sock *fdp = (struct curl_sock *)
		mempool_alloc(&ctx->sock_pool);
	if (fdp == NULL) {
		diag_set(OutOfMemory, sizeof(struct curl_sock),
				"mempool_alloc", "curl");
		return -1;
	}
	say_debug("add_sock");
	memset(fdp, 0, sizeof(struct curl_sock));

	fdp->curl_ctx = ctx;

	curl_set_sock(fdp, s, easy, action, ctx);

	curl_multi_assign(ctx->multi, s, fdp);

	++fdp->curl_ctx->stat.sockets_added;

	return 0;
}

/* Called on socket action */
static int
curl_sock_cb(CURL *e, curl_socket_t s, int what, void *cbp, void *sockp)
{
	struct curl_ctx *l = (struct curl_ctx *) cbp;
	struct curl_sock *fdp = (struct curl_sock *) sockp;

	static const char *whatstr[] = {
		"none", "IN", "OUT", "INOUT", "REMOVE" };

	say_debug("e = %p, s = %i, what = %s, cbp = %p, sockp = %p",
			e, s, whatstr[what], cbp, sockp);

	if (what == CURL_POLL_REMOVE)
		curl_remove_sock(fdp, l);
	else {
		if (fdp == NULL) {
			if (curl_add_sock(s, e, what, l) < 0)
				return 1;
			}
		else {
			say_debug("Changing action from = %s, to = %s",
					whatstr[fdp->action], whatstr[what]);
			curl_set_sock(fdp, s, e, what, l);
		}
	}

	return 0;
}

/* Called on read action. Sents body to server */
static size_t
curl_read_cb(void *ptr, size_t size, size_t nmemb, void *ctx)
{
	say_debug("Read_cb: size = %zu, nmemb = %zu",
			size, nmemb);

	struct curl_request *req = (struct curl_request *) ctx;
	const size_t total_size = size * nmemb;
	if (!req->body) {
		return total_size;
	}
	size_t to_send = total_size;
	if (req->sent + total_size > req->read)
		to_send = req->read - req->sent;
	memcpy(ptr, req->body + req->sent, to_send);
	req->sent += to_send;
	say_debug("Sent: %zu", to_send);
	return to_send;
}

/* Internal function. Push piece of data to ibuf.
 * Used in write_cb and headers_cb */
static size_t
curl_push_buffer(struct ibuf *bufp, char *data, size_t size)
{
	assert(data);
	assert(bufp);
	char *p = (char *) ibuf_alloc(bufp, size);
	if (!p) {
		diag_set(OutOfMemory, size, "ibuf_alloc", "curl");
		return 0;
	}
	memcpy(p, data, size);
	say_debug("End headers");
	return size;
}

/* Called on write action.
 * Receives data from server and writes it to buffer */
static size_t
curl_write_cb(char *ptr, size_t size, size_t nmemb, void *ctx)
{
	say_debug("Write_cb: size = %zu, nmemb = %zu", size, nmemb);

	struct curl_response *resp = (struct curl_response *) ctx;
	const size_t bytes = size * nmemb;

	return curl_push_buffer(&resp->body, ptr, bytes);
}

/* Called on receiving headers action.
 * Receives parsed headers from server and writes them to buffer */
static size_t
curl_header_cb(char *buffer, size_t size, size_t nitems, void *ctx)
{
	say_debug("Header_cb: size = %zu, mitems = %zu\n , string=%s",
			size, nitems, buffer);
	struct curl_response *resp = (struct curl_response *) ctx;
	const size_t bytes = size * nitems;
	return curl_push_buffer(&resp->headers, buffer, bytes);
}


/* Utilite function*/
static inline void
curl_map_codes(struct curl_response *resp)
{
	switch (resp->curl_code) {
	case CURLE_OK:
		break;
	case CURLE_SSL_CACERT:
		/* nginx code: SSL Certificate Error */
		resp->http_code = 495;
		break;
	case CURLE_PEER_FAILED_VERIFICATION:
		/* nginx code: SSL Certificate Error */
		resp->http_code = 495;
		break;
	case CURLE_GOT_NOTHING:
		/* nginx code: No Response*/
		resp->http_code = 444;
		break;
	case CURLE_HTTP_RETURNED_ERROR:
		// error is already in http_code
		break;
	case CURLE_WRITE_ERROR:
		// error must be written in the last diag_set in write_cb
		break;
	case  CURLE_READ_ERROR:
		diag_set(SystemError, "failed to write to server");
		break;
	case CURLE_UNKNOWN_OPTION:
		/* Error in code */
		assert(false);
	}
	resp->errmsg = curl_easy_strerror(resp->curl_code);
}

static inline int
curl_check_user_error(struct curl_response *resp)
{
	bool bad = false;
	switch (resp->curl_code) {
		case CURLE_MALFORMAT_USER:
			bad = true;
			break;
		case CURLE_UNSUPPORTED_PROTOCOL:
			bad = true;
			break;
		case CURLE_COULDNT_RESOLVE_HOST:
			bad = true;
			break;
		case CURLE_COULDNT_CONNECT:
			bad = true;
			break;
		case CURLE_SSL_CRL_BADFILE:
			bad = true;
			break;
		case CURLE_OUT_OF_MEMORY:
			diag_set(OutOfMemory, 1, "libcurl", "curl");
			return -1;
	}
	if (bad) {
		diag_set(ClientError, ER_ILLEGAL_PARAMS,
				curl_easy_strerror(resp->curl_code));
		return -1;
	}
	return 0;
}

/* Makes response ready to pass to user */
static struct curl_response*
curl_complete_response(struct curl_response *resp)
{
	assert(resp);

	curl_map_codes(resp);
	char *bufp;

	if (ibuf_used(&resp->headers) > 0) {
		bufp = (char *) ibuf_alloc(&resp->headers, 1);
		if (!bufp) {
			diag_set(OutOfMemory, 1, "ibuf_alloc", "curl");
			return NULL;
		}
		*bufp = 0;
	}

	if (ibuf_used(&resp->body) > 0) {
		bufp = (char *) ibuf_alloc(&resp->body, 1);
		if (!bufp) {
			diag_set(OutOfMemory, 1, "ibuf_alloc", "curl");
			return NULL;
		}
		*bufp = 0;
	}
	return resp;
}

static struct curl_response *
curl_response_new(struct curl_ctx* ctx)
{
	assert(ctx);
	struct curl_response *resp = mempool_alloc(&ctx->resp_pool);
	if (!resp) {
		diag_set(OutOfMemory, sizeof(struct curl_response),
				"mempool_alloc", "curl");
		return NULL;
	}
	resp->ctx = ctx;
	ibuf_create(&resp->headers, &cord()->slabc, 1);
	ibuf_create(&resp->body, &cord()->slabc, 1);
	ipc_cond_create(&resp->cond);
	return resp;
}

/** lib C API {{{
 */

struct curl_ctx*
curl_ctx_create(struct curl_ctx *ctx, bool pipeline, long max_conns)
{
	assert(ctx);

	memset(ctx, 0, sizeof(struct curl_ctx));

	mempool_create(&ctx->req_pool, &cord()->slabc,
			sizeof(struct curl_request));
	mempool_create(&ctx->resp_pool, &cord()->slabc,
			sizeof(struct curl_response));
	mempool_create(&ctx->sock_pool, &cord()->slabc,
			sizeof(struct curl_sock));

	ctx->multi = curl_multi_init();
	if (ctx->multi == NULL) {
		diag_set(SystemError, "failed to init multi handler");
		goto error_exit;
	}
	ev_timer_init(&ctx->timer_event, curl_timer_cb, 0., 0.);
	ctx->timer_event.data = (void *) ctx;

	curl_multi_setopt(ctx->multi, CURLMOPT_SOCKETFUNCTION, curl_sock_cb);
	curl_multi_setopt(ctx->multi, CURLMOPT_SOCKETDATA, (void *) ctx);

	curl_multi_setopt(ctx->multi, CURLMOPT_TIMERFUNCTION,
			curl_multi_timer_cb);
	curl_multi_setopt(ctx->multi, CURLMOPT_TIMERDATA, (void *) ctx);

	if (pipeline)
		curl_multi_setopt(ctx->multi, CURLMOPT_PIPELINING, 1L);

	if (max_conns > 0)
		curl_multi_setopt(ctx->multi, CURLMOPT_MAXCONNECTS, max_conns);

	return ctx;

error_exit:
	curl_ctx_destroy(ctx);
	return NULL;
}

void
curl_ctx_destroy(struct curl_ctx *ctx)
{
	if (ctx == NULL)
		return;
	if (ctx->multi != NULL)
		curl_multi_cleanup(ctx->multi);

	mempool_destroy(&ctx->req_pool);
	mempool_destroy(&ctx->resp_pool);
	mempool_destroy(&ctx->sock_pool);
}

struct curl_response*
curl_request_execute(struct curl_request *req, const char *method,
							const char *url)
{
	if (!method) {
		diag_set(ClientError, ER_ILLEGAL_PARAMS,
				"method must be not NULL string");
		return NULL;
	}

	if (!url) {
		diag_set(ClientError, ER_ILLEGAL_PARAMS,
				"method must be not NULL string");
		return NULL;
	}

	++req->ctx->stat.active_requests;
	struct curl_response *resp = curl_response_new(req->ctx);
	if (!resp) {
		goto error_exit;
	}

	curl_easy_setopt(req->easy, CURLOPT_PRIVATE, (void *) resp);
	if (curl_request_add_header_content_length(req) < 0) {
		goto error_exit;
	}
	curl_easy_setopt(req->easy, CURLOPT_URL, url);
	curl_easy_setopt(req->easy, CURLOPT_FOLLOWLOCATION, 1);

	curl_easy_setopt(req->easy, CURLOPT_SSL_VERIFYPEER, 1);

	if (strncmp(method, "GET", sizeof("GET") - 1) == 0) {
		curl_easy_setopt(req->easy, CURLOPT_HTTPGET, 1L);
	}
	else if (strncmp(method, "HEAD", sizeof("HEAD") - 1) == 0) {
		curl_easy_setopt(req->easy, CURLOPT_NOBODY, 1L);
	}
	else if (strncmp(method, "POST", sizeof("POST") - 1) == 0) {
		if (req->read <= 0) {
			diag_set(ClientError, ER_ILLEGAL_PARAMS,
				"Empty body is to be sent with post request");
			goto error_exit;
		}
		if (curl_request_add_header(req, "Accept: */*") < 0) {
			goto error_exit;
		}
		curl_easy_setopt(req->easy, CURLOPT_POST, 1L);
	}
	else if (strncmp(method, "PUT", sizeof("PUT") - 1) == 0) {
		if (req->read <= 0) {
			diag_set(ClientError, ER_ILLEGAL_PARAMS,
				"Empty body is to be sent with put request");
			goto error_exit;
		}
		if (curl_request_add_header(req, "Accept: */*") < 0) {
			goto error_exit;
		}
		curl_easy_setopt(req->easy, CURLOPT_UPLOAD, 1L);
	}
	else if (
		strncmp(method, "OPTIONS", sizeof("OPTIONS") - 1) == 0 ||
		strncmp(method, "DELETE", sizeof("DELETE") - 1) == 0 ||
		strncmp(method, "TRACE", sizeof("TRACE") - 1) == 0 ||
		strncmp(method, "CONNECT", sizeof("CONNECT") - 1) == 0
		 ) {
		curl_easy_setopt(req->easy, CURLOPT_CUSTOMREQUEST, method);
	}
	else {
		diag_set(ClientError, ER_ILLEGAL_PARAMS,
				"undefined method");
		goto error_exit;
	}



	curl_easy_setopt(req->easy, CURLOPT_READFUNCTION, curl_read_cb);
	curl_easy_setopt(req->easy, CURLOPT_READDATA, (void *) req);

	curl_easy_setopt(req->easy, CURLOPT_WRITEFUNCTION, curl_write_cb);
	curl_easy_setopt(req->easy, CURLOPT_WRITEDATA, (void *) resp);

	curl_easy_setopt(req->easy, CURLOPT_HEADERFUNCTION, curl_header_cb);
	curl_easy_setopt(req->easy, CURLOPT_HEADERDATA, (void *) resp);

	curl_easy_setopt(req->easy, CURLOPT_NOPROGRESS, 1L);

	curl_easy_setopt(req->easy, CURLOPT_HTTP_VERSION,
			CURL_HTTP_VERSION_1_1);

	/* Headers have to be set right before add_handle() */
	if (req->headers != NULL)
		curl_easy_setopt(req->easy, CURLOPT_HTTPHEADER, req->headers);

	++req->ctx->stat.total_requests;

	CURLMcode mcode = curl_multi_add_handle(req->ctx->multi, req->easy);


	if (mcode != CURLM_OK && mcode != CURLM_BAD_SOCKET) {
		++req->ctx->stat.failed_requests;
		if (mcode == CURLM_OUT_OF_MEMORY) {
			diag_set(OutOfMemory, 0,
					"curl_multi_add_handle", "curl");
		} else {
			/* error in code */
			if (mcode != CURLM_LAST)
				say_error(curl_multi_strerror(mcode));
			else
				say_error("Unknown error");
			assert(mcode == CURLM_OK);
		}
	}
	ipc_cond_wait_timeout(&resp->cond, TIMEOUT_INFINITY);

	if (curl_check_user_error(resp) < 0) {
		diag_set(ClientError, ER_ILLEGAL_PARAMS,
				curl_easy_strerror(resp->curl_code));
		goto error_exit;
	}
	return curl_complete_response(resp);
error_exit:
	curl_response_delete(resp);
	return NULL;
}

int
curl_set_headers(struct curl_request *req, const char* key, const char* value)
{
	assert(key);
	assert(value);
	char tmp[4096];
	snprintf(tmp, sizeof(tmp) - 1, "%s: %s",
			key, value);
	if (curl_request_add_header(req, tmp) < 0) {
		return -1;
	}
	return 0;
}

int
curl_set_keepalive(struct curl_request *req, long idle, long interval)
{
	assert(req);
	assert(req->easy);
#if (LIBCURL_VERSION_MAJOR >= 7 && \
	LIBCURL_VERSION_MINOR >= 25 && \
	LIBCURL_VERSION_PATCH >= 0 )

	if (idle > 0 && interval > 0) {

		curl_easy_setopt(req->easy, CURLOPT_TCP_KEEPALIVE, 1L);
		curl_easy_setopt(req->easy, CURLOPT_TCP_KEEPIDLE, idle);
		curl_easy_setopt(req->easy, CURLOPT_TCP_KEEPINTVL, interval);
		static char buf[26];

		assert(req);
		snprintf(buf, sizeof(buf) - 1, "Keep-Alive: timeout=%d",
				(int) idle);
		if (curl_request_add_header(req, "Connection: Keep-Alive") < 0 ||
			curl_request_add_header(req, buf) < 0) {
			return -1;
		}
	} else {
		if (curl_request_add_header(req, "Connection: close") < 0) {
			return -1;
		}
	}

#else /* < 7.25.0 */

#endif
	return 0;
}

struct curl_request*
curl_request_new(struct curl_ctx *ctx)
{
	assert(ctx);

	struct curl_request *req = (struct curl_request *)
				mempool_alloc(&ctx->req_pool);

	if (!req) {
		diag_set(OutOfMemory, sizeof(struct curl_request),
				"mempool_alloc", "curl");
		return NULL;
	}

	req->body = NULL;
	req->read = 0;
	req->sent = 0;
	req->headers = NULL;

	req->ctx = ctx;

	req->easy = curl_easy_init();

	if (req->easy == NULL)
	{
		diag_set(SystemError, "failed to init easy handler");
		mempool_free(&ctx->req_pool, req);
		return NULL;
	}

	return req;
}

void
curl_request_delete(struct curl_request *req)
{
	if (!req)
		return;
	if (req->headers) {
		curl_slist_free_all(req->headers);
		req->headers = NULL;
	}

	if (req->easy) {
		curl_easy_cleanup(req->easy);
		req->easy = NULL;
	}

	if (req->body) {
		free(req->body);
		req->read = 0;
		req->sent = 0;
		req->body = NULL;
	}

	--req->ctx->stat.active_requests;
	mempool_free(&req->ctx->req_pool, req);
}

void
curl_response_delete(struct curl_response *resp) {
	if (!resp)
		return;
	assert(resp->ctx);
	ibuf_destroy(&resp->headers);
	ibuf_destroy(&resp->body);

	ipc_cond_destroy(&resp->cond);

	mempool_free(&resp->ctx->resp_pool, resp);
}
/* }}}//lib C API
 * */
