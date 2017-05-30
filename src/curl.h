#ifndef DRIVER_H_INCLUDED
#define DRIVER_H_INCLUDED 1
/*
 * Copyright (C) 2016 - 2017 Tarantool AUTHORS: please see AUTHORS file.
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

#include <diag.h>
#include "curl/curl.h"
#include "small/ibuf.h"
#include "small/mempool.h"
#include <ipc.h>

/*
 * Structures {{{
*/

/* Statistics structure */
struct curl_stat {
	uint64_t total_requests;
	uint64_t http_200_responses;
	uint64_t http_other_responses;
	size_t failed_requests;
	size_t active_requests;
	size_t sockets_added;
	size_t sockets_deleted;
};

/* Context of curl */
struct curl_ctx {

	/* Libev timer-watcher*/
	struct ev_timer timer_event;

	/* Curl multi handler*/
	CURLM *multi;

	/* Memory pools for requests and responses */
	struct mempool req_pool;
	struct mempool resp_pool;
	struct mempool sock_pool;

	/* Various values of statistics, they are used only for all
	 * Connection in curl context */
	struct curl_stat stat;
};

struct curl_request {

	/* Information associated with a specific easy handle */
	CURL *easy;

	/* Reference to curl context */
	struct curl_ctx *ctx;

	/* HTTP headers */
	struct curl_slist *headers;

	/* body to be sent to server,
	 * its length, number of bytes sent to server */
	char *body;
	size_t read;
	size_t sent;
};

/* Response structure. User gets it after executing request */
struct curl_response {
	/* Reference to curl context */
	struct curl_ctx *ctx;
	/* Internal curl code */
	int curl_code;
	/* Http code */
	int http_code;
	/* buffer of headers */
	struct ibuf headers;
	/* buffer of body */
	struct ibuf body;
	/* Error message */
	const char *errmsg;
	/* Internal condition variable */
	struct ipc_cond cond;
};

/*
 * }}} Structures
 */

/*
 * Curl context API
 * {{{
 */

/**
 *\brief Creates new context.
 *\param ctx pointer to allocated memory of structure ctx
 *\param max_conn - Maximum number of entries in the Connection cache
 *\param pipeline - Set to true to enable pipelining for this multi handle
 *\return pointer to created context of NULL in case of error
 */
struct curl_ctx*
curl_ctx_create(struct curl_ctx *ctx, bool pipeline, long max_conns);

/**
 *\brief destroy context object.
 *\param ctx pointer to allocated memory of structure ctx
 *\details doesn't free allocated memory
 */
void
curl_ctx_destroy(struct curl_ctx *);

/* }}}
 */

/*
 * Request API
 * {{{
 */

/**
 * \brief Creates object of request
 * \param ctx - reference to context
 * \return new request object or NULL in case of error
 */
struct curl_request*
curl_request_new(struct curl_ctx *ctx);

/**
 * \brief This function does async HTTP request
 * \param request - reference to request object with filled fields
 * \return pointer structure curl_response or
 * \details User recieves the reference to object response,
 * which should be destroyed with curl_response_destroy() at the end
 * Don't delete the request object before handling response!
 * That will destroy some fields in response object.
 */
struct curl_response*
curl_request_execute(struct curl_request *, const char *method,
		const char *url);

/**
 * \brief Delete request object
 * \param request - reference to object
 * \details Should be called even if error in execute appeared
 */
void
curl_request_delete(struct curl_request *req);

/**
 * Setter field headers.
 * \param req - object request
 * \param key - name of header
 * \param value - value of header
 */
int
curl_set_headers(struct curl_request *req, const char *key, const char *value);

/**
 * Sets body of request
 * \param req - reference to curl_request
 * \param body
 * \param bytes - number of bytes to be sent
 */
static inline int
curl_set_body(struct curl_request *req, const char *body, size_t bytes)
{
	assert(req);
	assert(body);
	say_debug("body:%s, bytes:%zu", body, bytes);
	if (bytes > 0) {
		req->body = (char *) malloc(bytes);
		if (!req->body) {
			diag_set(OutOfMemory, bytes, "malloc", "curl");
			return -1;
		}
		memcpy(req->body, body, bytes);
		req->read = bytes;
		req->sent = 0;
	}
	return 0;
}

/** Sets Max amount of cached alive Connections
 * \param req
 * \param max_conns
 * */
static inline void
curl_set_max_conns(struct curl_request *req, long max_conns)
{
	assert(req);
	assert(req->easy);
	if (max_conns > 0)
		curl_easy_setopt(req->easy, CURLOPT_MAXCONNECTS, max_conns);
}


/**
 * \brief Non-universal keepalive knobs (Linux, AIX, HP-UX, more)
 * \param req - reference to request
 * \param idle - delay, in seconds, that the operating system will wait
 * 		while the connection is idle before sending keepalive probes
 * \param interval - Sets the interval, in seconds,
 * 			that the operating system will wait
 * 			between sending keepalive probes
 * \details Depends on version of libcurl. Added in 7.25.0
 */
int
curl_set_keepalive(struct curl_request *req, long idle, long interval);

/**
 * \brief Set the "low speed time"
 * \param req
 * \param low_speed_time
 * \details If the download receives less than "low speed limit" bytes/second
 * during "low speed time" seconds, the operations is aborted.
 * You could i.e if you have a pretty high speed Connection,
 * abort if it is less than 2000 bytes/sec during 20 seconds;
*/
static inline void
curl_set_low_speed_time(struct curl_request *req, long low_speed_time)
{
	assert(req->easy);
	if (low_speed_time > 0)
		curl_easy_setopt(req->easy, CURLOPT_LOW_SPEED_TIME,
							low_speed_time);
}

/**
 * \brief Set the "low speed limit"
 * \param req
 * \param low_speed_limit
 * \details If the download receives less than "low speed limit" bytes/second
 * during "low speed time" seconds, the operations is aborted.
 * You could i.e if you have a pretty high speed Connection,
 * abort if it is less than 2000 bytes/sec during 20 seconds;
 */
static inline void
curl_set_low_speed_limit(struct curl_request *req, long low_speed_limit)
{
	assert(req);
	assert(req->easy);
	if (low_speed_limit > 0)
		curl_easy_setopt(req->easy, CURLOPT_LOW_SPEED_LIMIT,
				low_speed_limit);
}

/* \brief Set Time-out the read operation after this amount of seconds
 * \param req
 * \param read_timeout
 */
static inline void
curl_set_read_timeout(struct curl_request *req, long read_timeout)
{
	assert(req);
	assert(req->easy);
	if (read_timeout > 0)
		curl_easy_setopt(req->easy, CURLOPT_TIMEOUT, read_timeout);
}

/*
 * \brief Sets connect timout
 * \param req
 * \req connect_timeout
 * \details Time-out connects operations after this amount of seconds,
 * if connects are OK within this time, then fine...
 * This only aborts the Connect phase.
 */
static inline void
curl_set_connect_timeout(struct curl_request *req, long connect_timeout)
{
	assert(req);
	assert(req->easy);
	if (connect_timeout > 0)
		curl_easy_setopt(req->easy, CURLOPT_CONNECTTIMEOUT,
				connect_timeout);
}

/* \brief Sets DNS cache timeout
 * \param req
 * \param dns_cache_timeout
 */
static inline void
curl_set_dns_cache_timeout(struct curl_request *req, long dns_cache_timeout)
{
	assert(req);
	assert(req->easy);
	if (dns_cache_timeout > 0)
		curl_easy_setopt(req->easy, CURLOPT_DNS_CACHE_TIMEOUT,
						 dns_cache_timeout);
}

/* \brief Enables/Disables curl verbose mode
 * \param req
 * \param curl_verbose - flag
 */
static inline void
curl_set_verbose(struct curl_request *req, bool curl_verbose)
{
	assert(req);
	assert(req->easy);
	if (curl_verbose)
		curl_easy_setopt(req->easy, CURLOPT_VERBOSE, 1L);
}

/* \brief Sets directory with certificates
 * \param req
 * \ca_path - Path to directory holding one or more certificates
 * to verify the peer with
 */
static inline void
curl_set_ca_path(struct curl_request *req, const char *ca_path)
{
	assert(req);
	assert(req->easy);
	if (ca_path)
		curl_easy_setopt(req->easy, CURLOPT_CAPATH, ca_path);
}

/* \brief Sets name of file with certificates
 * \param req
 * \param ca_file - File holding one or more certificates
 * to verify the peer with
 */
static inline void
curl_set_ca_file(struct curl_request *req, const char *ca_file)
{
	assert(req);
	assert(req->easy);
	if (ca_file)
		curl_easy_setopt(req->easy, CURLOPT_CAINFO, ca_file);
}

/*
 * }}} Request API
 */

/*
 * Response API
 * {{{
 */

/**
 * \brief Destroy the response object
 * \param ctx - reference to context
 * \param resp - reference to response object
 */
void
curl_response_delete(struct curl_response *resp);

/* \brief Get response buffers
 * \param resp - response
 * \details Buffers will be destroyed after call reponse_destroy
 */
static inline char *
curl_response_headers(struct curl_response* resp) {
	if (ibuf_used(&resp->headers) > 0) {
		return resp->headers.buf;
	}
	return NULL;
}

/* \brief Get response body
 * \param resp - response
 * \details Body will be destroyed after call reponse_destroy
 */
static inline char *
curl_response_body(struct curl_response* resp) {
	if (ibuf_used(&resp->body) > 0) {
		return resp->body.buf;
	}
	return NULL;
}

#endif /* DRIVER_H_INCLUDED */
