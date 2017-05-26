#include "sql.h"
#include "box/sql.h"
#include "box/iproto_constants.h"
#include "small/region.h"
#include "box/memtx_tuple.h"
#include "box/port.h"

#include "box/sql/sqlite3.h"
#include "lua/utils.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

struct prep_stmt
{
	sqlite3_stmt *stmt;
};

struct prep_stmt_list
{
	uint8_t         *mem_end;   /* denotes actual size of sql_ctx struct */
	uint32_t         pool_size; /* mem at the end used for aux allocations;
				       pool grows from mem_end
				       towards stmt[] array */
	uint32_t         last_select_stmt_index; /* UINT32_MAX if no selects */
	uint32_t         column_count; /* in last select stmt */
	uint32_t         stmt_count;
	struct prep_stmt stmt[6];  /* overlayed with the mem pool;
				      actual size could be larger or smaller */
	/* uint8_t mem_pool[] */
};

static inline int
prep_stmt_list_needs_free(struct prep_stmt_list *l)
{
	return (uint8_t *)(l + 1) != l->mem_end;
}

/* Release resources and free the list itself, unless it was preallocated
 * (i.e. l points to an automatic variable) */
static void
prep_stmt_list_free(struct prep_stmt_list *l)
{
	if (l == NULL)
		return;
	for (size_t i = 0, n = l->stmt_count; i < n; i++)
		sqlite3_finalize(l->stmt[i].stmt);
	if (prep_stmt_list_needs_free(l))
		free(l);
}

static struct prep_stmt_list *
prep_stmt_list_init(struct prep_stmt_list *prealloc)
{
	prealloc->mem_end = (uint8_t *)(prealloc + 1);
	prealloc->pool_size = 0;
	prealloc->last_select_stmt_index = UINT32_MAX;
	prealloc->column_count = 0;
	prealloc->stmt_count = 0;
	return prealloc;
}

/* Allocate mem from the prep_stmt_list pool.
 * If not enough space is available, reallocates the list.
 * If reallocation is needed but l was preallocated, old mem is left
 * intact and a new memory chunk is allocated. */
static void *
prep_stmt_list_palloc(struct prep_stmt_list **pl,
		      size_t size, size_t alignment)
{
	assert((alignment & (alignment - 1)) == 0); /* 2 ^ k */
	assert(alignment <= __alignof__((*pl)->stmt[0]));

	struct prep_stmt_list *l = *pl;
	uint32_t pool_size = l->pool_size;
	uint32_t pool_size_max = (uint32_t)(
		l->mem_end - (uint8_t *)(l->stmt + l->stmt_count)
	);

	assert(UINT32_MAX - pool_size >= size);
	pool_size += size;

	assert(UINT32_MAX - pool_size >= alignment - 1);
	pool_size += alignment - 1;
	pool_size &= ~(alignment - 1);

	if (pool_size > pool_size_max) {
		size_t prev_size = l->mem_end - (uint8_t *)l;
		size_t size = prev_size;
		while (size < prev_size + (pool_size - pool_size_max)) {
			assert(SIZE_MAX - size >= size);
			size += size;
		}
		if (prep_stmt_list_needs_free(l)) {
			l = realloc(l, size);
			if (l == NULL)
				return NULL;
		} else {
			l = malloc(size);
			if (l == NULL)
				return NULL;
			memcpy(l, *pl, prev_size);
		}
		l->mem_end = (uint8_t *)l + size;
		/* move the pool data */
		memmove((uint8_t *)l + prev_size - l->pool_size,
			l->mem_end - l->pool_size,
			l->pool_size);
		*pl = l;
	}

	l->pool_size = pool_size;
	return l->mem_end - pool_size;
}

/* push new stmt; reallocate memory if needed
 * returns a pointer to the new stmt or NULL if out of memory.
 * If reallocation is needed but l was preallocated, old mem is left
 * intact and a new memory chunk is allocated. */
static struct prep_stmt *
prep_stmt_list_push(struct prep_stmt_list **pl)
{
	struct prep_stmt_list *l;
	/* make sure we don't collide with the pool */
	if (prep_stmt_list_palloc(pl, sizeof(l->stmt[0]), 1
				  ) == NULL)
		return NULL;
	l = *pl;
	l->pool_size -= sizeof(l->stmt[0]);
	return l->stmt + (l->stmt_count++);
}

static void
lua_push_column_names(struct lua_State *L, struct prep_stmt_list *l)
{
	sqlite3_stmt *stmt = l->stmt[l->last_select_stmt_index].stmt;
	int n = l->column_count;
	lua_createtable(L, n, 0);
	for (int i = 0; i < n; i++) {
		const char *name = sqlite3_column_name(stmt, i);
		lua_pushstring(L, name == NULL ? "" : name);
		lua_rawseti(L, -2, i+1);
	}
}

static void
lua_push_row(struct lua_State *L, struct prep_stmt_list *l)
{
	sqlite3_stmt *stmt = l->stmt[l->last_select_stmt_index].stmt;
	int column_count = l->column_count;
	char *typestr = (void *)(l->mem_end - column_count);

	lua_createtable(L, column_count, 0);
	lua_rawgeti(L, LUA_REGISTRYINDEX, luaL_array_metatable_ref);
	lua_setmetatable(L, -2);

	for (int i = 0; i < column_count; i++) {
		int type = sqlite3_column_type(stmt, i);
		switch (type) {
		case SQLITE_INTEGER:
			typestr[i] = 'i';
			lua_pushinteger(L, sqlite3_column_int(stmt, i));
			break;
		case SQLITE_FLOAT:
			typestr[i] = 'f';
			lua_pushnumber(L, sqlite3_column_double(stmt, i));
			break;
		case SQLITE_TEXT: {
			const void *text = sqlite3_column_text(stmt, i);
			typestr[i] = 's';
			lua_pushlstring(L, text,
					sqlite3_column_bytes(stmt, i));
			break;
		}
		case SQLITE_BLOB: {
			const void *blob = sqlite3_column_blob(stmt, i);
			typestr[i] = 'b';
			lua_pushlstring(L, blob,
					sqlite3_column_bytes(stmt, i));
			break;
		}
		case SQLITE_NULL:
			typestr[i] = '-';
			lua_rawgeti(L, LUA_REGISTRYINDEX, luaL_nil_ref);
			break;
		default:
			typestr[i] = '?';
			assert(0);
		}
		lua_rawseti(L, -2, i+1);
	}

	lua_pushlstring(L, typestr, column_count);
	lua_rawseti(L, -2, 0);
}

/**
 * Get the meta information about the result of the prepared
 * statement. The meta is returned inside a memtx tuple.
 *
 * @param stmt   Prepared statement.
 * @param region Region memory allocator.
 *
 * @retval not NULL Meta information tuple.
 * @retval     NULL Memory error.
 */
static struct tuple *
get_sql_description(struct sqlite3_stmt *stmt, struct region *region)
{
	int column_count = sqlite3_column_count(stmt);
	assert(column_count > 0);
	size_t description_size = mp_sizeof_array(column_count);
	description_size += (mp_sizeof_map(1) +
			     mp_sizeof_uint(IPROTO_FIELD_NAME)) * column_count;
	for (int i = 0; i < column_count; ++i) {
		const char *name = sqlite3_column_name(stmt, i);
		/*
		 * Can not fail, since all column names
		 * are preallocated during prepare phase and
		 * column_name simply returns them.
		 */
		assert(name != NULL);
		description_size += mp_sizeof_str(strlen(name));
	}
	size_t used = region_used(region);
	char *pos = (char *)region_alloc(region, description_size);
	if (pos == NULL) {
		diag_set(OutOfMemory, description_size, "region_alloc",
			 "description");
		return NULL;
	}
	char *begin = pos;

	pos = mp_encode_array(pos, column_count);
	for (int i = 0; i < column_count; ++i) {
		const char *name = sqlite3_column_name(stmt, i);
		pos = mp_encode_map(pos, 1);
		pos = mp_encode_uint(pos, IPROTO_FIELD_NAME);
		pos = mp_encode_str(pos, name, strlen(name));
	}
	struct tuple *ret = memtx_tuple_new(tuple_format_default, begin, pos);
	region_truncate(region, used);
	if (ret != NULL)
		tuple_ref(ret);
	return ret;
}

/**
 * Convert sqlite3 row to the tuple.
 *
 * @param stmt Started prepared statement. At least one
 *        sqlite3_step must be done.
 * @param region Region memory allocator.
 *
 * @retval not NULL Converted tuple.
 * @retval     NULL Memory error.
 */
static struct tuple *
sqlite3_stmt_to_tuple(struct sqlite3_stmt *stmt, struct region *region)
{
	int column_count = sqlite3_column_count(stmt);
	assert(column_count > 0);
	size_t size = mp_sizeof_array(column_count);
	for (int i = 0; i < column_count; ++i) {
		int type = sqlite3_column_type(stmt, i);
		switch(type) {
			case SQLITE_INTEGER: {
				int64_t n = sqlite3_column_int64(stmt, i);
				if (n >= 0)
					size += mp_sizeof_uint(n);
				else
					size += mp_sizeof_int(n);
				break;
			}
			case SQLITE_FLOAT:
				size += mp_sizeof_double(
						sqlite3_column_double(stmt, i));
				break;
			case SQLITE_TEXT:
				size += mp_sizeof_str(
						sqlite3_column_bytes(stmt, i));
				break;
			case SQLITE_BLOB:
				size += mp_sizeof_bin(
						sqlite3_column_bytes(stmt, i));
				break;
			case SQLITE_NULL:
				size += mp_sizeof_nil();
				break;
			default:
				unreachable();
		}
	}

	size_t used = region_used(region);
	char *pos = (char *)region_alloc(region, size);
	if (pos == NULL) {
		diag_set(OutOfMemory, size, "region_alloc", "raw tuple");
		return NULL;
	}
	char *begin = pos;
	pos = mp_encode_array(pos, column_count);
	for (int i = 0; i < column_count; ++i) {
		int type = sqlite3_column_type(stmt, i);
		switch(type) {
			case SQLITE_INTEGER: {
				int64_t n = sqlite3_column_int64(stmt, i);
				if (n >= 0)
					pos = mp_encode_uint(pos, n);
				else
					pos = mp_encode_int(pos, n);
				break;
			}
			case SQLITE_FLOAT:
				pos = mp_encode_double(pos,
						sqlite3_column_double(stmt, i));
				break;
			case SQLITE_TEXT:
				pos = mp_encode_str(pos,
					(const char *)sqlite3_column_text(stmt,
									  i),
					sqlite3_column_bytes(stmt, i));
				break;
			case SQLITE_BLOB:
				pos = mp_encode_bin(pos,
					sqlite3_column_blob(stmt, i),
					sqlite3_column_bytes(stmt, i));
				break;
			case SQLITE_NULL:
				pos = mp_encode_nil(pos);
				break;
			default:
				unreachable();
		}
	}
	struct tuple *ret = memtx_tuple_new(tuple_format_default, begin, pos);
	region_truncate(region, used);
	if (ret != NULL)
		tuple_ref(ret);
	return ret;
}

/**
 * Bind parameter values to their positions in the prepared
 * statement.
 *
 * @param db SQLite3 engine.
 * @param stmt Prepared statement.
 * @param parameters MessagePack array of parameters, without
 *        array header. Each parameter either must have scalar
 *        type, or must be a map with the following format:
 *        {name: value}. Name - string name of the named
 *        parameter, value - scalar value of the parameter. Named
 *        and numeric parameters can be mixed. For more details
 *        @sa https://www.sqlite.org/lang_expr.html#varparam.
 * @param parameters_count Count of parameters.
 *
 * @retval  0 Success.
 * @retval -1 Client error.
 */
static int
sqlite3_stmt_bind_msgpuck_parameters(sqlite3 *db, struct sqlite3_stmt *stmt,
				     const char *parameters,
				     uint32_t parameters_count)
{
	assert(parameters_count > 0);
	assert(parameters != NULL);
	uint32_t pos = 1;
	for (uint32_t i = 0; i < parameters_count; pos = ++i + 1) {
		if (mp_typeof(*parameters) == MP_MAP) {
			uint32_t len = mp_decode_map(&parameters);
			if (len != 1 || mp_typeof(*parameters) != MP_STR)
				goto error;
			const char *name = mp_decode_str(&parameters, &len);
			pos = sqlite3_bind_parameter_lindex(stmt, name, len);
			if (pos == 0)
				goto error;
		}
		switch (mp_typeof(*parameters)) {
			case MP_UINT: {
				uint64_t n = mp_decode_uint(&parameters);
				if (n > INT64_MAX) {
					diag_set(ClientError, ER_UNSUPPORTED,
						 "SQL", "numbers greater than "\
						 "int64_max");
					return -1;
				}
				if (sqlite3_bind_int64(stmt, pos,
						       n) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_INT: {
				int64_t n = mp_decode_int(&parameters);
				if (sqlite3_bind_int64(stmt, pos,
						       n) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_STR: {
				uint32_t len;
				const char *s = mp_decode_str(&parameters,
							      &len);
				/*
				 * Parameters are allocated within
				 * message pack, received from the
				 * iproto thread. IProto thread
				 * now is waiting for the response
				 * and it will not free the
				 * parameters until
				 * sqlite3_finalize. So there is
				 * no need to copy the parameters
				 * and we can use SQLITE_STATIC.
				 */
				if (sqlite3_bind_text64(stmt, pos, s, len,
						      SQLITE_STATIC,
						      SQLITE_UTF8) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_DOUBLE: {
				double n = mp_decode_double(&parameters);
				if (sqlite3_bind_double(stmt, pos,
							n) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_FLOAT: {
				float n = mp_decode_float(&parameters);
				if (sqlite3_bind_double(stmt, pos,
							n) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_NIL: {
				mp_decode_nil(&parameters);
				if (sqlite3_bind_null(stmt, pos) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_BOOL: {
				/*
				 * SQLite doesn't really support
				 * boolean. Use int instead.
				 */
				int f = mp_decode_bool(&parameters) ? 1 : 0;
				if (sqlite3_bind_int(stmt, pos, f) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_BIN: {
				uint32_t len;
				const char *bin = mp_decode_bin(&parameters,
								&len);
				if (sqlite3_bind_blob64(stmt, pos,
						    (const void *)bin, len,
						    SQLITE_STATIC) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_EXT: {
				const char *start = parameters;
				mp_next(&parameters);
				if (sqlite3_bind_blob64(stmt, pos,
						    (const void *)start,
						    parameters - start,
						    SQLITE_STATIC) != SQLITE_OK)
					goto sql_error;
				break;
			}
			case MP_ARRAY:
			case MP_MAP:
				goto error;
			default:
				unreachable();
		}
	}
	return 0;

error:
	diag_set(ClientError, ER_ILLEGAL_SQL_BIND, (unsigned)pos);
	return -1;
sql_error:
	diag_set(ClientError, ER_SQL, sqlite3_errmsg(db));
	return -1;
}

/**
 * Execute SQL query and return result tuples with meta data.
 *
 * @param sql SQL query.
 * @param length Length of the @sql.
 * @param parameters SQL query parameters.
 *        @sa sqlite3_stmt_bind_msgpuck_parameters.
 * @param parameters_count Count of @parameters.
 * @param[out] description Meta information of the result rows.
 * @param port Storage for the result rows.
 * @param region Region memory allocator.
 *
 * @retval  0 Success.
 * @retval -1 Client or memory error.
 */
int
iproto_sql_execute(const char *sql, uint32_t length, const char *parameters,
		   uint32_t parameters_count, struct tuple **description,
		   struct port *port, struct region *region)
{
	assert(description != NULL);
	assert(port != NULL);
	*description = NULL;
	sqlite3 *db = sql_get();
	if (db == NULL) {
		diag_set(ClientError, ER_SQL, "sql processor is not ready");
		return -1;
	}
	int rc;
	int column_count = 0;
	sqlite3_stmt *stmt;
	if (sqlite3_prepare_v2(db, sql, length, &stmt, &sql) != SQLITE_OK)
		goto sql_error;
	if (stmt == NULL)
		/* Empty request. */
		return 0;
	if (parameters_count != 0 &&
	    sqlite3_stmt_bind_msgpuck_parameters(db, stmt, parameters,
						 parameters_count) != 0) {
			goto error;
	}
	column_count = sqlite3_column_count(stmt);
	if (column_count == 0) {
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);
		if (rc != SQLITE_OK && rc != SQLITE_DONE)
			goto sql_error;
		sqlite3_finalize(stmt);
		return 0;
	}
	assert(column_count > 0);
	*description = get_sql_description(stmt, region);
	if (*description == NULL)
		goto error;

	while ((rc = sqlite3_step(stmt) == SQLITE_ROW)) {
		struct tuple *next = sqlite3_stmt_to_tuple(stmt, region);
		if (next == NULL)
			goto error;
		rc = port_add_tuple(port, next);
		tuple_unref(next);
		if (rc != 0)
			goto error;
	}
	if (rc != SQLITE_OK)
		goto sql_error;
	return 0;

sql_error:
	diag_set(ClientError, ER_SQL, sqlite3_errmsg(db));
error:
	sqlite3_finalize(stmt);
	if (*description != NULL)
		tuple_unref(*description);
	*description = NULL;
	return -1;
}

static int
lua_sql_execute(struct lua_State *L)
{
	int rc;
	sqlite3 *db = sql_get();
	struct prep_stmt_list *l = NULL, stock_l;
	size_t length;
	const char *sql, *sql_end;

	if (db == NULL)
		return luaL_error(L, "not ready");

	sql = lua_tolstring(L, 1, &length);
	if (sql == NULL)
		return luaL_error(L, "usage: box.sql.execute(sqlstring)");

	assert(length <= INT_MAX);
	sql_end = sql + length;

	l = prep_stmt_list_init(&stock_l);
	while (sql != sql_end) {

		struct prep_stmt *ps = prep_stmt_list_push(&l);
		if (ps == NULL)
			goto outofmem;
		rc = sqlite3_prepare_v2(db, sql, (int)(sql_end - sql),
					&ps->stmt, &sql);
		if (rc != SQLITE_OK)
			goto sqlerror;

		if (ps->stmt == NULL) {
			/* only whitespace */
			assert(sql == sql_end);
			l->stmt_count --;
			break;
		}

		int column_count = sqlite3_column_count(ps->stmt);
		if (column_count == 0) {
			while ((rc = sqlite3_step(ps->stmt)) == SQLITE_ROW) { ; }
			if (rc != SQLITE_OK && rc != SQLITE_DONE)
				goto sqlerror;
		} else {
			char *typestr;
			l->column_count = column_count;
			l->last_select_stmt_index = l->stmt_count - 1;

			assert(l->pool_size == 0);
			typestr = prep_stmt_list_palloc(&l, column_count, 1);
			if (typestr == NULL)
				goto outofmem;

			lua_settop(L, 1); /* discard any results */

			/* create result table */
			lua_createtable(L, 7, 0);
			lua_pushvalue(L, lua_upvalueindex(1));
			lua_setmetatable(L, -2);
			lua_push_column_names(L, l);
			lua_rawseti(L, -2, 0);

			int row_count = 0;
			while ((rc = sqlite3_step(ps->stmt) == SQLITE_ROW)) {
				lua_push_row(L, l);
				row_count++;
				lua_rawseti(L, -2, row_count);
			}

			if (rc != SQLITE_OK)
				goto sqlerror;

			l->pool_size = 0;
		}
	}
	prep_stmt_list_free(l);
	return lua_gettop(L) - 1;
sqlerror:
	lua_pushstring(L, sqlite3_errmsg(db));
	prep_stmt_list_free(l);	
	return lua_error(L);
outofmem:
	prep_stmt_list_free(l);
	return luaL_error(L, "out of memory");
}

void
box_lua_sqlite_init(struct lua_State *L)
{
	static const struct luaL_Reg module_funcs [] = {
		{"execute", lua_sql_execute},
		{NULL, NULL}
	};

	/* used by lua_sql_execute via upvalue */
	lua_createtable(L, 0, 1);
	lua_pushstring(L, "sequence");
	lua_setfield(L, -2, "__serialize");

	luaL_openlib(L, "box.sql", module_funcs, 1);
	lua_pop(L, 1);
}

