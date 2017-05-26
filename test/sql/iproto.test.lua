remote = require('net.box')

box.sql.execute('create table test (id primary key, a float, b text)')
space = box.space.test
space:replace{1, 2, '3'}
space:replace{4, 5, '6'}
space:replace{7, 8.5, '9'}
box.sql.execute('select * from test')
box.schema.user.grant('guest','read,write,execute', 'universe')
cn = remote.connect(box.cfg.listen)
cn:ping()

--
-- Static queries, with no parameters.
--

-- Simple select.
cn:sql_execute('select * from test')

-- Operation with boolean result.
cn:sql_execute('insert into test values (10, 11, NULL)')
cn:sql_execute('delete from test where a = 5')

-- SQL errors.
cn:sql_execute('insert into not_existing_table values ("kek")')
cn:sql_execute('insert qwerty gjsdjq  q  qwd qmq;; q;qwd;')

-- Empty result.
cn:sql_execute('select id as identifier from test where a = 5;')

-- netbox API errors.
cn:sql_execute(100)
cn:sql_execute('select 1', nil, {dry_run = true})

--
-- Parmaeters bindig.
--

cn:sql_execute('select * from test where id = ?', {1})
parameters = {}
parameters[1] = {}
parameters[1][':value'] = 1
cn:sql_execute('select * from test where id = :value', parameters)
cn:sql_execute('select ?, ?, ?', {1, 2, 3})
parameters = {}
parameters[1] = 10
parameters[2] = {}
parameters[2]['@value2'] = 12
parameters[3] = {}
parameters[3][':value1'] = 11
cn:sql_execute('select ?, :value1, @value2', parameters)

parameters = {}
parameters[1] = {}
parameters[1]['$value3'] = 1
parameters[2] = 2
parameters[3] = {}
parameters[3][':value1'] = 3
parameters[4] = 4
parameters[5] = 5
parameters[6] = {}
parameters[6]['@value2'] = 6
cn:sql_execute('select $value3, ?, :value1, ?, ?, @value2, ?, $value3', parameters)

-- Try not-integer types.
msgpack = require('msgpack')
cn:sql_execute('select ?, ?, ?, ?, ?', {'abc', -123.456, msgpack.NULL, true, false})

-- Try to replace '?' in meta with something meaningful.
cn:sql_execute('select ? as kek, ? as kek2', {1, 2})

--
-- Errors during parameters binding.
--
-- Try value > INT64_MAX. SQLite can't bind it, since it has no
-- suitable method in its bind API.
cn:sql_execute('select ? as big_uint', {0xefffffffffffffff})
-- Bind incorrect parameters.
cn:sql_execute('select ?', { {1, 2, 3} })
parameters = {}
parameters[1] = {}
parameters[1][100] = 200
cn:sql_execute('select ?', parameters)

parameters = {}
parameters[1] = {}
parameters[1][':value'] = {kek = 300}
cn:sql_execute('select :value', parameters)

cn:close()
box.schema.user.revoke('guest', 'read,write,execute', 'universe')
box.sql.execute('drop table test')
space = nil
