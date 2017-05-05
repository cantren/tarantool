env = require('test_run')
test_run = env.new()
fiber = require('fiber')

--
-- Tests on data integrity after dump of memory runs or range
-- compaction.
--
-- The aim is to test vy_write_iterator. There are several combinations
-- of various commands that can occur:
--   1) delete followed by upsert : write iterator should convert
--      upsert to replace (insert)
--   2) upsert followed by delete: the upsert is filtered out,
--      delete can be filtered out or not depending on whether it's
--      compaction (filtered out) or dump (preserved)
--   3) upsert followed by upsert: two upserts are folded together
--      into one
--   4) upsert followed by replace: upsert is replaced
--   5) replace followed by upsert: two commands are folded
--      into a single replace with upsert ops applied
--   6) replace followed by delete:
--      both are eliminated in case of compaction;
--      replace is filtered out if it's dump
--   7) delete followed by replace: delete is filtered out
--   8) replace followed by replace: the first replace is filtered
--      out
--   9) single upsert (for completeness)
--   10) single replace (for completeness)

space = box.schema.space.create('test', { engine = 'vinyl' })
--
--
pk = space:create_index('primary', { page_size = 12 * 1024, range_size = 12 * 1024 })

-- Insert many big tuples and then call snapshot to
-- force dumping and compacting.

big_val = string.rep('1', 2000)

_ = space:insert{1}
_ = space:insert{2, big_val}
_ = space:insert{3, big_val}
_ = space:insert{5, big_val}
_ = space:insert{6, big_val}
_ = space:insert{7, big_val}
_ = space:insert{8, big_val}
_ = space:insert{9, big_val}
_ = space:insert{10, big_val}
_ = space:insert{11, big_val}
space:count()
box.snapshot()

--
-- Create a couple of tiny runs on disk, to increate the "number of runs"
-- heuristic of hte planner and trigger compaction
--

space:insert{12}
box.snapshot()
space:insert{13}
box.snapshot()
#space:select{}

space:drop()

--
-- Create a vinyl index with small page_size parameter, so that
-- big tuples will not fit in a single page.
--

space = box.schema.space.create('test', { engine = 'vinyl' })
pk = space:create_index('primary', { page_size = 256, range_size = 3 * 1024 })
space:insert({1})
box.snapshot()

big_val = string.rep('1', 2000)

_ = space:insert{2, big_val}
_ = space:insert{3, big_val}
_ = space:insert{5, big_val}
_ = space:insert{6, big_val}
_ = space:insert{7, big_val}
_ = space:insert{8, big_val}
_ = space:insert{9, big_val}
_ = space:insert{10, big_val}
_ = space:insert{11, big_val}
-- Increate the number of runs, trigger compaction
space:count()
box.snapshot()
space:insert{12}
box.snapshot()
space:insert{13}
box.snapshot()
#space:select{}

space:drop()

-- Test dumping and compacting a space with more than one index.

space = box.schema.space.create('test', { engine = 'vinyl' })
pk = space:create_index('primary', { page_size = 512, range_size = 1024 * 12 })
index2 = space:create_index('secondary', { parts = {2, 'string'}, page_size = 512, range_size = 1024 * 12 })
for i = 1, 100 do space:insert{i, ''..i} if i % 2 == 0 then box.snapshot() end end
space:delete{1}
space:delete{10}
space:delete{100}
box.snapshot()

index2:delete{'9'}
index2:delete{'99'}
box.snapshot()

space:select{2}

-- Test that not dumped changes are visible.

space:upsert({2, '2'}, {{'=', 3, 22}})
space:select{2}
space:upsert({2, '2'}, {{'!', 3, 222}})
space:select{2}
space:upsert({2, '2'}, {{'!', 3, 2222}})

space:select{2}
box.snapshot()

space:select{2}
space:update({2}, {{'!', 3, 22222}})
box.snapshot()

space:select{2}

space:drop()

space = box.schema.space.create('test', { engine = 'vinyl' })
pk = space:create_index('primary', { page_size = 128, range_size = 1024 })

-- Test that snaphot() inside a transaction doesn't lose data
-- and that upserts are successfully merged.

box.begin()
space:upsert({2}, {{'=', 2, 22}})
space:upsert({2}, {{'!', 2, 222}})
space:upsert({2}, {{'!', 2, 2222}})
space:select{}

box.snapshot()

box.commit()

space:select{}
space:insert({3})

box.snapshot()

space:select{}

--
-- Verify that deletion of tuples with key 2 and 3 is
-- successfully dumped and compacted.
--

box.begin()

space:delete{2}
space:delete{3}

box.commit()

space:upsert({10}, {{'!', 2, 10}})
box.snapshot()
space:select{}

-- Test that deletion is successfully dumped and compacted.

space:delete{10}

space:upsert({10}, {{'!', 2, 10}})
space:upsert({10}, {{'!', 2, 10}})
box.snapshot()
space:select{}
space:delete{10}

space:upsert({10}, {{'!', 2, 10}})
space:delete({10})
box.snapshot()
space:select{}

-- Test that if replace is met then previous upsert is ignored.

space:upsert({10}, {{'!', 2, 10}})
space:replace({10, 100})
box.snapshot()
space:select{}
space:delete{10}

-- Test that dumping and compacting didn't lose single upsert.

space:upsert({100}, {{'!', 2, 100}})
box.snapshot()
space:select{}
space:delete{100}

-- Verify that if upsert goes after replace then they will be merged.
space:replace({200})
space:upsert({200}, {{'!', 2, 200}})
box.snapshot()
space:select{}
space:delete{200}

-- Insert more tuples than can fit in range_size

big_val = string.rep('1', 400)
_ = space:replace({1, big_val})
_ = space:replace({2, big_val})
_ = space:replace({3, big_val})
_ = space:replace({4, big_val})
_ = space:replace({5, big_val})
_ = space:replace({6, big_val})
_ = space:replace({7, big_val})
space:count()
box.snapshot()
space:count()
space:delete({1})
space:delete({2})
space:delete({3})
space:delete({4})
space:delete({5})
space:delete({6})
space:delete({7})
space:select{}
box.snapshot()
space:select{}

-- Test that update successfully merged with replace and other updates
space:insert({1})
space:update({1}, {{'=', 2, 111}})
space:update({1}, {{'!', 2, 11}})
space:update({1}, {{'+', 3, 1}, {'!', 4, 444}})
space:select{}
box.snapshot()
space:select{}
space:delete{1}
box.snapshot()
space:select{}

-- Test upsert after deletion

space:insert({1})
box.snapshot()
space:select{}
space:delete({1})
space:upsert({1}, {{'!', 2, 111}})
space:select{}
box.snapshot()
space:select{}
space:delete({1})

-- Test upsert before deletion

space:insert({1})
box.snapshot()
space:select{}
space:upsert({1}, {{'!', 2, 111}})
space:delete({1})
box.snapshot()
space:select{}

-- Test deletion before replace

space:insert({1})
box.snapshot()
space:select{}
space:delete({1})
space:replace({1, 1})
box.snapshot()
space:select{}
space:delete({1})

-- Test replace before deletion

space:replace({5, 5})
space:delete({5})
box.snapshot()
space:select{}

-- Test many replaces

space:replace{6}
space:replace{6, 6, 6}
space:replace{6, 6, 6, 6}
space:replace{6, 6, 6, 6, 6}
space:replace{6, 6, 6, 6, 6, 6}
space:replace{6, 6, 6, 6, 6, 6, 6}
box.snapshot()
space:select{}
space:delete({6})

space:drop()

-- gh-1725 merge iterator can't merge more than two runs

space = box.schema.space.create('tweedledum', {engine = 'vinyl'})
pk = space:create_index('primary')
-- integer keys
space:replace{1, 'tuple'}
box.snapshot()
space:replace{2, 'tuple 2'}
box.snapshot()
space:replace{3, 'tuple 3'}
pk:get{1} or {'none'}
pk:get{2}
pk:get{3}

space:drop()

--
-- gh-1920: squash subsequences of statements of the same key
-- according to exising read views.
--
test_run:cmd("setopt delimiter ';'")

function create_iterator(obj, key, opts)
	local iter, key, state = obj:pairs(key, opts)
	local res = {}
	res['iter'] = iter
	res['key'] = key
	res['state'] = state
	return res
end;

function iterator_next(iter_obj)
	local st, tp = iter_obj.iter.gen(iter_obj.key, iter_obj.state)
	return tp
end;

function iterate_over(iter_obj)
	local tp = nil
	local ret = {}
	local i = 0
	tp = iterator_next(iter_obj)
	while tp do
		ret[i] = tp
		i = i + 1
		tp = iterator_next(iter_obj)
	end
	return ret
end;

test_run:cmd("setopt delimiter ''");

--
--STATEMENT: REPL  DEL  REPL  REPL  REPL  REPL  REPL  REPL
--LSN:        7     8    9     10    11    12    13    14
--READ VIEW:  *          *                 *
--          \____/\________/\_________________/\___________/
--          merge   merge          merge           merge
--
space = box.schema.space.create('test', {engine = 'vinyl'})
pk = space:create_index('pk')
-- Open iterator after some statements and get one key from it to
-- open read view.
space:replace{1, 1}
space:replace{1, 2}
space:replace{1, 3}
space:replace{1, 4}
first_rv = create_iterator(pk)
iterator_next(first_rv)

space:delete{1}
space:replace{1, 5}
second_rv = create_iterator(pk)
iterator_next(second_rv)

space:replace{1, 6}
space:replace{1, 7}
space:replace{1, 8}
third_rv = create_iterator(pk)
iterator_next(third_rv)

space:replace{1, 9}
space:replace{1, 10}
box.snapshot()
-- Wait dump end
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end
space:select{}
space:drop()

--
--STATEMENT: UPS  UPS  UPS  UPS  UPS  UPS  UPS  UPS  UPS  UPS
--LSN:        5    6    7    8    9   10   11   12   13   14
--READ VIEW:       *                  *              *
--          \________/\_________________/\_____________/\_____/
--            squash         squash           squash     squash
--
space = box.schema.space.create('test', {engine = 'vinyl'})
pk = space:create_index('pk', {run_count_per_level = 10})
-- Create a run to turn off 'last level' optimizations of the
-- write iterator.
space:replace{1234}
box.snapshot()
-- Wait end of the dump
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end

space:upsert({1}, {{'!', 2, 1}})
-- Dump upsert, without converting into replace.
box.snapshot()
while pk:info().run_count ~= 2 do fiber.sleep(0.01) end

-- Dump 10 runs to fill the first level. And create some
-- activated iterators to create read views.
space:upsert({1}, {{'!', 2, 2}})
box.snapshot()
while pk:info().run_count ~= 3 do fiber.sleep(0.01) end

first_iter = create_iterator(pk)
iterator_next(first_iter)

space:upsert({1}, {{'!', 2, 3}})
box.snapshot()
while pk:info().run_count ~= 4 do fiber.sleep(0.01) end

space:upsert({1}, {{'!', 2, 4}})
box.snapshot()
while pk:info().run_count ~= 5 do fiber.sleep(0.01) end

space:upsert({1}, {{'!', 2, 5}})
box.snapshot()
while pk:info().run_count ~= 6 do fiber.sleep(0.01) end

space:upsert({1}, {{'!', 2, 6}})
box.snapshot()
while pk:info().run_count ~= 7 do fiber.sleep(0.01) end

second_iter = create_iterator(pk)
iterator_next(second_iter)

space:upsert({1}, {{'!', 2, 7}})
box.snapshot()
while pk:info().run_count ~= 8 do fiber.sleep(0.01) end

space:upsert({1}, {{'!', 2, 8}})
box.snapshot()
while pk:info().run_count ~= 9 do fiber.sleep(0.01) end

space:upsert({1}, {{'!', 2, 9}})
box.snapshot()
while pk:info().run_count ~= 10 do fiber.sleep(0.01) end

third_iter = create_iterator(pk)
iterator_next(third_iter)

space:upsert({1}, {{'!', 2, 10}})
-- Dump 11-th run to trigger compaction. During compact new run
-- will contain 4 squashed subsequences of lsns of the key {1}.
box.snapshot()
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end
space:select{}
space:drop()

--
--STATEMENT: REPL     DEL REPL     REPL
--LSN:        5       6    6        7
--READ VIEW:               *
--           \_______________/\_______/
--            \_____/\______/
--             merge  skip as
--                    optimized
--                     update
-- DEL and REPL with lsn 6 can be skipped for read view 6 for
-- secondary index, because they do not change secondary key.
--
-- Secondary index can skip optimized update even in a case
-- when there are older read views.
space = box.schema.space.create('test', {engine = 'vinyl'})
pk = space:create_index('pk', {run_count_per_level = 3, page_size = 80, range_size = 160})
sec = space:create_index('sec', {parts = {2, 'unsigned'}, run_count_per_level = 3, page_size = 80, range_size = 160})
-- Turn off 'last level' optimization.
space:replace{1234, 1234}
box.snapshot()
while sec:info().run_count ~= 1 do fiber.sleep(0.01) end

space:replace({1, 1})
box.snapshot()
while sec:info().run_count ~= 2 do fiber.sleep(0.01) end

-- Fix read view for first REPLACE.
first_iter = create_iterator(sec)
iterator_next(first_iter)

-- Write something not optimized to dump both secondary and
-- primary.
space:replace{0, 0}
-- Create optimized REPLACE+DELETE. They will be skipped during
-- secondary index dump.
space:update({1}, {{'!', 3, 3}})
get_res = nil
-- Create a transaction with read view. After compaction this
-- transaction will have to see optimized update from secondary
-- index.
f = fiber.create(function() box.begin() sec:get{1} fiber.sleep(1000) get_res = sec:get{1} box.commit() end)
box.snapshot()
while sec:info().run_count ~= 3 do fiber.sleep(0.01) end

-- Trigger compaction.
prev_run_count = sec:info().run_count
while sec:info().run_count >= prev_run_count do space:replace{1, 1, 1} box.snapshot() fiber.sleep(0.01) end

f:wakeup()
-- Wait get_res.
while get_res == nil do fiber.sleep(0.01) end
-- Secondary index skipped {1, 1, 3}, but contains key {1} in
-- read view and {1, 1, 3} in primary index. And get_res must be
-- {1, 1, 3}.
get_res
space:select{}
pk:select{}
sec:select{}
space:drop()
