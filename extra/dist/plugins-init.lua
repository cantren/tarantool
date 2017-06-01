local fio = require('fio')
local path = fio.dirname(debug.getinfo(1).source:sub(2))

local ctl = require('ctl')

if not ctl.libraries['control'] then
    dofile(fio.pathjoin(path, 'control.lua'))
    dofile(fio.pathjoin(path, 'xlog.lua'))
end
