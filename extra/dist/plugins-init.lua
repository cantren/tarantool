local fio = require('fio')
local path = fio.dirname(debug.getinfo(1).source:sub(2))

if not require('ctl').libraries['control'] then
    dofile(fio.pathjoin(path, 'control.lua'))
    dofile(fio.pathjoin(path, 'xlog.lua'))
end
