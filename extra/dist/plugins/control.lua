local tntctl = require('ctl')
local utils  = require('ctl.utils')

local ffi     = require('ffi')
local fio     = require('fio')
local log     = require('log')
local pwd     = require('pwd')
local json    = require('json')
local yaml    = require('yaml')
local errno   = require('errno')
local fiber   = require('fiber')
local netbox  = require('net.box')
local socket  = require('socket')
local tclear  = require('table.clear')
local console = require('console')

local error = utils.error
local syserror = utils.syserror
local log_syserror = utils.log_syserror

ffi.cdef[[
    typedef int pid_t;
    int kill(pid_t pid, int sig);
    int isatty(int fd);
    int getppid(void);
]]

local TIMEOUT_INFINITY = 100 * 365 * 86400

local function stdin_isatty()
    return ffi.C.isatty(0) == 1
end

-- return linkmode, instance_name
-- return nil in case of error
local function find_instance_name(ctx, positional_arguments)
    local instance_name = nil
    if ctx.linkmode then
        instance_name = ctx.program_name
        local stat = fio.lstat(instance_name)
        if stat == nil then
            log_syserror("failed to stat file '%s'", instance_name)
            return nil
        end
        if not stat:is_link() then
            log.error("expected '%s' to be symlink", instance_name)
            return nil
        end
        return fio.basename(ctx.program_name, '.lua')
    end
    return fio.basename(table.remove(ctx.positional_arguments, 1), '.lua')
end

local function control_prepare_context(ctl, ctx)
    ctx = ctx or {}
    ctx.program_name = ctl.program_name
    ctx.usermode     = ctl.usermode
    ctx.linkmode     = ctl.linkmode
    ctx.default_cfg  = {
        pid_file   = ctl:get_config('default_cfg.pid_file'  ),
        wal_dir    = ctl:get_config('default_cfg.wal_dir'   ),
        memtx_dir  = ctl:get_config('default_cfg.memtx_dir' ),
        vinyl_dir  = ctl:get_config('default_cfg.vinyl_dir' ),
        log        = ctl:get_config('default_cfg.log'       ),
        background = ctl:get_config('default_cfg.background'),
    }
    ctx.instance_dir = ctl:get_config('instance_dir')

    ctx.instance_name = find_instance_name(ctx)
    if ctx.instance_name == nil then
        log.error('Expected to find instance name, got nothing')
        return false
    end

    ctx.pid_file_path = ctx.default_cfg.pid_file
    ctx.console_sock_path = fio.pathjoin(
        ctx.pid_file_path,
        ctx.instance_name .. '.control'
    )
    ctx.console_sock = 'unix/:' .. ctx.console_sock_path
    ctx.instance_path = fio.pathjoin(
        ctx.instance_dir,
        ctx.instance_name .. '.lua'
    )
    if not fio.stat(ctx.instance_path) then
        log.error("instance '%s' isn't found in %s",
                  ctx.instance_name .. '.lua',
                  ctx.instance_dir)
        return false
    end
    ctx.default_cfg.pid_file = fio.pathjoin(
        ctx.pid_file_path, ctx.instance_name .. '.pid'
    )
    ctx.default_cfg.log = fio.pathjoin(
        ctx.default_cfg.log, ctx.instance_name .. '.log'
    )

    ctx.default_cfg.wal_dir   = fio.pathjoin(ctx.default_cfg.wal_dir,   ctx.instance_name)
    ctx.default_cfg.memtx_dir = fio.pathjoin(ctx.default_cfg.memtx_dir, ctx.instance_name)
    ctx.default_cfg.vinyl_dir = fio.pathjoin(ctx.default_cfg.vinyl_dir, ctx.instance_name)

    if not ctx.usermode then
        ctx.username = ctl:get_config('default_cfg.username')
        local user_info = pwd.getpw(ctx.username)
        if user_info == nil then
            error('failed to find user "%s"', ctx.username)
        end
        if user_info.group == nil then
            error('failed to find group of user "%s"', ctx.username)
        end
        ctx.groupname = user_info.group.name
    end

    if ctx.command == 'eval' then
        ctx.eval_source = table.remove(ctx.positional_arguments, 1)
        if ctx.eval_source == nil and stdin_isatty() then
            log.error("expected source to evaluate, got nothing")
            return false
        end
    end

    local rv = os.execute("systemctl 2>/dev/null | grep '\\-\\.mount' " ..
                          "1>/dev/null 2>/dev/null") == 0
    ctx.with_systemd = rv and ctx.usermode
    ctx.under_systemd = ctx.with_systemd and ffi.C.getppid() > 1

    return true
end

--------------------------------------------------------------------------------

local function read_file(filename)
    local file = fio.open(filename, {'O_RDONLY'})
    if file == nil then
        return nil
    end

    local buf = {}
    local i = 1
    while true do
        buf[i] = file:read(1024)
        if buf[i] == nil then
            return nil
        elseif buf[i] == '' then
            break
        end
        i = i + 1
    end
    return table.concat(buf)
end

local function execute_remote(uri, code)
    local remote = netbox.connect(uri, {
        console = true, connect_timeout = 1
    })
    if remote == nil then
        return nil
    end
    return true, remote:eval(code)
end

local function check_file(path)
    local rv, err = loadfile(path)
    if rv == nil then
        return err
    end
    return nil
end

-- shift argv to remove 'tarantoolctl' from arg[0]
local function shift_argv(arg, argno, argcount)
    local new_arg = {}
    for i = argno, 128 do
        if arg[i + argcount] == nil then
            break
        end
        new_arg[i] = arg[i + argcount]
    end
    new_arg[-1] = arg[-1]
    tclear(arg)
    for k, v in pairs(new_arg) do
        arg[k] = v
    end
end

-- Removes leading and trailing whitespaces
local function string_trim(str)
    return str:gsub("^%s*(.-)%s*$", "%1")
end

local function logger_parse(logstr)
    -- syslog
    if logstr:find("syslog:") then
        logstr = string_trim(logstr:sub(8))
        local args = {}
        logstr:gsub("([^,]+)", function(keyval)
            keyval:gsub("([^=]+)=([^=]+)", function(key, val)
                args[key] = val
            end)
        end)
        return 'syslog', args
    -- pipes
    elseif logstr:find("pipe:")   then
        logstr = string_trim(logstr:sub(6))
        return 'pipe', logstr
    elseif logstr:find("|")       then
        logstr = string_trim(logstr:sub(2))
        return 'pipe', logstr
    -- files
    elseif logstr:find("file:")   then
        logstr = string_trim(logstr:sub(6))
        return 'file', logstr
    else
        logstr = string_trim(logstr)
        return 'file', logstr
    end
end

local function syserror_format(fmt, ...)
    local stat = true
    if select('#', ...) > 0 then
        stat, fmt = pcall(string.format, fmt, ...)
    end
    if not stat then
        error(fmt, 2)
    end
    return string.format('[errno %s] %s: %s', errno(), fmt, errno.strerror())
end

-- It's not 100% result guaranteed function, but it's ok for most cases
-- Won't help in multiple race-conditions
-- Returns nil if Tarantool isn't started,
-- Returns PID if Tarantool isn't started
-- Returns false, error if error occured
local function check_start(pid_file)
    log.verbose('Checking Tarantool with "%s" pid_file', pid_file)
    local fh = fio.open(pid_file, 'O_RDONLY')
    if fh == nil then
        if errno() == errno.ENOENT then
            return nil
        end
        return false, syserror_format("failed to open pid_file %s", pid_file)
    end

    local raw_pid = fh:read(64); fh:close()
    local pid     = tonumber(raw_pid)

    if pid == nil or pid <= 0 then
        return false, string.format(
            "bad contents of pid file %s: '%s'",
            pid_file, raw_pid
        )
    end

    if ffi.C.kill(pid, 0) < 0 then
        if errno() == errno.ESRCH then
            return nil
        end
        return false, syserror_format("kill of %d failed", pid)
    end
    return pid
end

-- Additionally check that we're able to write into socket
local function check_start_full(socket_path, pid_file)
    local stat, pid_check = check_start(pid_file)
    if not stat then
        return stat, pid_check
    end

    if not fio.stat(socket_path) then
        return false, "Tarantool process exists, but control socket doesn't"
    end
    local s = socket.tcp_connect('unix/', socket_path)
    if s == nil then
        return false, syserror_format(
            "Tarantool process exists, but connection to console socket failed"
        )
    end

    local check_cmd = "return 1\n"
    if s:write(check_cmd) == -1 then
        return false, syserror_format(
            "failed to write %s bytes to control socket", check_cmd
        )
    end
    if s:read({ '[.][.][.]' }, 2) == -1 then
        return false, syserror_format("failed to read until delimiter '...'")
    end

    return stat
end

local function mkdir(ctx, dir)
    log.info("recreating directory '%s'", dir)
    if not fio.mkdir(dir, tonumber('0750', 8)) then
        log_syserror("failed mkdir '%s'", dir)
        return false
    end

    if not ctx.usermode and not fio.chown(dir, ctx.username, ctx.groupname) then
        log_syserror("failed chown (%s, %s, %s)", ctx.username,
                     ctx.groupname, dir)
        return false
    end
    return true
end

local function mk_default_dirs(ctx, cfg)
    local init_dirs = {
        fio.dirname(cfg.pid_file),
        cfg.wal_dir,
        cfg.memtx_dir,
        cfg.vinyl_dir,
    }
    local log_type, log_args = logger_parse(cfg.log)
    if log_type == 'file' then
        table.insert(init_dirs, fio.dirname(log_args))
    end
    for _, dir in ipairs(init_dirs) do
        if not fio.stat(dir) and not mkdir(ctx, dir) then
            return false
        end
    end
    return true
end

local function wrapper_cfg_closure(ctx)
    local orig_cfg    = box.cfg
    local default_cfg = ctx.default_cfg

    return function(cfg)
        for i, v in pairs(default_cfg) do
            if cfg[i] == nil then
                cfg[i] = v
            end
        end

        -- force these startup options
        cfg.pid_file = default_cfg.pid_file
        if os.getenv('USER') ~= default_cfg.username then
            cfg.username = default_cfg.username
        else
            cfg.username = nil
        end
        if cfg.background == nil then
            cfg.background = not ctx.under_systemd
        end

        if mk_default_dirs(ctx, cfg) == false then
            error('failed to create instance directories')
        end
        local success, data = pcall(orig_cfg, cfg)
        if not success then
            log.error("Configuration failed: %s", data)
            if type(cfg) ~= 'function' then
                local log_type, log_args = logger_parse(cfg.log)
                if log_type == 'file' and fio.stat(log_args) then
                    os.execute('tail -n 10 ' .. log_args)
                end
            end
            os.exit(1)
        end

        fiber.name(ctx.instance_name)
        log.info("Run console at '%s'", ctx.console_sock_path)
        console.listen(ctx.console_sock)
        -- gh-1293: members of `tarantool` group should be able to do `enter`
        local mode = '0664'
        if not fio.chmod(ctx.console_sock_path, tonumber(mode, 8)) then
            log_syserror("can't chmod(%s, %s)", ctx.console_sock_path, mode)
        end

        return data
    end
end

local function start(ctx)
    local function basic_start(ctx)
        log.info("Starting instance '%s'...", ctx.instance_name)
        if ctx.under_systemd then
            local cmd = "systemctl start tarantool@" .. ctx.instance_name
            log.info("Forwarding to '%s'", cmd)
            os.execute(cmd)
            return 0
        end
        local err = check_file(ctx.instance_path)
        if err ~= nil then
            error("Failed to check instance file '%s'", err)
        end
        log.verbose("Instance file is OK")
        local pid, stat = check_start(ctx.default_cfg.pid_file)
        if type(pid) == 'number' then
            error("The daemon is already running with PID %s", pid)
        elseif pid == false then
            log.error("Failed to determine status of instance '%s'",
                      ctx.instance_name)
            error(stat)
        end
        log.verbose("Instance '%s' wasn't started before", ctx.instance_name)
        box.cfg = wrapper_cfg_closure(ctx)
        require('title').update({
            script_name = ctx.instance_path,
            __defer_update = true
        })
        shift_argv(arg, 0, 2)
        -- it may throw error, but we will catch it in start() function
        dofile(ctx.instance_path)
        return 0
    end

    local stat, rv = pcall(basic_start, ctx)
    if rv ~= 0 then stat = false end
    if not stat then
        log.error("Failed to start Tarantool instance '%s'", ctx.instance_name)
        if type(rv) == 'string' then
            if rv:match('Please call box.cfg') then
                rv = rv:split('\n')
            end
            local rv_debug = nil
            if rv:match(':%d+: ') then
                rv_debug, rv = rv:match('(.+:%d+): (.+)')
            end
            log.verbose("Error occured at %s", rv_debug)
            log.error(rv)
        end
        if type(box.cfg) ~= 'function' then
            local log_type, log_args = logger_parse(ctx.default_cfg.log)
            if log_type == 'file' and fio.stat(log_args) then
                os.execute('tail -n 10 ' .. log_args)
            end
        end
        return false
    end
    return true
end

local function stop(ctx)
    local function basic_stop(ctx)
        log.info("Stopping instance '%s'...", ctx.instance_name)
        if ctx.under_systemd then
            local cmd = "systemctl stop tarantool@" .. ctx.instance_name
            log.info("Forwarding to '%s'", cmd)
            os.execute(cmd)
            return 0
        end
        if fio.stat(ctx.console_sock_path) then
            fio.unlink(ctx.console_sock_path)
        end

        local pid_file = ctx.default_cfg.pid_file
        if fio.stat(pid_file) == nil then
            log.info("Process is not running (pid: %s)", pid_file)
            return 0
        end

        local f = fio.open(pid_file, 'O_RDONLY')
        if f == nil then
            syserror("failed to read pid file %s", pid_file)
        end

        local raw_pid = f:read(64); f:close()
        local pid     = tonumber(raw_pid)

        if pid == nil or pid <= 0 then
            if fio.unlink(pid_file) == nil then
                log_syserror('failed to unlink file %s', pid_file)
            end
            error("bad contents of pid file %s: '%s'", pid_file, raw_pid)
        end

        if ffi.C.kill(pid, 15) < 0 then
            log_syserror("failed to kill process %d", pid)
        end

        if fio.stat(pid_file) then
            fio.unlink(pid_file)
        end
        return 0
    end

    local stat, rv = pcall(basic_stop, ctx)
    if rv ~= 0 then stat = false end
    if not stat then
        log.error("Failed to stop Tarantool instance '%s'", ctx.instance_name)
        if type(rv) == 'string' then
            local rv_debug = nil
            if rv:match(':%d+: ') then
                rv_debug, rv = rv:match('(.+:%d+): (.+)')
            end
            log.verbose("Error occured at %s", rv_debug)
            log.error(rv)
        end
        return false
    end
    return true
end

local function restart(ctx)
    local function basic_restart(ctx)
        local err = check_file(ctx.instance_path)
        if err ~= nil then
            error("Failed to check instance file '%s'", err)
        end
        if not stop(ctx) then
            return 1
        end
        fiber.sleep(1)
        if not start(ctx) then
            return 1
        end
        return 0
    end

    local stat, rv = pcall(basic_restart, ctx)
    if rv ~= 0 then stat = false end
    if not stat then
        log.error("Failed to restart Tarantool instance '%s'", ctx.instance_name)
        if type(rv) == 'string' then
            local rv_debug = nil
            if rv:match(':%d+: ') then
                rv_debug, rv = rv:match('(.+:%d+): (.+)')
            end
            log.verbose("Error occured at %s", rv_debug)
            log.error(rv)
        end
        return false
    end
    return true
end

local function logrotate(ctx)
    local function basic_logrotate(ctx)
        log.info("Rotating log of Tarantool instance '%s'...",
                 ctx.instance_name)
        local stat, pid_check = check_start_full(
            ctx.console_sock_path, ctx.default_cfg.pid_file
        )
        if stat == nil then
            error("instance '%s' isn't started", ctx.instance_name)
        elseif not stat then
            log.error("Failed to determine status of instance '%s'",
                      ctx.instance_name)
            error(pid_check)
        end

        local s = socket.tcp_connect('unix/', ctx.console_sock_path)
        if s == nil then
            syserror("failed to connect to instance '%s'",
                            ctx.instance_name)
        end

        local rotate_cmd = [[
            require('log'):rotate()
            require('log').info("Rotate log file")
        ]]
        if s:write(rotate_cmd) == -1 then
            syserror("failed to write %s bytes", #rotate_cmd)
        end
        if s:read({ '[.][.][.]' }, 2) == -1 then
            syserror("failed to read until delimiter '...'")
        end
        return 0
    end

    local stat, rv = pcall(basic_logrotate, ctx)
    if rv ~= 0 then stat = false end
    if not stat then
        log.error("Failed to rotate log of instance '%s'",
                  ctx.instance_name)
        if type(rv) == 'string' then
            local rv_debug = nil
            if rv:match(':%d+: ') then
                rv_debug, rv = rv:match('(.+:%d+): (.+)')
            end
            log.verbose("Error occured at %s", rv_debug)
            log.error(rv)
        end
        return false
    end
    return true
end

local function status(ctx)
    local pid_file = ctx.default_cfg.pid_file
    local console_sock = ctx.console_sock_path

    if ctx.under_systemd then
        local cmd = "systemctl status tarantool@" .. ctx.instance_name
        log.info("Forwarding to '%s'", cmd)
        os.execute(cmd)
        return 0
    end

    if fio.stat(pid_file) == nil then
        if errno() == errno.ENOENT then
            log.info('%s is stopped (pid file does not exist)',
                     ctx.instance_name)
            return false
        end
        log_syserror("can't access pid file %s: %s", pid_file)
        return false
    end

    if fio.stat(console_sock) == nil and errno() == errno.ENOENT then
        log.info("pid file exists, but the control socket (%s) doesn't",
                 console_sock)
        return false
    end

    local s = socket.tcp_connect('unix/', console_sock)
    if s == nil then
        if errno() ~= errno.EACCES then
            log_syserror("can't access control socket '%s'", console_sock)
            return false
        end
        return true
    end
    s:close()

    log.info('%s is running (pid: %s)', ctx.instance_name, pid_file)
    return true
end

local function eval(ctx)
    local function basic_eval(ctx)
        local console_sock_path = ctx.console_sock_path
        local code = nil
        if not ctx.eval_source then
            code = io.stdin:read("*a")
        else
            code = read_file(ctx.eval_source)
            if code == nil then
                syserror("failed to open '%s'", ctx.eval_source)
            end
        end

        assert(code ~= nil, "Check that we've successfully loaded file")

        if fio.stat(console_sock_path) == nil then
            error(
                "pid file exists, but the control socket (%s) doesn't",
                console_sock_path
            )
        end

        local status, full_response = execute_remote(ctx.console_sock, code)
        if status == false then
            error(
                "control socket exists, but Tarantool doesn't listen on it"
            )
        end
        local error_response = yaml.decode(full_response)[1]
        if type(error_response) == 'table' and error_response.error then
            error(error_response.error)
        end

        io.stdout:write(full_response)
        io.stdout:flush()
        return 0
    end

    local stat, rv = pcall(basic_eval, ctx)
    if rv ~= 0 then stat = false end
    if not stat then
        if type(rv) == 'string' or type(rv) == 'cdata' then
            rv = tostring(rv)
            log.error("Failed eval command on instance '%s'", ctx.instance_name)
            local rv_debug = nil
            if rv:match(':%d+: ') then
                rv_debug, rv = rv:match('(.+:%d+): (.+)')
            end
            log.verbose("Error occured at %s", rv_debug)
            log.error(rv)
        end
        return false
    end
    return true
end

local function enter(ctx)
    local console_sock_path = ctx.console_sock_path
    if fio.stat(console_sock_path) == nil then
        log.error("Failed to enter into instance '%s'", ctx.instance_name)
        log_syserror("can't connect to %s", console_sock_path)
        if not ctx.usermode and errno() == errno.EACCES then
            log.error("please, add $USER to group '%s' with command",
                      ctx.group_name)
            log.error('usermod -a -G %s $USER', ctx.group_name)
        end
        return false
    end

    local cmd = string.format(
        "require('console').connect('%s', { connect_timeout = %s })",
        ctx.console_sock, TIMEOUT_INFINITY
    )

    console.on_start(function(self) self:eval(cmd) end)
    console.on_client_disconnect(function(self) self.running = false end)
    console.start()
    return true
end

local function check(ctx)
    log.error("Checking instance file '%s'...", ctx.instance_path)
    local rv = check_file(ctx.instance_path)
    if rv ~= nil then
        log.info("Failed to check instance file: %s", ctx.instance_path)
        return false
    end
    log.info("Instance file is OK")
    return true
end

tntctl:register_config('default_cfg.pid_file', {
    default = '/var/run/tarantool',
    type    = 'string',
})
tntctl:register_config('default_cfg.wal_dir', {
    default = '/var/lib/tarantool',
    type    = 'string',
})
tntctl:register_config('default_cfg.memtx_dir', {
    deprecated = 'default_cfg.snap_dir',
    default    = '/var/lib/tarantool',
    type       = 'string',
})
tntctl:register_config('default_cfg.vinyl_dir', {
    default = '/var/lib/tarantool',
    type    = 'string',
})
tntctl:register_config('default_cfg.log', {
    deprecated = 'default_cfg.logger',
    default    = '/var/lib/tarantool',
    type       = 'string',
})
tntctl:register_config('default_cfg.username', {
    default = 'tarantool',
    type    = 'string',
})
tntctl:register_config('default_cfg.background', {
    default = true,
    type    = 'boolean',
})
tntctl:register_config('instance_dir', {
    default = '/etc/tarantool/instances.enabled',
    type    = 'string',
})

local control_library = tntctl:register_library('control')
control_library:register_prepare('control', control_prepare_context)
control_library:register_method('start', start, {
    help = {
        description = [=[
            Start Tarantool instance if it's not already started. Tarantool
            instance should be maintained using tarantoolctl only
        ]=],
        header   = "%s start <instance_name>",
        linkmode = "%s start"
    },
    exiting = false,
})
control_library:register_method('stop', stop, {
    help = {
        description = [=[
            Stop Tarantool instance if it's not already stopped
        ]=],
        header   = "%s stop <instance_name>",
        linkmode = "%s stop",
    },
    exiting = true,
})
control_library:register_method('status', status, {
    help = {
        description = [=[
            Show status of Tarantool instance. (started/stopped)
        ]=],
        header   = "%s status <instance_name>",
        linkmode = "%s status",
    },
    exiting = true,
})
control_library:register_method('restart', restart, {
    help = {
        description = [=[
            Stop and start Tarantool instance (if it's already started, fail
            otherwise)
        ]=],
        header   = "%s restart <instance_name>",
        linkmode = "%s restart",
    },
    exiting = true,
})
control_library:register_method('logrotate', logrotate, {
    help = {
        description = [=[
            Rotate log of started Tarantool instance. Works only if logging is
            set into file. Pipe/Syslog aren't supported.
        ]=],
        header   = "%s logrotate <instance_name>",
        linkmode = "%s logrotate",
    },
    exiting = false,
})
control_library:register_method('check', check, {
    help = {
        description = [=[
            Check instance script for syntax errors
        ]=],
        header   = "%s check <instance_name>",
        linkmode = "%s check",
    },
    exiting = true,
})
control_library:register_method('enter', enter, {
    help = {
        description = [=[
            Enter interactive Lua console of instance
        ]=],
        header   = "%s enter <instance_name>",
        linkmode = "%s enter",
    },
    exiting = true,
})
control_library:register_method('eval', eval, {
    help = {
        description = [=[
            Evaluate local file on Tarantool instance (if it's already started,
            fail otherwise)
        ]=],
        header = {
            "%s eval <instance_name> <lua_file>",
            "<command> | %s eval <instance_name>"
        },
        linkmode = {
            "%s eval <lua_file>",
            "<command> | %s eval"
        },
    },
    exiting = true,
})
tntctl:register_alias('start',     'control.start'    )
tntctl:register_alias('stop',      'control.stop'     )
tntctl:register_alias('restart',   'control.restart'  )
tntctl:register_alias('logrotate', 'control.logrotate')
tntctl:register_alias('status',    'control.status'   )
tntctl:register_alias('eval',      'control.eval'     )
tntctl:register_alias('reload',    'control.eval'     , { deprecated = true })
tntctl:register_alias('enter',     'control.enter'    )
tntctl:register_alias('check',     'control.check'    )

local function connect(ctx)
    local function basic_connect(ctx)
        if ctx.connect_code then
            local status, full_response = execute_remote(ctx.remote_host,
                                                        ctx.connect_code)
            if not status then
                error('failed to connect to Tarantool')
            end
            local error_response = yaml.decode(full_response)[1]
            if type(error_response) == 'table' and error_response.error then
                log.error("Error, while executing remote command:")
                log.info(error_response.error)
            end
            log.info(full_response)
            return 0
        end
        -- Otherwise we're starting console
        console.on_start(function(self)
            local status, reason = pcall(console.connect,
                ctx.remote_host, { connect_timeout = TIMEOUT_INFINITY }
            )
            if not status then
                self:print(reason)
                self.running = false
            end
        end)

        console.on_client_disconnect(function(self) self.running = false end)
        console.start()
        return 0
    end

    local stat, rv = pcall(basic_connect, ctx)
    if rv ~= 0 then stat = false end
    if not stat then
        log.error("Failed to connect to remote instance '%s'", ctx.remote_host)
        if type(rv) == 'string' then
            local rv_debug = nil
            if rv:match(':%d+: ') then
                rv_debug, rv = rv:match('(.+:%d+): (.+)')
            end
            log.verbose("Error occured at %s", rv_debug)
            log.error(rv)
        end
        return false
    end
    return true
end

local function console_prepare_context(ctl, ctx)
    if ctx.command == 'connect' then
        ctx.connect_endpoint = table.remove(ctx.positional_arguments, 1)
        if ctx.connect_endpoint == nil then
            log.error("Expected URI to connect to")
            return false
        end
        if not stdin_isatty() then
            ctx.connect_code = io.stdin:read("*a")
            if not ctx.connect_code or ctx.connect_code == '' then
                log.error("Failed to read from stdin")
                return false
            end
        else
            ctx.connect_code = nil
        end
    end
    return true
end

local console_library = tntctl:register_library('console', { weight = 20 })
console_library:register_prepare('connect', console_prepare_context)
console_library:register_method('connect', connect, {
    help = {
        description = [=[
            Connect to Tarantool instance on admin/console port. Supports both
            TCP/Unix sockets
        ]=],
        header = {
            "%s connect <instance_uri>",
            "<command> | %s connect <instance_uri>"
        },
    },
    exiting = true,
})
tntctl:register_alias('connect', 'console.connect')
