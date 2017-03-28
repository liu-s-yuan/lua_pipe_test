local pipe_pipe = require("dep.pipe.pipe")
local pipe_reader = require("dep.pipe.reader")
local pipe_write = require("dep.pipe.writer")
local resty_md5 = require("resty.md5")
local resty_string = require("resty.string")

local _M = {}

local function make_md5_filter(rst)

    local md5 = resty_md5:new()
        if not md5 then
            return nil, 'md5_err', "failed to create md5 object"
        end

    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)

        if rbufs[1] == '' then
            local digest = md5:final()
            rst[1] = resty_string.to_hex(digest)
        end

        local ok = md5:update(rbufs[1])
        if not ok then
            return nil, 'md5_err', "failed to add data"
        end
    end
end

local function get_md5_val(fpath, mode)

    local md5 = resty_md5:new()
    if not md5 then
        return nil, 'md5_err', "failed to create md5 object"
    end

    fp, err_msg = io.open(fpath, mode or 'r')
    assert(fp ~= nil, 'file_error'..(err_msg or ''))

    while true do
        local data = fp:read(1024*1024)
        assert(data ~= nil, 'file_error'..',read data error, file path: '..fpath)

        if (data == '') then
            break
        end

        local ok = md5:update(data)
        if not ok then
            return nil, 'md5_err', "failed to add data"
        end
    end

    fp:close()
    fp = nil

    local digest = md5:final()
    local res = resty_string.to_hex(digest)

    return res
end

function test1()

    local writers = {}
    for _, fpath in ipairs({
        {'/tmp/t1.out'},
        {'/tmp/t2.out'},
        {'/tmp/t3.out'},
        --{'/tmp/t4.out', 'r'},
        {'/tmp/t4.out', 'w'},
        {'/tmp/t6.out', 'a'},
        --{'/tmp/aaa/t7.out'},
        }) do

        local file_writer = pipe_write.make_file_writer(fpath[1], fpath[2])
        table.insert(writers, file_writer)
    end

    local case = {
        --ips, port, verb, uri, opts

        {{'148.251.24.173'}, 80, 'GET', '/ftp/lua-5.3.4.tar.gz', {headers={Host='www.lua.org'}}},
        {{'148.251.24.173'}, 8080, 'GET', '/ftp/lua-5.3.4.tar.gz', {headers={Host='www.lua.org'}}},
        {{'148.251.24.173'}, 8081, 'GET', '/ftp/lua-5.3.4.tar.gz', {headers={Host='www.lua.org'}}},
        {{'148.251.24.173'}, 9080, 'GET', '/ftp/lua-5.3.4.tar.gz', {headers={Host='www.lua.org'}}},
        {{'148.251.24.173'}, 80, 'POST', '/ftp/lua-5.3.4.tar.gz', {headers={Host='www.lua.org'}}},
        {{'148.251.24.173'}, 80, '', '/ftp/lua-5.3.4.tar.gz', {headers={Host='www.lua.org'}}},
        }

    for _, ca in ipairs(case) do

        local md5_http = {}
        local md5_filter = make_md5_filter(md5_http)

        local http_reader = pipe_reader.make_http_reader(ca[1], ca[2], ca[3], ca[4], ca[5])

        --local pipe = pipe_pipe:new({http_reader}, writers, md5_filter)
        local pipe = pipe_pipe:new({http_reader}, writers)

        local is_running = function() return true end

        local rst, err_code, err_msg = pipe:pipe(is_running)

        if rst ~= nil then
            return nil, err_code, err_msg
        end

        local t1_md5_val
        t1_md5_val, err_code, err_msg = get_md5_val('/tmp/t1')
        if err_code ~= nil then
            return _, err_code, err_msg
        end

        assert(t1_md5_val == md5_http, 'md5_error'..', t1 is not equal to md5_http')

    end
end

return _M
