#!/usr/bin/lua
-------------------------------------------------------------------------------
--
-- @script: lua-mapreduce-server.lua
--
-- @author:  Rohit Joshi
--
-- @copyright Joshi Ventures LLC ? 2012
--
-- @license Apache License, Version 2.0
--
-- VERSION HISTORY:
-- 1.0 9/13/2012 - Initial release
--
-------------------------------------------------------------------------------
-- Purpose: Lua Map-Reduce server

require "copas"
require "logging.console"
local socket = require("socket")

--- utils.lua
require "utils/utils"
--- requires serialize.lua
require "utils/serialize"

local logger = logging.console()
logger:setLevel (logging.WARN)

local server_socket
local map_tasks = {}
local finalfn
local map_results = {}
local reduce_results = {}
local task_file_content = nil
local taskfn

local tasks_completed = false
local co_reducefn
local co_taskfn
local co_finalfn



------------------------------------------------------------------------------
--- Get Reduce  task
-- @param socket clinet
-- @param number of messages to send
------------------------------------------------------------------------------
local function get_reduce_task()
    logger:debug("Getting reduce task from map results " .. #map_results)
	for key, value in pairs(map_results) do
		logger:debug("map_results task:" .. key .. ", value:" .. table.tostring(value))
		coroutine.yield(key, value)

	end

end


------------------------------------------------------------------------------
--- Send map task
-- @param socket clinet
-- @param number of messages to send
------------------------------------------------------------------------------
local function send_map_task(client, key, content)

	local t = {}
	 t[key]=content

	 local value = serialize(t)


	local len = string.len(value)
	local cmd_t = {}
	cmd_t['c'] = "map"
	cmd_t['k']=key
	cmd_t['l'] = len
	local msg = serialize(cmd_t) .. "\r\n"

    logger:debug("Sending map command " .. msg)
	local bytesSent, status = client:send(msg)
	if(status == "closed") then

		logger:error("send_map_task():Connection closed by foreign host.")
		return status;
	end
	local bytesSent, status = client:send(value)
	logger:debug("Map task  payload length " .. len .. ", bytes sent " .. bytesSent )

	--receive OK
    local data, status = client:receive("*l")

	if(status == "closed") then
			logger:error("send_map_task():Connection closed by foreign host.")
			return status;
	end
    logger:debug("Received send result:" .. data)

	return status
end

------------------------------------------------------------------------------
--- Send map task
-- @param socket clinet
-- @param number of messages to send
------------------------------------------------------------------------------
local function send_reduce_task(client, key, v)
	logger:debug("Sending reduce task with key:" .. key)
	 local t = {}
	 local kv = {}

	 local value = serialize(v)



	local len = string.len(value)
	local cmd_t = {}
	cmd_t['c'] = "reduce"
	cmd_t['k']=key
	cmd_t['l'] = len
	local msg = serialize(cmd_t) .. "\r\n"

    logger:debug("Sending send_reduce_task command:" .. msg)
	local bytesSent, status = client:send(msg)
	if(status == "closed") then

		logger:error("send_reduce_task():Connection closed by foreign host.")
		return status;
	end
	local bytesSent, status = client:send(value)
	if(status == "closed") then
		logger:debug("send_reduce_task():Connection closed by foreign host.")
		return status;
	end

	return status
end

------------------------------------------------------------------------------
--- Receive map results
-- @param socket clinet
-- @param number of messages to send
------------------------------------------------------------------------------
local function receive_map_result(client, key)

    logger:debug("receive_map_result with key:" .. key)
    local completed_resp = "map:completed:" .. key .. "\r\n"
	local data
	local s
    repeat
		 local data, status = client:receive("*l")

		if(status == "closed") then

				logger:error("Connection closed by foreign host.")
				return status, key;
		end
         if(status) then logger:debug("receive_map_result Status:" .. status) end
		if(data ~= nil) then

			if(string.starts(data, "map:completed")) then
				return "ok", key
			end

			local map_result = loadstring( data)()

			local m_key = map_result["k"]
			local m_kv = map_result["v"]
          -- for m_key, m_kv in pairs(map_result) do
			--logger:debug("k:" .. m_key )
		    for k , v in pairs(m_kv) do
				if(k ~= nil and v ~= nil) then
					logger:debug("adding map result  k:" .. k .. "v:" .. v)
					if(map_results[k] == nil) then
						map_results[k] = { v }
					else
						table.insert(map_results[k], v)
					end

				end
			end
		 --  end
		end
		s = status
	until (  data == nil or data == completed_resp)
	--process reduce task

	return "ok", key
end

------------------------------------------------------------------------------
--- Receive reduce results
-- @param socket clinet
-- @param number of messages to send
------------------------------------------------------------------------------
local function receive_reduce_result(client, key)

    local completed_resp = "reduce:completed:" .. key
	logger:debug("Receiving Reduce Result for key:" .. key)
    repeat
		local data, status = client:receive("*l")
		if(status == "closed") then
			logger:debug("Lost task " .. key)
			logger:debug("Connection closed by foreign host.")
			return status;
		end

		if(string.starts(data, "reduce:completed")) then
				return "ok", key
			end
		local reduce_result = loadstring(data)()
		local k = reduce_result['k']
		local v = reduce_result['v']
		if(k ~= nil and v ~= nil) then
		   logger:debug("adding reduce result  k:" .. k .. ", v:" .. v)
		   reduce_results[k] =  v
		end
	until (  data == nil or data == completed_resp)
	return "ok", key
end
------------------------------------------------------------------------------
--- Run client handler
-- @param socket clinet
-- @param number of messages to send
------------------------------------------------------------------------------
local function client_handler(skt, host, port)

	local peername =  host .. ":" .. port
	logger:info ("Received client connection  from '%s':" , peername)
	skt:setoption('tcp-nodelay', true)
	-- skt:settimeout(1)
	local client = copas.wrap(skt)

	if(tasks_completed) then
		logger:info("No pending task exists. waiting to receive new task")
		while tasks_completed do
			socket:select(nil, nil, 1)
		end
	end

	local task_t = {}
    task_t['c'] = "taskfile"
	task_t['l'] = string.len(task_file_content)
	local task_t_str = serialize(task_t)
	local bytes_sent, status = client:send(task_t_str .. "\r\n")
	if(status == "closed") then
		logger:error("Connection closed by foreign host. Failed to send taskfile command")
		return;
	end
    local bytes_sent, status = client:send(task_file_content)
	if(status == "closed") then
		logger:error("Connection closed by foreign host. Failed to send task_file_content")
		return;
	end
	local taskfile_resp, status = client:receive("*l")
	if(status == "closed") then
		logger:error("Connection closed by foreign host. Failed to send task_file_content")
		return;
	end
	logger:debug("Received response for task_file sent:" .. taskfile_resp)
	logger:info("Taskfile is sent to client %s successfully", peername)
	local cl = os.clock()


	-- get all map tasks
	logger:info("Sending map task to client '%s'", peername)

	repeat

		local ok , key, content = coroutine.resume(co_taskfn); --get task

		if(ok and key ~= nil and content ~= nil) then
		logger:debug("\n\n**********************Got the map task with key:" .. key  .. "**********************\n\n")
			local status = send_map_task(client, key, content)
			if(status == "closed") then
				logger:error("Lost task " .. key)
				logger:error("Connection closed by foreign host.")
				return;
			end

			local status, mapr_key = receive_map_result(client, key)
			if(status == "closed") then
				logger:error("Lost task " .. key)
				logger:error("Connection closed by foreign host.")
				return;
			end


		end

	until (coroutine.status(co_taskfn) == 'dead' or ok ~= true or content == nil or status~="ok")
    logger:info("All map task processing is completed and map results received successfully")

	logger:info("Sending reduce tasks to clinet '%s'", peername)

    repeat

	    logger:debug("get reduce task")
		local ok , key, content = coroutine.resume(co_reducefn)
		if(ok and key ~= nil and content ~= nil) then
            logger:debug("Got the reduce task:" .. key)
			local status = send_reduce_task(client, key, content)
			if(status == "closed") then
				logger:error("Lost task " .. key)
				logger:error("Connection closed by foreign host.")
				return;
			end

			local status, r_key, r_data = receive_reduce_result(client, key)
			if(status == "closed") then
				logger:error("Lost task " .. key)
				logger:error("Connection closed by foreign host.")
				return;
			end


		end

	until (coroutine.status(co_reducefn) == 'dead' or ok ~= true or content == nil or status=="ok")

	logger:info("All reduce tasks results are received successfully")
	print("Total time for map-reduce:" .. os.clock() - cl)


	logger:info("Calling final task for client %s", peername)

    repeat


		local ok  = coroutine.resume(co_finalfn, reduce_results)
	until (coroutine.status(co_finalfn) == 'dead' or ok ~= true)

    logger:info("Completed all tasks. Closing client connect:" .. peername)
	tasks_completed = true
	skt:close()

	return;
end

------------------------------------------------------------------------------
--- Validate arguments
-- @return host, port and task_file
------------------------------------------------------------------------------
local function validate_args(arg)
   local usage = "Usage lua-mapreduce-server.lua -t <task file name>  [ -s server  -p port -l loglevel]"
   local opts = getopt( arg, "hspdtl" )

   if(opts["h"] ~= nil) then
       print(usage)
       return;
   end
   local host = opts["s"]
   if(host == nil) then host = "127.0.0.1" end
   local port = opts["p"]
   if( port == nil ) then port = "10000" end
   local task_file = opts["t"]
   if(task_file == nil) then
	logger:debug("task file is not defined. Must specify option -t")
	logger:debug(usage)
	return
   end

   local loglevel = opts["l"]
	if(loglevel == nil) then
		loglevel = "warn"
	elseif(loglevel ~= "debug" and loglevel ~= "info" and loglevel ~= "warn" and loglevel ~= "error") then
		print("Error: Invalid loglevel: " .. loglevel .. ". Valid options are debug, info, warn or error")
		return;
	end

   return host, port, task_file, loglevel

end
------------------------------------------------------------------------------
--- Read task file and validate it
-- @return content of the task file
------------------------------------------------------------------------------
local function read_task_file(file)
	local f = assert(io.open(file, "rb"))
    local content = f:read("*all")
    f:close()
	logger:debug("Validate content of the task file: " .. file)
	local f = assert(loadstring(content))()
	local mr_t = mapreducefn()
	finalfn = mr_t.finalfn
	taskfn = mr_t.taskfn

	co_reducefn = coroutine.create(get_reduce_task)
	co_taskfn = coroutine.create(taskfn)
	co_finalfn = coroutine.create(finalfn)
    return content

end

------------------------------------------------------------------------------
--- main function (entry point)
-- @return content of the task file
------------------------------------------------------------------------------
local function main()
    -- parse command line args and validate
	local host, port, task_file, loglevel = validate_args(arg)
	if(host == nil or port == nil or task_file == nil or loglevel == nil) then return end

	set_loglevel(logger, loglevel)

	-- read task file content
	task_file_content = read_task_file(task_file)
	if(task_file_content == nil) then return end


    logger:debug("Binding to " .. host .. ":" .. port)
	server_socket = socket.bind(host, port)
	copas.addserver(server_socket,
			function(c) return client_handler(c, c:getpeername()) end
		)

	copas.loop(0.1)
end

---- start main
main()

