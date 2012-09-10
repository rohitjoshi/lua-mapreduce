-------------------------------------------------------------------------------
--
-- @script: lua-mapreduce-task-file.lua
--
-- @author:  Rohit Joshi
--
-- @copyright Joshi Ventures LLC � 2012
--
-- @license Apache License, Version 2.0
--
-- VERSION HISTORY:
-- 1.0 8/09/2012 - Initial release
--
-------------------------------------------------------------------------------
-- Purpose: It defines the task file to process the files in given directory
-- and count all the workds
-- NOTE: mapfn, reducefn, finalfn and get_taskfn must used coroutine.yield
-- as they are invoked using coroutines
-------------------------------------------------------------------------------
local lfs = require "lfs"
 --logger = logging.console()


------------------------------------------------------------------------------
--- Read file from the disk
-- @return content of the task file
------------------------------------------------------------------------------
local function read_file(file_name)
    local f = assert(io.open(file_name, "rb"))
    local content = f:read("*all")
    f:close()
	return content
end



------------------------------------------------------------------------------
--- Read source : It reads the source and creates a task
-- @return content of the task file
------------------------------------------------------------------------------
local function read_source()
	--local file_path = system.pathForFile( "*.lua", lfs.currentdir() )
	local file_path = lfs.currentdir()
	--logger:debug("Current directory path:" .. file_path)
	local source_table = {}
	for file in lfs.dir(file_path) do
	    if(string.find(file, ".lua") ~= nil) then
		--	logger:debug("File name:" .. file_path .. "/" .. file)
			local c = read_file(file_path .. "/" .. file)
		--	logger:debug("file:" .. file .. ", length:" .. #c)

			if( c ~= nil) then
				source_table[file]=c
			end
		end
	end

	return source_table

end
------------------------------------------------------------------------------
--- mapfunction: It splits content of the lines by \r\n (windows)
--  and than splits each lines into words
--  @param key:  file name which needs to be processed
--  @param value: conent of the file
-- @return it returns the word and count 1
------------------------------------------------------------------------------
 function mapreducefn()

   local mr = {}


--- Server functions  ------------------------------------------------------


    --- Get task: It returns the task based on source. It could be data from the disk or streaming
	mr.taskfn = function()
		--logger:debug("Getting map task")
		local tasks = read_source()  -- read source is utility function defined to read data source
		for key, value in pairs(tasks) do
			coroutine.yield(key, value)
		end
	end

	--- Final result function which will be called when map and reduce task are completed
	mr.finalfn = function (results)
		print("Final results of the task:")
		for key, value in pairs(results) do
			print( key .. ":" .. value)
			coroutine.yield()
		end
	end




---  Client functions

	-- Map function : Here it splits the content of the files into lines and each line into words
	mr.mapfn = function(key, value)
		--logger:debug("mapfn with key:" .. key .. ", value :" .. value .. "\r\n\r\n")
		local file_words = {}

		local lines = value:split("[^\r\n%s]+")
	--	logger:debug("Number of lines in " .. #lines .. " in the file " .. key)

		for k, w in  ipairs(lines) do

			if(w ~= nil) then

				local words = {}
				string.gsub(w, "(%a+)", function (word)
					table.insert(words, string.lower(word))
				end)


				--local words = w:split("[^ %s]+")

				if(words ~= nil) then
					--logger:debug("Number of words in line " .. k .. " are " .. #words)
					for j=1, #words do
						--logger:debug("mapfn:yielding " .. words[j])
						coroutine.yield(words[j], 1)

					end
				end

			end
		end
	end



	---Reduce function: It returns the numbe of entries for the values

	mr.reducefn = function (key, value)
		--logger:debug("reducefn: for key:" .. key ..  ", number of words  :" .. #value)
		coroutine.yield(key, #value)
	end



	return mr;

end




