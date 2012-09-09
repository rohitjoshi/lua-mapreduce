-----------------------------------------------------------------------------
--- table value to string
--- @param table value
--- @return string with table value
-----------------------------------------------------------------------------
function table.val_to_str ( v )

  if "string" == type( v ) then
    v = string.gsub( v, "\n", "\\n" )
    if string.match( string.gsub(v,"[^'\"]",""), '^"+$' ) then
      return "'" .. v .. "'"
    end
    return '"' .. string.gsub(v,'"', '\\"' ) .. '"'
  elseif "number" == type( v ) then
	  return v
  else
    return "table" == type( v ) and table.tostring( v ) or
      tostring( v )
  end
end

-----------------------------------------------------------------------------
--- table key to string
--- @param table key
--- @return return table key format [ key ]
-----------------------------------------------------------------------------
function table.key_to_str ( k )
  if "string" == type( k ) and string.match( k, "^[_%a][_%a%d]*$" ) then
    return k
  else
    return "[" .. table.val_to_str( k ) .. "]"
  end
end

-----------------------------------------------------------------------------
--- table to string
--- @param table
--- @return string
-----------------------------------------------------------------------------
function table.tostring( tbl )
  local result, done = {}, {}
  for k, v in ipairs( tbl ) do
    table.insert( result, table.val_to_str( v ) )
    done[ k ] = true
  end
  for k, v in pairs( tbl ) do
    if not done[ k ] then
      table.insert( result,
        table.key_to_str( k ) .. "=" .. table.val_to_str( v ) )
    end
  end
  return "{" .. table.concat( result, "," ) .. "}"
end

-----------------------------------------------------------------------------
--- Split the string based on pattern
--- @param string
--- @param pattern
--- @return table with all tokens
-----------------------------------------------------------------------------
function string.split (str, pattern)
  pattern = pattern or "[^%s]+"
  if pattern:len() == 0 then pattern = "[^%s]+" end
  local parts = {__index = table.insert}
  setmetatable(parts, parts)
  str:gsub(pattern, parts)
  setmetatable(parts, nil)
  parts.__index = nil
  return parts
end

-----------------------------------------------------------------------------
--- Check if string starts with given string
--- @param string to check
--- @param character set
--- @return true if string starts with the token
-----------------------------------------------------------------------------
function string.starts(String,Start)
   return string.sub(String,1,string.len(Start))==Start
end

-----------------------------------------------------------------------------
--- Check if string ends with given string
--- @param string to check
--- @param character set
--- @return true if string ends with the token
-----------------------------------------------------------------------------
function string.ends(String,End)
   return End=='' or string.sub(String,-string.len(End))==End
end

-----------------------------------------------------------------------------
--- String to the table : It calls loadstring and passes the string
-- @param table data
-- @return table
------------------------------------------------------------------------------
function string.table( data)
	loadstring("local tmptable = {" .. data .. "}")
	return tmptable;
end

------------------------------------------------------------------------------
--- Get command line optipo
-- @param args
-- @param options
------------------------------------------------------------------------------
function getopt( arg, options )
  local tab = {}
  for k, v in ipairs(arg) do
    if string.sub( v, 1, 2) == "--" then
      local x = string.find( v, "=", 1, true )
      if x then tab[ string.sub( v, 3, x-1 ) ] = string.sub( v, x+1 )
      else      tab[ string.sub( v, 3 ) ] = true
      end
    elseif string.sub( v, 1, 1 ) == "-" then
      local y = 2
      local l = string.len(v)
      local jopt
      while ( y <= l ) do
        jopt = string.sub( v, y, y )
        if string.find( options, jopt, 1, true ) then
          if y < l then
            tab[ jopt ] = string.sub( v, y+1 )
            y = l
          else
            tab[ jopt ] = arg[ k + 1 ]
          end
        else
          tab[ jopt ] = true
        end
        y = y + 1
      end
    end
  end
  return tab
end

------------------------------------------------------------------------------
--- set the log level
-- @return host, port and task_file
------------------------------------------------------------------------------
function set_loglevel(logger, level)
  if("debug" == level) then logger:setLevel (logging.DEBUG)
  elseif("info" == level) then logger:setLevel (logging.INFO)
  elseif("warn" == level) then logger:setLevel (logging.WARN)
  elseif("error" == level) then logger:setLevel (logging.ERROR)
  else logger:setLevel (logging.ERROR)
  end

end
