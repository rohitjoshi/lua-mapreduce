lua-mapreduce
=============

Map Reduce implemented in Lua

[lua-mapreduce](https://github.com/rohitjoshi/lua-mapreduce) is a fast and easy MapReduce implementation for lua inspired by other ma-reduce implementation and particularly octopy in python.

It doesn't aim to meet all your distributed computing needs, but its simple approach is amendable to a large proportion of parallelizable tasks. If your code has a for-loop, there's a good chance that you can make it distributed with just a few small changes. 

It uses following lua modules. 

1. [lausocket](http://w3.impa.br/~diego/software/luasocket/): tcp client-server connectivity
2. [copas](http://keplerproject.github.com/copas/): Coroutine Oriented Portable Asynchronous Services for Lua
3. [lualogging](www.keplerproject.org/lualogging)
4. [serialize](https://github.com/fab13n/metalua/blob/master/src/lib/serialize.lua)(included in this project)
5. [lanes](https://github.com/LuaLanes/lanes): multithreading library for Lua
6. [luafilesystem](http://keplerproject.github.com/luafilesystem/): Used only in the task-file example to list files from the directory. lua-mapreduce client/server doesn't depend on this module

For windows, you can install [luaforwindows](http://code.google.com/p/luaforwindows/) which includes these modules.

For Linux/Unix/MacOS and Windows: you can use [LuaDist](http://luadist.org/) 

Under a Debian GNU/Linux system you can install the dependencies with:
  apt-get install lua-logging lua-copas lua-socket lua-filesystem
  lanes is not yet packaged for Debian, you can apt-get install luarocks and then do
  luarocks install lanes (as root or with sudo)

### Directory structure:
1. [lua-mapreduce-server.lua](https://github.com/rohitjoshi/lua-mapreduce/blob/master/lua-mapreduce-server.lua) : It is a map-reduce server which receives the connections from clients, sends them task-file and than sends them tasks to perform map/reduce functionality.

2. [lua-mapreduce-client.lua](https://github.com/rohitjoshi/lua-mapreduce/blob/master/lua-mapreduce-client.lua) : It connects to the server, receives the task and executes map/reduce functions defines in the task-file

3. [utils/utils.lua](https://github.com/rohitjoshi/lua-mapreduce/blob/master/utils/utils.lua) : Provides utility functionality

4. [utils/serialize.lua](https://github.com/rohitjoshi/lua-mapreduce/blob/master/utils/serialize.lua) : Provides table serialization functionality 

5. [example/word-count-taskfile.lua](https://github.com/rohitjoshi/lua-mapreduce/blob/master/example/word-count-taskfile.lua) : Example task-file for counting words from all .txt files in a given directory (it uses the current directory if none is specified).
More details on how to create task file is given in [word-count example](https://github.com/rohitjoshi/lua-mapreduce/wiki/Example---word-count) page of wiki.

### Example:
1. Start Server:
lua-mapreduce-server.lua  -t task-file.lua [-s server-ip -p port  -l loglevel -a command-line argument to task]

2. Start Client:
lua-mapreduce-client.lua [-s server-ip -p port -l loglevel]

### Todo
1. Add support to handled failed task. currently if client disconnect, the task handled by the client is lost
2. Support for multiple client connections  based on number of cores available on the computer. Use copas for async
3. Ability to send multiple task-files to the server.
4. Add more example of task-files
5. Add support for filter after reduce is performed
6. Possibly integrate with apache-mesos
