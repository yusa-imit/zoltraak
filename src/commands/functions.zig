const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const RespValue = @import("../protocol/parser.zig").RespValue;
const functions_mod = @import("../storage/functions.zig");
const scripting = @import("../scripting/lua_engine.zig");
const RedisContext = @import("../scripting/redis_api.zig").RedisContext;
const ScriptStore = @import("../storage/scripting.zig").ScriptStore;
const Aof = @import("../storage/aof.zig").Aof;
const PubSub = @import("../storage/pubsub.zig").PubSub;
const TxState = @import("../commands/transactions.zig").TxState;
const ReplicationState = @import("../storage/replication.zig").ReplicationState;
const ClientRegistry = @import("../commands/client.zig").ClientRegistry;
const RespProtocol = @import("../commands/client.zig").RespProtocol;

/// FUNCTION LOAD [REPLACE] <code>
/// Register a Lua function library
pub fn cmdFunctionLoad(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len < 3) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function load' command") };
    }

    var replace = false;
    var code_arg_idx: usize = 2;

    // Check for REPLACE flag
    if (args.len >= 4 and std.ascii.eqlIgnoreCase(args[2], "REPLACE")) {
        replace = true;
        code_arg_idx = 3;
    }

    const code = args[code_arg_idx];

    // Parse Shebang to extract library name
    const shebang = functions_mod.parseShebang(code) catch {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR library code must start with a Shebang statement") };
    };

    // Validate engine
    if (!std.mem.eql(u8, shebang.engine, "lua")) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR unsupported engine (only 'lua' is supported)") };
    }

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if library exists
    if (!replace and storage.functions.getLibrary(shebang.library_name) != null) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR library already exists (use REPLACE to overwrite)") };
    }

    // Create library
    var library = try functions_mod.Library.init(allocator, shebang.library_name, "lua", code);
    errdefer library.deinit();

    // Execute Lua code to register functions via redis.register_function()
    var reg_context = functions_mod.FunctionRegistrationContext.init(allocator, shebang.library_name);
    defer reg_context.deinit();

    // Create Lua engine without RedisContext (FUNCTION LOAD doesn't need redis.call())
    var lua_engine = scripting.LuaEngine.init(allocator, null, 5000) catch |err| {
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to initialize Lua engine: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };
    defer lua_engine.deinit();

    // Load library code and collect registered functions
    lua_engine.loadLibrary(code, &reg_context) catch |err| {
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to load library: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };

    // Transfer registered functions to library
    reg_context.transferToLibrary(&library) catch |err| {
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to register functions: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };

    // Add or replace library
    if (replace) {
        storage.functions.replaceLibrary(library) catch |err| {
            if (err == error.LibraryNotFound) {
                // Library doesn't exist, just add it
                try storage.functions.addLibrary(library);
            } else {
                return err;
            }
        };
    } else {
        storage.functions.addLibrary(library) catch |err| {
            if (err == error.LibraryExists) {
                return RespValue{ .error_string = try allocator.dupe(u8, "ERR library already exists (use REPLACE to overwrite)") };
            } else if (err == error.FunctionExists) {
                return RespValue{ .error_string = try allocator.dupe(u8, "ERR function name already exists in another library") };
            }
            return err;
        };
    }

    // Return library name
    return RespValue{ .bulk_string = try allocator.dupe(u8, shebang.library_name) };
}

/// FCALL <function> <numkeys> [key...] [arg...]
/// Call a registered function
pub fn cmdFcall(
    allocator: std.mem.Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: [][]const u8,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
    // args are stripped of "FCALL": args[0]=function, args[1]=numkeys, args[2..]=keys+argv
    if (args.len < 2) {
        return try allocator.dupe(u8, "-ERR wrong number of arguments for 'fcall' command\r\n");
    }

    const function_name = args[0];
    const numkeys_str = args[1];

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return try allocator.dupe(u8, "-ERR value is not an integer or out of range\r\n");
    };

    if (args.len < 2 + numkeys) {
        return try allocator.dupe(u8, "-ERR Number of keys can't be greater than number of args\r\n");
    }

    // Extract keys and argv
    const keys = if (numkeys > 0) args[2 .. 2 + numkeys] else &[_][]const u8{};
    const argv = if (args.len > 2 + numkeys) args[2 + numkeys ..] else &[_][]const u8{};

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Lookup function
    const func_info = storage.functions.getFunction(function_name) orelse {
        return try std.fmt.allocPrint(allocator, "-ERR Function not found\r\n", .{});
    };

    // Get library code
    const library = storage.functions.getLibrary(func_info.library_name) orelse {
        return try allocator.dupe(u8, "-ERR Library not found (internal error)\r\n");
    };

    // Start execution tracking
    try storage.functions.startExecution(function_name, args);
    defer storage.functions.stopExecution();

    // Create RedisContext for redis.call/pcall
    const redis_ctx = try allocator.create(RedisContext);
    redis_ctx.* = RedisContext{
        .allocator = allocator,
        .storage = storage,
        .aof = aof,
        .ps = ps,
        .subscriber_id = subscriber_id,
        .tx = tx,
        .repl = repl,
        .my_port = my_port,
        .replica_stream = replica_stream,
        .replica_idx = replica_idx,
        .client_registry = client_registry,
        .client_id = client_id,
        .script_store = script_store,
        .shutdown_state = shutdown_state,
        .databases = databases,
        .num_databases = num_databases,
    };

    // Create Lua engine with RedisContext
    var lua_engine = scripting.LuaEngine.init(allocator, redis_ctx, 5000) catch |err| {
        allocator.destroy(redis_ctx);
        const err_msg = try std.fmt.allocPrint(allocator, "-ERR Failed to initialize Lua engine: {}\r\n", .{err});
        return err_msg;
    };
    defer allocator.destroy(redis_ctx); // LuaEngine does not own redis_ctx
    defer lua_engine.deinit();

    // Call the function — returns raw RESP bytes
    const result_bytes = lua_engine.callFunction(library.code, function_name, numkeys, keys, argv) catch |err| {
        return try std.fmt.allocPrint(allocator, "-ERR Failed to call function: {}\r\n", .{err});
    };

    return result_bytes;
}

/// FCALL_RO <function> <numkeys> [key...] [arg...]
/// Call a registered function in read-only mode
pub fn cmdFcallRo(
    allocator: std.mem.Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: [][]const u8,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
    // args are stripped of "FCALL_RO": args[0]=function, args[1]=numkeys, args[2..]=keys+argv
    if (args.len < 2) {
        return try allocator.dupe(u8, "-ERR wrong number of arguments for 'fcall_ro' command\r\n");
    }

    const function_name = args[0];
    const numkeys_str = args[1];

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return try allocator.dupe(u8, "-ERR value is not an integer or out of range\r\n");
    };

    if (args.len < 2 + numkeys) {
        return try allocator.dupe(u8, "-ERR Number of keys can't be greater than number of args\r\n");
    }

    // Extract keys and argv
    const keys = if (numkeys > 0) args[2 .. 2 + numkeys] else &[_][]const u8{};
    const argv = if (args.len > 2 + numkeys) args[2 + numkeys ..] else &[_][]const u8{};

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Lookup function
    const func_info = storage.functions.getFunction(function_name) orelse {
        return try allocator.dupe(u8, "-ERR Function not found\r\n");
    };

    // Get library code
    const library = storage.functions.getLibrary(func_info.library_name) orelse {
        return try allocator.dupe(u8, "-ERR Library not found (internal error)\r\n");
    };

    // Start execution tracking
    try storage.functions.startExecution(function_name, args);
    defer storage.functions.stopExecution();

    // Create RedisContext for redis.call/pcall with read_only=true
    const redis_ctx = try allocator.create(RedisContext);
    redis_ctx.* = RedisContext{
        .allocator = allocator,
        .storage = storage,
        .aof = aof,
        .ps = ps,
        .subscriber_id = subscriber_id,
        .tx = tx,
        .repl = repl,
        .my_port = my_port,
        .replica_stream = replica_stream,
        .replica_idx = replica_idx,
        .client_registry = client_registry,
        .client_id = client_id,
        .script_store = script_store,
        .shutdown_state = shutdown_state,
        .databases = databases,
        .num_databases = num_databases,
        .read_only = true, // FCALL_RO enforces read-only semantics
    };

    // Create Lua engine with RedisContext
    var lua_engine = scripting.LuaEngine.init(allocator, redis_ctx, 5000) catch |err| {
        allocator.destroy(redis_ctx);
        return try std.fmt.allocPrint(allocator, "-ERR Failed to initialize Lua engine: {}\r\n", .{err});
    };
    defer allocator.destroy(redis_ctx); // LuaEngine does not own redis_ctx
    defer lua_engine.deinit();

    // Call the function — returns raw RESP bytes
    const result_bytes = lua_engine.callFunction(library.code, function_name, numkeys, keys, argv) catch |err| {
        return try std.fmt.allocPrint(allocator, "-ERR Failed to call function: {}\r\n", .{err});
    };

    return result_bytes;
}

/// FUNCTION FLUSH [ASYNC|SYNC]
/// Delete all function libraries
pub fn cmdFunctionFlush(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function flush' command") };
    }

    // Note: ASYNC/SYNC modes are accepted but ignored (we have no background threads)
    if (args.len == 3) {
        const mode = args[2];
        if (!std.ascii.eqlIgnoreCase(mode, "ASYNC") and !std.ascii.eqlIgnoreCase(mode, "SYNC")) {
            return RespValue{ .error_string = try allocator.dupe(u8, "ERR invalid FUNCTION FLUSH mode (expected ASYNC or SYNC)") };
        }
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.functions.flush();

    return RespValue{ .simple_string = try allocator.dupe(u8, "OK") };
}

/// FUNCTION DELETE <library_name>
/// Delete a function library
pub fn cmdFunctionDelete(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len != 3) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function delete' command") };
    }

    const library_name = args[2];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.functions.removeLibrary(library_name) catch |err| {
        if (err == error.LibraryNotFound) {
            return RespValue{ .error_string = try allocator.dupe(u8, "ERR Library not found") };
        }
        return err;
    };

    return RespValue{ .simple_string = try allocator.dupe(u8, "OK") };
}

/// Recursively deinit a RespValue (for nested arrays/maps)
fn deinitRespValue(value: *const RespValue, allocator: std.mem.Allocator) void {
    switch (value.*) {
        .array => |arr| {
            for (arr) |*item| {
                deinitRespValue(item, allocator);
            }
            // Cast away const for free
            allocator.free(@constCast(arr));
        },
        .bulk_string => |s| allocator.free(@constCast(s)),
        .simple_string => |s| allocator.free(@constCast(s)),
        .error_string => |s| allocator.free(@constCast(s)),
        else => {},
    }
}

/// Write a bulk string to a buffer (helper for RESP building)
fn appendBulkString(buf: *std.ArrayList(u8), alloc: std.mem.Allocator, s: []const u8) !void {
    try std.fmt.format(buf.writer(alloc), "${d}\r\n", .{s.len});
    try buf.appendSlice(alloc, s);
    try buf.appendSlice(alloc, "\r\n");
}

/// FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]
/// List all function libraries.
/// RESP3: each library entry is a map; each function sub-entry is a map; flags is a set.
/// RESP2: each library entry is a flat array of key-value pairs (unchanged).
pub fn cmdFunctionList(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
    protocol_version: RespProtocol,
) ![]const u8 {
    var library_pattern: ?[]const u8 = null;
    var with_code = false;

    var i: usize = 2;
    while (i < args.len) {
        if (std.ascii.eqlIgnoreCase(args[i], "LIBRARYNAME")) {
            if (i + 1 >= args.len) {
                var w = @import("../protocol/writer.zig").Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR LIBRARYNAME requires a pattern argument");
            }
            library_pattern = args[i + 1];
            i += 2;
        } else if (std.ascii.eqlIgnoreCase(args[i], "WITHCODE")) {
            with_code = true;
            i += 1;
        } else {
            var w = @import("../protocol/writer.zig").Writer.init(allocator);
            defer w.deinit();
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "ERR unknown FUNCTION LIST option: {s}", .{args[i]}) catch "ERR unknown option";
            return w.writeError(msg);
        }
    }

    const resp3 = protocol_version == .RESP3;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // First pass: count matching libraries
    var lib_count: usize = 0;
    {
        var it = storage.functions.libraries.iterator();
        while (it.next()) |entry| {
            if (library_pattern) |pat| {
                if (!std.mem.eql(u8, pat, entry.value_ptr.name) and !std.mem.eql(u8, pat, "*")) continue;
            }
            lib_count += 1;
        }
    }

    var buf = std.ArrayList(u8){ .items = &[_]u8{}, .capacity = 0 };
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    // Outer array: one entry per library
    try w.print("*{d}\r\n", .{lib_count});

    var lib_iter = storage.functions.libraries.iterator();
    while (lib_iter.next()) |lib_entry| {
        const lib = lib_entry.value_ptr;

        if (library_pattern) |pat| {
            if (!std.mem.eql(u8, pat, lib.name) and !std.mem.eql(u8, pat, "*")) continue;
        }

        // Library entry header: map or flat array
        if (resp3) {
            // %3 or %4 (with WITHCODE)
            const nfields: usize = if (with_code) 4 else 3;
            try w.print("%{d}\r\n", .{nfields});
        } else {
            // *8 or *10 (flat array of key-value pairs)
            const nitems: usize = if (with_code) 10 else 8;
            try w.print("*{d}\r\n", .{nitems});
        }

        // library_name
        try appendBulkString(&buf, allocator, "library_name");
        try appendBulkString(&buf, allocator, lib.name);

        // engine
        try appendBulkString(&buf, allocator, "engine");
        try appendBulkString(&buf, allocator, lib.engine);

        // functions
        try appendBulkString(&buf, allocator, "functions");
        const func_count = lib.functions.count();
        try w.print("*{d}\r\n", .{func_count});

        var func_iter = lib.functions.iterator();
        while (func_iter.next()) |func_entry| {
            const func = func_entry.value_ptr;

            if (resp3) {
                // %3 map per function: name, description, flags
                try w.print("%3\r\n", .{});
            } else {
                // *8 flat array: name, <val>, description, <val>, flags, <val>
                try w.print("*8\r\n", .{});
            }

            try appendBulkString(&buf, allocator, "name");
            try appendBulkString(&buf, allocator, func.name);

            try appendBulkString(&buf, allocator, "description");
            try appendBulkString(&buf, allocator, func.description);

            try appendBulkString(&buf, allocator, "flags");
            // flags: RESP3 set (~0), RESP2 array (*0)
            if (resp3) {
                try buf.appendSlice(allocator, "~0\r\n");
            } else {
                try buf.appendSlice(allocator, "*0\r\n");
            }
        }

        // library_code (WITHCODE only)
        if (with_code) {
            try appendBulkString(&buf, allocator, "library_code");
            try appendBulkString(&buf, allocator, lib.code);
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// FUNCTION DUMP
/// Return the serialized payload of all loaded libraries
pub fn cmdFunctionDump(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function dump' command") };
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const payload = try storage.functions.dump(allocator);

    return RespValue{ .bulk_string = payload };
}

/// FUNCTION RESTORE <payload> [FLUSH | APPEND | REPLACE]
/// Restore libraries from a binary payload
pub fn cmdFunctionRestore(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len < 3) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function restore' command") };
    }

    const payload = args[2];

    // Parse mode (default: APPEND)
    var mode = functions_mod.FunctionStore.RestoreMode.Append;
    if (args.len >= 4) {
        const mode_str = args[3];
        if (std.ascii.eqlIgnoreCase(mode_str, "FLUSH")) {
            mode = .Flush;
        } else if (std.ascii.eqlIgnoreCase(mode_str, "APPEND")) {
            mode = .Append;
        } else if (std.ascii.eqlIgnoreCase(mode_str, "REPLACE")) {
            mode = .Replace;
        } else {
            return RespValue{ .error_string = try allocator.dupe(u8, "ERR invalid FUNCTION RESTORE mode (expected FLUSH, APPEND, or REPLACE)") };
        }
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.functions.restore(payload, mode) catch |err| {
        const err_msg = switch (err) {
            error.InvalidPayload => "ERR invalid payload format",
            error.LibraryExists => "ERR library already exists (use REPLACE mode to overwrite)",
            error.FunctionExists => "ERR function name already exists in another library",
            else => "ERR failed to restore functions",
        };
        return RespValue{ .error_string = try allocator.dupe(u8, err_msg) };
    };

    return RespValue{ .simple_string = try allocator.dupe(u8, "OK") };
}

/// FUNCTION STATS
/// Return execution statistics for functions.
/// RESP3: returns a map with running_script (false or map) and engines (map of maps).
/// RESP2: returns a flat array of key-value pairs (unchanged).
pub fn cmdFunctionStats(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
    protocol_version: RespProtocol,
) ![]const u8 {
    var w = @import("../protocol/writer.zig").Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'function stats' command");
    }

    const resp3 = protocol_version == .RESP3;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    var buf = std.ArrayList(u8){ .items = &[_]u8{}, .capacity = 0 };
    errdefer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // Outer: RESP3 map (%2), RESP2 flat array (*4)
    if (resp3) {
        try writer.writeAll("%2\r\n");
    } else {
        try writer.writeAll("*4\r\n");
    }

    // running_script key
    try appendBulkString(&buf, allocator, "running_script");

    // running_script value
    if (storage.functions.getExecutionStats()) |stats| {
        // Running: RESP3 %3 map, RESP2 *6 flat array
        if (resp3) {
            try writer.writeAll("%3\r\n");
        } else {
            try writer.writeAll("*6\r\n");
        }
        try appendBulkString(&buf, allocator, "name");
        try appendBulkString(&buf, allocator, stats.function_name);

        try appendBulkString(&buf, allocator, "command");
        try writer.print("*{d}\r\n", .{stats.command_args.len});
        for (stats.command_args) |arg| {
            try appendBulkString(&buf, allocator, arg);
        }

        try appendBulkString(&buf, allocator, "duration_ms");
        try writer.print(":{d}\r\n", .{stats.duration_ms});
    } else {
        // Not running: RESP3 boolean false, RESP2 null bulk string
        if (resp3) {
            try writer.writeAll("#f\r\n");
        } else {
            try writer.writeAll("$-1\r\n");
        }
    }

    // engines key
    try appendBulkString(&buf, allocator, "engines");

    // engines value: RESP3 %1 map (LUA -> %2 map), RESP2 *2 flat array
    const lib_count = storage.functions.libraries.count();
    const func_count = storage.functions.getTotalFunctionCount();

    if (resp3) {
        try writer.writeAll("%1\r\n");
        try appendBulkString(&buf, allocator, "LUA");
        try writer.writeAll("%2\r\n");
        try appendBulkString(&buf, allocator, "libraries_count");
        try writer.print(":{d}\r\n", .{lib_count});
        try appendBulkString(&buf, allocator, "functions_count");
        try writer.print(":{d}\r\n", .{func_count});
    } else {
        try writer.writeAll("*2\r\n");
        try appendBulkString(&buf, allocator, "LUA");
        try writer.writeAll("*4\r\n");
        try appendBulkString(&buf, allocator, "libraries_count");
        try writer.print(":{d}\r\n", .{lib_count});
        try appendBulkString(&buf, allocator, "functions_count");
        try writer.print(":{d}\r\n", .{func_count});
    }

    return buf.toOwnedSlice(allocator);
}

/// FUNCTION KILL
/// Terminate a long-running function
pub fn cmdFunctionKill(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function kill' command") };
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.functions.requestKill() catch |err| {
        const err_msg = switch (err) {
            error.NotBusy => "NOTBUSY No scripts in execution right now.",
            error.Unkillable => "UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.",
        };
        return RespValue{ .error_string = try allocator.dupe(u8, err_msg) };
    };

    return RespValue{ .simple_string = try allocator.dupe(u8, "OK") };
}

/// FUNCTION HELP
/// Return help text for FUNCTION subcommands
pub fn cmdFunctionHelp(
    allocator: std.mem.Allocator,
    args: [][]const u8,
) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function help' command") };
    }

    const help_lines = [_][]const u8{
        "FUNCTION DELETE <library-name> -- Delete a library and all its functions.",
        "FUNCTION DUMP -- Return the serialized payload of loaded libraries.",
        "FUNCTION FLUSH [ASYNC|SYNC] -- Delete all libraries and functions.",
        "FUNCTION HELP -- Return helpful text about FUNCTION subcommands.",
        "FUNCTION KILL -- Kill the function currently in execution.",
        "FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE] -- Return information about all loaded libraries.",
        "FUNCTION LOAD [REPLACE] <engine-name> <library-name> <code> -- Create a new library.",
        "FUNCTION RESTORE <serialized-value> [FLUSH|APPEND|REPLACE] -- Restore libraries from a payload.",
        "FUNCTION STATS -- Return information about the function currently running and available engines.",
    };

    var array = try allocator.alloc(RespValue, help_lines.len);
    for (help_lines, 0..) |line, i| {
        array[i] = RespValue{ .bulk_string = try allocator.dupe(u8, line) };
    }

    return RespValue{ .array = array };
}
