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
) !RespValue {
    if (args.len < 4) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'fcall' command") };
    }

    const function_name = args[2];
    const numkeys_str = args[3];

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR value is not an integer or out of range") };
    };

    if (args.len < 4 + numkeys) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR Number of keys can't be greater than number of args") };
    }

    // Extract keys and argv
    const keys = if (numkeys > 0) args[4 .. 4 + numkeys] else &[_][]const u8{};
    const argv = if (args.len > 4 + numkeys) args[4 + numkeys ..] else &[_][]const u8{};

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Lookup function
    const func_info = storage.functions.getFunction(function_name) orelse {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR Function not found") };
    };

    // Get library code
    const library = storage.functions.getLibrary(func_info.library_name) orelse {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR Library not found (internal error)") };
    };

    // Start execution tracking
    try storage.functions.startExecution(function_name, args[1..]);
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
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to initialize Lua engine: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };
    defer lua_engine.deinit(); // deinit will destroy redis_ctx

    // Call the function
    const result_str = lua_engine.callFunction(library.code, function_name, numkeys, keys, argv) catch |err| {
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to call function: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };

    // Return result as bulk string
    return RespValue{ .bulk_string = result_str };
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
) !RespValue {
    if (args.len < 4) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'fcall_ro' command") };
    }

    const function_name = args[2];
    const numkeys_str = args[3];

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR value is not an integer or out of range") };
    };

    if (args.len < 4 + numkeys) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR Number of keys can't be greater than number of args") };
    }

    // Extract keys and argv
    const keys = if (numkeys > 0) args[4 .. 4 + numkeys] else &[_][]const u8{};
    const argv = if (args.len > 4 + numkeys) args[4 + numkeys ..] else &[_][]const u8{};

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Lookup function
    const func_info = storage.functions.getFunction(function_name) orelse {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR Function not found") };
    };

    // Get library code
    const library = storage.functions.getLibrary(func_info.library_name) orelse {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR Library not found (internal error)") };
    };

    // Start execution tracking
    try storage.functions.startExecution(function_name, args[1..]);
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
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to initialize Lua engine: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };
    defer lua_engine.deinit(); // deinit will destroy redis_ctx

    // Call the function
    const result_str = lua_engine.callFunction(library.code, function_name, numkeys, keys, argv) catch |err| {
        const err_msg = try std.fmt.allocPrint(allocator, "ERR Failed to call function: {}", .{err});
        return RespValue{ .error_string = err_msg };
    };

    // Return result as bulk string
    return RespValue{ .bulk_string = result_str };
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

/// FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]
/// List all function libraries
pub fn cmdFunctionList(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    var library_pattern: ?[]const u8 = null;
    var with_code = false;

    // Parse optional arguments
    var i: usize = 2;
    while (i < args.len) {
        if (std.ascii.eqlIgnoreCase(args[i], "LIBRARYNAME")) {
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = try allocator.dupe(u8, "ERR LIBRARYNAME requires a pattern argument") };
            }
            library_pattern = args[i + 1];
            i += 2;
        } else if (std.ascii.eqlIgnoreCase(args[i], "WITHCODE")) {
            with_code = true;
            i += 1;
        } else {
            const err_msg = try std.fmt.allocPrint(allocator, "ERR unknown FUNCTION LIST option: {s}", .{args[i]});
            return RespValue{ .error_string = err_msg };
        }
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Build response array (one entry per library)
    var libraries_array = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
    errdefer {
        for (libraries_array.items) |*item| {
            deinitRespValue(item, allocator);
        }
        libraries_array.deinit(allocator);
    }

    var lib_iter = storage.functions.libraries.iterator();
    while (lib_iter.next()) |lib_entry| {
        const lib = lib_entry.value_ptr;

        // Filter by library name pattern if specified
        if (library_pattern) |pattern| {
            // Simple glob matching (exact or wildcard)
            if (!std.mem.eql(u8, pattern, lib.name) and !std.mem.eql(u8, pattern, "*")) {
                continue;
            }
        }

        // Build library entry (flat array of key-value pairs)
        var lib_array = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
        errdefer {
            for (lib_array.items) |*item| {
                deinitRespValue(item, allocator);
            }
            lib_array.deinit(allocator);
        }

        // library_name field
        try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, "library_name") });
        try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, lib.name) });

        // engine field
        try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, "engine") });
        try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, lib.engine) });

        // functions field (nested array of function metadata)
        try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, "functions") });
        var funcs_array = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
        errdefer {
            for (funcs_array.items) |*item| {
                deinitRespValue(item, allocator);
            }
            funcs_array.deinit(allocator);
        }

        var func_iter = lib.functions.iterator();
        while (func_iter.next()) |func_entry| {
            const func = func_entry.value_ptr;

            // Build function metadata (flat array)
            var func_array = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
            errdefer {
                for (func_array.items) |*item| {
                    deinitRespValue(item, allocator);
                }
                func_array.deinit(allocator);
            }

            try func_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "name") });
            try func_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, func.name) });

            try func_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "description") });
            try func_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, func.description) });

            try func_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "flags") });
            var flags_array = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
            // No flags yet, return empty array
            try func_array.append(allocator, RespValue{ .array = try flags_array.toOwnedSlice(allocator) });

            try funcs_array.append(allocator, RespValue{ .array = try func_array.toOwnedSlice(allocator) });
        }
        try lib_array.append(allocator,RespValue{ .array = try funcs_array.toOwnedSlice(allocator) });

        // library_code field (optional, only if WITHCODE)
        if (with_code) {
            try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, "library_code") });
            try lib_array.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, lib.code) });
        }

        try libraries_array.append(allocator, RespValue{ .array = try lib_array.toOwnedSlice(allocator) });
    }

    return RespValue{ .array = try libraries_array.toOwnedSlice(allocator) };
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
/// Return execution statistics for functions
pub fn cmdFunctionStats(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = try allocator.dupe(u8, "ERR wrong number of arguments for 'function stats' command") };
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Build RESP map/array
    var top_level = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
    errdefer {
        for (top_level.items) |*item| {
            deinitRespValue(item, allocator);
        }
        top_level.deinit(allocator);
    }

    // Add "running_script" key
    try top_level.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, "running_script") });

    // Add running_script value (null or map)
    if (storage.functions.getExecutionStats()) |stats| {
        // Build running_script map
        var running_map = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
        errdefer {
            for (running_map.items) |*item| {
                deinitRespValue(item, allocator);
            }
            running_map.deinit(allocator);
        }

        // name field
        try running_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "name") });
        try running_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, stats.function_name) });

        // command field (array)
        try running_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "command") });
        var cmd_array = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
        for (stats.command_args) |arg| {
            try cmd_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, arg) });
        }
        try running_map.append(allocator, RespValue{ .array = try cmd_array.toOwnedSlice(allocator) });

        // duration_ms field
        try running_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "duration_ms") });
        try running_map.append(allocator, RespValue{ .integer = stats.duration_ms });

        try top_level.append(allocator,RespValue{ .array = try running_map.toOwnedSlice(allocator) });
    } else {
        // No function running
        try top_level.append(allocator,RespValue{ .null_bulk_string = {} });
    }

    // Add "engines" key
    try top_level.append(allocator,RespValue{ .bulk_string = try allocator.dupe(u8, "engines") });

    // Add engines value (map)
    var engines_map = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
    errdefer {
        for (engines_map.items) |*item| {
            deinitRespValue(item, allocator);
        }
        engines_map.deinit(allocator);
    }

    // LUA engine entry
    try engines_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "LUA") });
    var lua_stats = std.ArrayList(RespValue){ .items = &[_]RespValue{}, .capacity = 0 };
    errdefer {
        for (lua_stats.items) |*item| {
            deinitRespValue(item, allocator);
        }
        lua_stats.deinit(allocator);
    }

    try lua_stats.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "libraries_count") });
    try lua_stats.append(allocator, RespValue{ .integer = @as(i64, @intCast(storage.functions.libraries.count())) });

    try lua_stats.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "functions_count") });
    try lua_stats.append(allocator, RespValue{ .integer = @as(i64, @intCast(storage.functions.getTotalFunctionCount())) });

    try engines_map.append(allocator, RespValue{ .array = try lua_stats.toOwnedSlice(allocator) });
    try top_level.append(allocator,RespValue{ .array = try engines_map.toOwnedSlice(allocator) });

    return RespValue{ .array = try top_level.toOwnedSlice(allocator) };
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
