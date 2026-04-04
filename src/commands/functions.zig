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

/// FUNCTION FLUSH [ASYNC|SYNC]
/// Delete all function libraries
pub fn cmdFunctionFlush(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    // Optional ASYNC/SYNC argument (ignored for now)
    _ = args;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.functions.flush();

    return RespValue{ .simple_string = try allocator.dupe(u8, "OK") };
}

/// FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]
/// List all function libraries
pub fn cmdFunctionList(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    _ = args;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Simplified stub implementation — return empty array for now
    // Full implementation requires more complex nested array handling
    const empty_array = try allocator.alloc(RespValue, 0);
    return RespValue{ .array = empty_array };
}
