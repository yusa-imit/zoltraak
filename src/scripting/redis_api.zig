// Redis API for Lua scripts: redis.call() and redis.pcall()
// Provides bridge between Lua scripts and Redis command execution

const std = @import("std");
const lua = @import("lua_ffi.zig");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");

const RespValue = protocol.RespValue;
const RespType = protocol.RespType;

// Import required types for executeCommand
const Storage = @import("../storage/memory.zig").Storage;
const Aof = @import("../storage/aof.zig").Aof;
const PubSub = @import("../storage/pubsub.zig").PubSub;
const TxState = @import("../commands/transactions.zig").TxState;
const ReplicationState = @import("../storage/replication.zig").ReplicationState;
const ClientRegistry = @import("../commands/client.zig").ClientRegistry;
const ScriptStore = @import("../storage/scripting.zig").ScriptStore;

/// Context passed to redis.call() and redis.pcall() C callbacks
/// Stored in Lua registry for access from C functions
/// Contains all parameters needed to call executeCommand
pub const RedisContext = struct {
    allocator: std.mem.Allocator,
    storage: *Storage,
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
    script_store: *ScriptStore,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
    read_only: bool = false, // If true, only allow read-only commands (for FCALL_RO/EVAL_RO)
};

/// C callback for redis.call()
/// Executes Redis command and propagates errors as Lua errors
export fn redis_call_impl(L: *lua.lua_State) callconv(.c) c_int {
    return redis_call_or_pcall(L, true) catch |err| {
        const err_msg = switch (err) {
            error.OutOfMemory => "Out of memory",
            else => "Internal error in redis.call",
        };
        lua.lua_pushstring(L, err_msg);
        _ = lua.lua_error(L);
        return 0; // unreachable, but needed for type
    };
}

/// C callback for redis.pcall()
/// Executes Redis command and catches errors as Lua tables {err = "..."}
export fn redis_pcall_impl(L: *lua.lua_State) callconv(.c) c_int {
    return redis_call_or_pcall(L, false) catch |err| {
        const err_msg = switch (err) {
            error.OutOfMemory => "Out of memory",
            else => "Internal error in redis.pcall",
        };
        lua.lua_pushstring(L, err_msg);
        _ = lua.lua_error(L);
        return 0; // unreachable
    };
}

/// Shared implementation for redis.call() and redis.pcall()
/// If propagate_errors=true, throws Lua error on Redis error (call behavior)
/// If propagate_errors=false, returns {err = "..."} table (pcall behavior)
fn redis_call_or_pcall(L: *lua.lua_State, propagate_errors: bool) !c_int {
    // Get RedisContext from Lua registry
    lua.lua_pushstring(L, "REDIS_CONTEXT");
    lua.lua_gettable(L, lua.LUA_REGISTRYINDEX);

    if (lua.lua_type(L, -1) != lua.LUA_TLIGHTUSERDATA) {
        lua.lua_pushstring(L, "ERR redis.call/pcall context not initialized");
        _ = lua.lua_error(L);
        return 0;
    }

    const ctx_ptr = lua.lua_touserdata(L, -1);
    const ctx: *RedisContext = @ptrCast(@alignCast(ctx_ptr));
    lua.lua_pop(L, 1); // pop context

    // Get number of arguments
    const nargs = lua.lua_gettop(L);
    if (nargs == 0) {
        if (propagate_errors) {
            lua.lua_pushstring(L, "ERR redis.call requires at least one argument");
            _ = lua.lua_error(L);
        } else {
            // pcall: return {err = "..."}
            lua.lua_createtable(L, 0, 1);
            lua.lua_pushstring(L, "ERR redis.call requires at least one argument");
            lua.lua_setfield(L, -2, "err");
        }
        return 1;
    }

    // Check read-only mode BEFORE building args (no allocations yet, so lua_error is safe).
    // Peek at Lua stack position 1 (the command name string) without allocating.
    if (ctx.read_only and nargs > 0) {
        const arg1_type = lua.lua_type(L, 1);
        if (arg1_type == lua.LUA_TSTRING) {
            var len: usize = 0;
            const str_ptr = lua.lua_tolstring(L, 1, &len);
            if (str_ptr) |ptr| {
                const cmd_name = ptr[0..len];
                const classifyCommand = @import("../commands/strings.zig").classifyCommand;
                const cmd_type = classifyCommand(cmd_name);
                if (cmd_type == .write) {
                    // No heap allocations yet — safe to call lua_error (longjmp) or return table.
                    const err_msg = try std.fmt.allocPrint(ctx.allocator, "ERR Write commands are not allowed from scripts in read-only mode (command: {s})", .{cmd_name});
                    if (propagate_errors) {
                        const err_z = ctx.allocator.dupeZ(u8, err_msg) catch {
                            ctx.allocator.free(err_msg);
                            lua.lua_pushstring(L, "ERR out of memory");
                            _ = lua.lua_error(L);
                            return 0;
                        };
                        ctx.allocator.free(err_msg);
                        lua.lua_pushstring(L, err_z.ptr);
                        ctx.allocator.free(err_z);
                        _ = lua.lua_error(L);
                        return 0; // unreachable
                    } else {
                        defer ctx.allocator.free(err_msg);
                        lua.lua_createtable(L, 0, 1);
                        const err_z = try ctx.allocator.dupeZ(u8, err_msg);
                        defer ctx.allocator.free(err_z);
                        lua.lua_pushstring(L, err_z.ptr);
                        lua.lua_setfield(L, -2, "err");
                        return 1;
                    }
                }
            }
        }
    }

    // Build RespValue array from Lua arguments
    var args = std.ArrayList(RespValue){};
    defer {
        // Free owned strings in RespValue items
        for (args.items) |arg| {
            if (arg == .bulk_string) {
                ctx.allocator.free(arg.bulk_string);
            }
        }
        args.deinit(ctx.allocator);
    }

    var i: c_int = 1;
    while (i <= nargs) : (i += 1) {
        const arg_type = lua.lua_type(L, i);
        const resp_val = switch (arg_type) {
            lua.LUA_TSTRING => blk: {
                var len: usize = 0;
                const str_ptr = lua.lua_tolstring(L, i, &len);
                const str = if (str_ptr) |ptr| ptr[0..len] else "";
                const owned = try ctx.allocator.dupe(u8, str);
                break :blk RespValue{ .bulk_string = owned };
            },
            lua.LUA_TNUMBER => blk: {
                const num = lua.lua_tonumber(L, i);
                const str = try std.fmt.allocPrint(ctx.allocator, "{d}", .{num});
                break :blk RespValue{ .bulk_string = str };
            },
            else => {
                if (propagate_errors) {
                    lua.lua_pushstring(L, "ERR redis.call arguments must be strings or numbers");
                    _ = lua.lua_error(L);
                } else {
                    lua.lua_createtable(L, 0, 1);
                    lua.lua_pushstring(L, "ERR redis.call arguments must be strings or numbers");
                    lua.lua_setfield(L, -2, "err");
                }
                return 1;
            },
        };
        try args.append(ctx.allocator, resp_val);
    }

    // Execute Redis command using real executeCommand
    const cmd_array = try args.toOwnedSlice(ctx.allocator);
    defer {
        // Free the array and its owned strings
        for (cmd_array) |item| {
            if (item == .bulk_string) {
                ctx.allocator.free(item.bulk_string);
            }
        }
        ctx.allocator.free(cmd_array);
    }
    const cmd = RespValue{ .array = cmd_array };

    // Import executeCommand at compile time
    const executeCommand = @import("../commands/strings.zig").executeCommand;

    const result = executeCommand(
        ctx.allocator,
        ctx.storage,
        cmd,
        ctx.aof,
        ctx.ps,
        ctx.subscriber_id,
        ctx.tx,
        ctx.repl,
        ctx.my_port,
        ctx.replica_stream,
        ctx.replica_idx,
        ctx.client_registry,
        ctx.client_id,
        ctx.script_store,
        ctx.shutdown_state,
        ctx.databases,
        ctx.num_databases,
    ) catch |err| {
        const err_msg = try std.fmt.allocPrint(ctx.allocator, "ERR {s}", .{@errorName(err)});
        defer ctx.allocator.free(err_msg);

        if (propagate_errors) {
            const err_z = try ctx.allocator.dupeZ(u8, err_msg);
            defer ctx.allocator.free(err_z);
            lua.lua_pushstring(L, err_z.ptr);
            _ = lua.lua_error(L);
            return 0;
        } else {
            // pcall: return {err = "..."}
            lua.lua_createtable(L, 0, 1);
            const err_z = try ctx.allocator.dupeZ(u8, err_msg);
            defer ctx.allocator.free(err_z);
            lua.lua_pushstring(L, err_z.ptr);
            lua.lua_setfield(L, -2, "err");
            return 1;
        }
    };
    defer ctx.allocator.free(result);

    // Parse RESP response and convert to Lua value
    var parser = protocol.Parser.init(ctx.allocator);
    defer parser.deinit();
    const parsed = try parser.parse(result);
    // parsed must be freed via parser.freeValue(parsed) on ALL exit paths.
    // We cannot use defer here for the propagate_errors=true path because
    // lua_error() uses longjmp which bypasses Zig defer cleanup.

    // Check if result is an error
    if (parsed == .error_string or parsed == .bulk_error) {
        const err_msg = if (parsed == .error_string) parsed.error_string else parsed.bulk_error;

        if (propagate_errors) {
            // Dupe err_msg before freeing parsed (err_msg points into parsed).
            const err_z = ctx.allocator.dupeZ(u8, err_msg) catch {
                parser.freeValue(parsed);
                lua.lua_pushstring(L, "ERR out of memory");
                _ = lua.lua_error(L);
                return 0; // unreachable
            };
            // Free all allocations before lua_error (longjmp bypasses defer).
            parser.freeValue(parsed);
            lua.lua_pushstring(L, err_z.ptr);
            ctx.allocator.free(err_z);
            _ = lua.lua_error(L);
            return 0; // unreachable
        } else {
            // pcall: no longjmp, defer is safe.
            defer parser.freeValue(parsed);
            lua.lua_createtable(L, 0, 1);
            const err_z = try ctx.allocator.dupeZ(u8, err_msg);
            defer ctx.allocator.free(err_z);
            lua.lua_pushstring(L, err_z.ptr);
            lua.lua_setfield(L, -2, "err");
            return 1;
        }
    }

    // Normal result: push to Lua stack then free parsed.
    // pushRespValueToLua is a Zig function so defer runs on both normal and error return.
    defer parser.freeValue(parsed);
    try pushRespValueToLua(L, ctx.allocator, parsed);
    return 1;
}

/// Convert RespValue to Lua value and push to stack
fn pushRespValueToLua(L: *lua.lua_State, allocator: std.mem.Allocator, value: RespValue) !void {
    switch (value) {
        .simple_string => |s| {
            lua.lua_pushlstring(L, s.ptr, s.len);
        },
        .bulk_string => |s| {
            lua.lua_pushlstring(L, s.ptr, s.len);
        },
        .integer => |i| {
            lua.lua_pushnumber(L, @floatFromInt(i));
        },
        .null_bulk_string, .null_array, .resp3_null => {
            lua.lua_pushnil(L);
        },
        .array => |arr| {
            lua.lua_createtable(L, @intCast(arr.len), 0);
            for (arr, 0..) |item, idx| {
                try pushRespValueToLua(L, allocator, item);
                lua.lua_rawseti(L, -2, @intCast(idx + 1)); // Lua is 1-indexed
            }
        },
        .boolean => |b| {
            lua.lua_pushboolean(L, if (b) 1 else 0);
        },
        .double => |d| {
            lua.lua_pushnumber(L, d);
        },
        .error_string, .bulk_error => {
            // Errors should be handled before calling this function
            lua.lua_pushnil(L);
        },
        else => {
            // For unsupported types, push nil
            lua.lua_pushnil(L);
        },
    }
}

/// C callback for redis.status_reply(msg)
/// Returns {ok = msg} table → produces +msg\r\n status reply
export fn redis_status_reply_impl(L: *lua.lua_State) callconv(.c) c_int {
    if (lua.lua_gettop(L) < 1 or lua.lua_type(L, 1) != lua.LUA_TSTRING) {
        lua.lua_pushstring(L, "ERR redis.status_reply requires a string argument");
        _ = lua.lua_error(L);
        return 0;
    }
    var len: usize = 0;
    const str_ptr = lua.lua_tolstring(L, 1, &len);
    lua.lua_createtable(L, 0, 1);
    if (str_ptr) |ptr| {
        lua.lua_pushlstring(L, ptr, len);
    } else {
        lua.lua_pushstring(L, "");
    }
    lua.lua_setfield(L, -2, "ok");
    return 1;
}

/// C callback for redis.error_reply(msg)
/// Returns {err = msg} table → produces -msg\r\n error reply
export fn redis_error_reply_impl(L: *lua.lua_State) callconv(.c) c_int {
    if (lua.lua_gettop(L) < 1 or lua.lua_type(L, 1) != lua.LUA_TSTRING) {
        lua.lua_pushstring(L, "ERR redis.error_reply requires a string argument");
        _ = lua.lua_error(L);
        return 0;
    }
    var len: usize = 0;
    const str_ptr = lua.lua_tolstring(L, 1, &len);
    lua.lua_createtable(L, 0, 1);
    if (str_ptr) |ptr| {
        lua.lua_pushlstring(L, ptr, len);
    } else {
        lua.lua_pushstring(L, "");
    }
    lua.lua_setfield(L, -2, "err");
    return 1;
}

/// C callback for redis.log(level, msg)
/// Logs message to stderr at specified level. Constants: redis.LOG_DEBUG=0..WARNING=3
export fn redis_log_impl(L: *lua.lua_State) callconv(.c) c_int {
    const nargs = lua.lua_gettop(L);
    if (nargs < 2) {
        // silently ignore invalid calls
        return 0;
    }
    const level = lua.lua_tonumber(L, 1);
    const level_int: i32 = @intFromFloat(level);

    var msg_len: usize = 0;
    const msg_ptr = lua.lua_tolstring(L, 2, &msg_len);
    const msg = if (msg_ptr) |ptr| ptr[0..msg_len] else "(nil)";

    const level_str: []const u8 = switch (level_int) {
        0 => "DEBUG",
        1 => "VERBOSE",
        2 => "NOTICE",
        3 => "WARNING",
        else => "NOTICE",
    };

    // Write to stderr — best effort, ignore errors
    std.debug.print("[{s}] (Lua) {s}\n", .{ level_str, msg });
    return 0;
}

/// C callback for redis.replicate_commands()
/// Deprecated in Redis 7.0, kept for backward compatibility. Returns true.
export fn redis_replicate_commands_impl(L: *lua.lua_State) callconv(.c) c_int {
    lua.lua_pushboolean(L, 1); // return true
    return 1;
}

/// C callback for redis.setresp(version)
/// Sets RESP protocol version for script return values (2 or 3).
/// Currently a no-op stub — keeps RESP2 behavior.
export fn redis_setresp_impl(L: *lua.lua_State) callconv(.c) c_int {
    const nargs = lua.lua_gettop(L);
    if (nargs < 1 or lua.lua_type(L, 1) != lua.LUA_TNUMBER) {
        lua.lua_pushstring(L, "ERR redis.setresp requires a numeric argument (2 or 3)");
        _ = lua.lua_error(L);
        return 0;
    }
    const version = lua.lua_tonumber(L, 1);
    const ver_int: i32 = @intFromFloat(version);
    if (ver_int != 2 and ver_int != 3) {
        lua.lua_pushstring(L, "RESP version must be 2 or 3.");
        _ = lua.lua_error(L);
        return 0;
    }
    // No return value (nil)
    return 0;
}

/// C callback for redis.sha1hex(str)
/// Computes SHA1 hex digest of a string. Used by scripts for fingerprinting.
export fn redis_sha1hex_impl(L: *lua.lua_State) callconv(.c) c_int {
    if (lua.lua_gettop(L) < 1) {
        lua.lua_pushstring(L, "ERR redis.sha1hex requires a string argument");
        _ = lua.lua_error(L);
        return 0;
    }
    var len: usize = 0;
    const str_ptr = lua.lua_tolstring(L, 1, &len);
    const input = if (str_ptr) |ptr| ptr[0..len] else "";

    var hasher = std.crypto.hash.Sha1.init(.{});
    hasher.update(input);
    var digest: [20]u8 = undefined;
    hasher.final(&digest);

    // Format as 40-char lowercase hex string
    var hex_buf: [40]u8 = undefined;
    for (digest, 0..) |byte, i| {
        _ = std.fmt.bufPrint(hex_buf[i * 2 .. i * 2 + 2], "{x:0>2}", .{byte}) catch {};
    }

    lua.lua_pushlstring(L, &hex_buf, hex_buf.len);
    return 1;
}

/// Register redis.call() and redis.pcall() as Lua global functions
pub fn registerRedisApi(L: *lua.lua_State, ctx: *RedisContext) !void {
    // Store context in Lua registry
    lua.lua_pushstring(L, "REDIS_CONTEXT");
    lua.lua_pushlightuserdata(L, ctx);
    lua.lua_settable(L, lua.LUA_REGISTRYINDEX);

    // Create redis table
    lua.lua_createtable(L, 0, 10);

    // redis.call
    lua.lua_pushcfunction(L, redis_call_impl);
    lua.lua_setfield(L, -2, "call");

    // redis.pcall
    lua.lua_pushcfunction(L, redis_pcall_impl);
    lua.lua_setfield(L, -2, "pcall");

    // redis.status_reply
    lua.lua_pushcfunction(L, redis_status_reply_impl);
    lua.lua_setfield(L, -2, "status_reply");

    // redis.error_reply
    lua.lua_pushcfunction(L, redis_error_reply_impl);
    lua.lua_setfield(L, -2, "error_reply");

    // redis.log
    lua.lua_pushcfunction(L, redis_log_impl);
    lua.lua_setfield(L, -2, "log");

    // redis.replicate_commands (deprecated no-op)
    lua.lua_pushcfunction(L, redis_replicate_commands_impl);
    lua.lua_setfield(L, -2, "replicate_commands");

    // redis.setresp
    lua.lua_pushcfunction(L, redis_setresp_impl);
    lua.lua_setfield(L, -2, "setresp");

    // redis.sha1hex
    lua.lua_pushcfunction(L, redis_sha1hex_impl);
    lua.lua_setfield(L, -2, "sha1hex");

    // Log level constants
    lua.lua_pushnumber(L, 0);
    lua.lua_setfield(L, -2, "LOG_DEBUG");
    lua.lua_pushnumber(L, 1);
    lua.lua_setfield(L, -2, "LOG_VERBOSE");
    lua.lua_pushnumber(L, 2);
    lua.lua_setfield(L, -2, "LOG_NOTICE");
    lua.lua_pushnumber(L, 3);
    lua.lua_setfield(L, -2, "LOG_WARNING");

    // Set redis as global
    lua.lua_setfield(L, lua.LUA_GLOBALSINDEX, "redis");
}

/// Register redis.register_function() for Functions API
/// Must be called with a FunctionRegistrationContext stored in Lua registry
pub fn registerFunctionsApi(L: *lua.lua_State) void {
    // Use rawget to bypass sandbox __index (redis may not exist when redis_ctx is null)
    lua.lua_pushstring(L, "redis");
    lua.lua_rawget(L, lua.LUA_GLOBALSINDEX);
    if (lua.lua_isnil(L, -1)) {
        lua.lua_pop(L, 1);
        lua.lua_createtable(L, 0, 2);
        // Use rawset to bypass sandbox __newindex when registering the new table
        lua.lua_pushstring(L, "redis");
        lua.lua_pushvalue(L, -2); // dup table
        lua.lua_rawset(L, lua.LUA_GLOBALSINDEX);
    }
    // Stack: [... redis_table]

    // Add redis.register_function
    lua.lua_pushcfunction(L, redis_register_function_impl);
    lua.lua_setfield(L, -2, "register_function");

    lua.lua_pop(L, 1); // pop redis table
}

/// C callback for redis.register_function()
/// Expected Lua API: redis.register_function(function_table)
/// function_table = { function_name = "...", description = "...", callback = function() ... end, flags = {...} }
export fn redis_register_function_impl(L: *lua.lua_State) callconv(.c) c_int {
    return redis_register_function_internal(L) catch {
        lua.lua_pushstring(L, "ERR Internal error in redis.register_function");
        _ = lua.lua_error(L);
        return 0; // unreachable
    };
}

fn redis_register_function_internal(L: *lua.lua_State) !c_int {
    // Get FunctionRegistrationContext from Lua registry
    lua.lua_pushstring(L, "FUNCTION_REG_CONTEXT");
    lua.lua_gettable(L, lua.LUA_REGISTRYINDEX);

    if (lua.lua_type(L, -1) != lua.LUA_TLIGHTUSERDATA) {
        lua.lua_pushstring(L, "ERR redis.register_function can only be called during FUNCTION LOAD");
        _ = lua.lua_error(L);
        return 0;
    }

    const ctx_ptr = lua.lua_touserdata(L, -1);
    const FunctionRegistrationContext = @import("../storage/functions.zig").FunctionRegistrationContext;
    const ctx: *FunctionRegistrationContext = @ptrCast(@alignCast(ctx_ptr));
    lua.lua_pop(L, 1); // pop context

    const nargs = lua.lua_gettop(L);
    var function_name: []const u8 = "";
    var description: []const u8 = "";

    if (nargs == 2 and lua.lua_type(L, 1) == lua.LUA_TSTRING and lua.lua_type(L, 2) == lua.LUA_TFUNCTION) {
        // Two-arg form: redis.register_function('name', function(keys, args) ... end)
        var name_len: usize = 0;
        const name_ptr = lua.lua_tolstring(L, 1, &name_len);
        function_name = if (name_ptr != null and name_len > 0) name_ptr.?[0..name_len] else "";
    } else if (nargs == 1 and lua.lua_type(L, 1) == lua.LUA_TTABLE) {
        // Table form: redis.register_function({function_name=..., callback=..., description=...})
        lua.lua_getfield(L, 1, "function_name");
        if (lua.lua_isnil(L, -1)) {
            lua.lua_pushstring(L, "ERR function_name is required");
            _ = lua.lua_error(L);
            return 0;
        }
        var fn_len: usize = 0;
        const fn_ptr = lua.lua_tolstring(L, -1, &fn_len);
        function_name = if (fn_ptr != null and fn_len > 0) fn_ptr.?[0..fn_len] else "";
        lua.lua_pop(L, 1);

        lua.lua_getfield(L, 1, "description");
        var desc_len: usize = 0;
        description = if (!lua.lua_isnil(L, -1)) blk: {
            const desc_ptr = lua.lua_tolstring(L, -1, &desc_len);
            break :blk if (desc_ptr != null and desc_len > 0) desc_ptr.?[0..desc_len] else "";
        } else "";
        lua.lua_pop(L, 1);

        // Validate callback field exists
        lua.lua_getfield(L, 1, "callback");
        if (lua.lua_type(L, -1) != lua.LUA_TFUNCTION) {
            lua.lua_pushstring(L, "ERR callback must be a function");
            _ = lua.lua_error(L);
            return 0;
        }
        lua.lua_pop(L, 1);
    } else {
        lua.lua_pushstring(L, "ERR redis.register_function expects (name, callback) or ({function_name=..., callback=...})");
        _ = lua.lua_error(L);
        return 0;
    }

    const flags: u8 = 0;

    // Register function in context
    ctx.registerFunction(function_name, description, flags) catch |err| {
        if (err == error.FunctionExists) {
            lua.lua_pushstring(L, "ERR function already registered in this library");
            _ = lua.lua_error(L);
        } else {
            return err;
        }
        return 0;
    };

    return 0;
}

/// Register a FCALL-mode redis.register_function() that stores functions in the Lua registry.
/// Used in callFunction() so library code can execute without a FunctionRegistrationContext.
/// Functions are stored in the registry under "FCALL_<name>" to bypass the sandbox.
pub fn registerFcallModeApi(L: *lua.lua_State) void {
    // Access the redis table using rawget to bypass sandbox __index
    lua.lua_pushstring(L, "redis");
    lua.lua_rawget(L, lua.LUA_GLOBALSINDEX);
    if (lua.lua_isnil(L, -1)) {
        lua.lua_pop(L, 1);
        lua.lua_createtable(L, 0, 1);
        // Set redis table via rawset to bypass sandbox __newindex
        lua.lua_pushstring(L, "redis");
        lua.lua_pushvalue(L, -2); // dup the table
        lua.lua_rawset(L, lua.LUA_GLOBALSINDEX);
    }
    // Set redis.register_function on the redis table (modifying the table itself, not _G)
    lua.lua_pushcfunction(L, redis_register_function_fcall_impl);
    lua.lua_setfield(L, -2, "register_function");
    lua.lua_pop(L, 1); // pop redis table (no need to re-set as global, we modified it in-place)
}

/// FCALL-mode redis.register_function(name, callback) or redis.register_function({table})
/// Stores callback in Lua registry under "FCALL_<name>" to bypass sandbox restrictions.
export fn redis_register_function_fcall_impl(L: *lua.lua_State) callconv(.c) c_int {
    const nargs = lua.lua_gettop(L);
    var name_str: [*c]const u8 = undefined;
    var name_len: usize = 0;

    if (nargs == 2 and lua.lua_type(L, 1) == lua.LUA_TSTRING and lua.lua_type(L, 2) == lua.LUA_TFUNCTION) {
        // Two-arg form: redis.register_function('name', function(keys, args) ... end)
        name_str = lua.lua_tolstring(L, 1, &name_len);
        // callback is already at stack index 2 — move it to top and remember its index
        lua.lua_pushvalue(L, 2); // dup callback to top
    } else if (nargs == 1 and lua.lua_type(L, 1) == lua.LUA_TTABLE) {
        // Table form: redis.register_function({function_name=..., callback=...})
        lua.lua_pushstring(L, "function_name");
        lua.lua_rawget(L, 1);
        if (lua.lua_isnil(L, -1)) {
            lua.lua_pop(L, 1);
            lua.lua_pushstring(L, "ERR function_name is required");
            _ = lua.lua_error(L);
            return 0;
        }
        name_str = lua.lua_tolstring(L, -1, &name_len);
        lua.lua_pop(L, 1); // pop function_name string

        lua.lua_pushstring(L, "callback");
        lua.lua_rawget(L, 1);
        if (lua.lua_type(L, -1) != lua.LUA_TFUNCTION) {
            lua.lua_pop(L, 1);
            lua.lua_pushstring(L, "ERR callback must be a function");
            _ = lua.lua_error(L);
            return 0;
        }
        // callback is now at stack top
    } else {
        lua.lua_pushstring(L, "ERR redis.register_function expects (name, callback) or ({function_name=..., callback=...})");
        _ = lua.lua_error(L);
        return 0;
    }

    if (name_len == 0 or name_str == null) {
        lua.lua_pushstring(L, "ERR invalid function name");
        _ = lua.lua_error(L);
        return 0;
    }

    // Build registry key: "FCALL_<name>"
    var key_buf: [256]u8 = undefined;
    const prefix = "FCALL_";
    if (prefix.len + name_len >= key_buf.len) {
        lua.lua_pushstring(L, "ERR function name too long");
        _ = lua.lua_error(L);
        return 0;
    }
    @memcpy(key_buf[0..prefix.len], prefix);
    @memcpy(key_buf[prefix.len .. prefix.len + name_len], name_str[0..name_len]);
    key_buf[prefix.len + name_len] = 0; // null-terminate

    // Store callback (at stack top) in registry under "FCALL_<name>"
    lua.lua_setfield(L, lua.LUA_REGISTRYINDEX, @as([*:0]const u8, @ptrCast(&key_buf)));

    return 0;
}
