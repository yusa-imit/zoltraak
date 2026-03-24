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

    // Check if result is an error
    if (parsed == .error_string or parsed == .bulk_error) {
        const err_msg = if (parsed == .error_string) parsed.error_string else parsed.bulk_error;

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
    }

    // Push result to Lua stack
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

/// Register redis.call() and redis.pcall() as Lua global functions
pub fn registerRedisApi(L: *lua.lua_State, ctx: *RedisContext) !void {
    // Store context in Lua registry
    lua.lua_pushstring(L, "REDIS_CONTEXT");
    lua.lua_pushlightuserdata(L, ctx);
    lua.lua_settable(L, lua.LUA_REGISTRYINDEX);

    // Create redis table
    lua.lua_createtable(L, 0, 2);

    // redis.call
    lua.lua_pushcfunction(L, redis_call_impl);
    lua.lua_setfield(L, -2, "call");

    // redis.pcall
    lua.lua_pushcfunction(L, redis_pcall_impl);
    lua.lua_setfield(L, -2, "pcall");

    // Set redis as global
    lua.lua_setfield(L, lua.LUA_GLOBALSINDEX, "redis");
}
