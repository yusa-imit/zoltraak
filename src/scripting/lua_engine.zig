// Lua scripting engine for Redis EVAL/EVALSHA commands
// Manages Lua state lifecycle, sandboxing, and script execution

const std = @import("std");
const lua = @import("lua_ffi.zig");
const redis_api = @import("redis_api.zig");
const lua_libraries = @import("lua_libraries.zig");
const Storage = @import("../storage/memory.zig").Storage;
const protocol = @import("../protocol/parser.zig");
const RespValue = protocol.RespValue;

pub const RedisContext = redis_api.RedisContext;

/// Lua engine instance - manages a single Lua VM
pub const LuaEngine = struct {
    L: *lua.lua_State,
    allocator: std.mem.Allocator,
    redis_ctx: ?*RedisContext,
    timeout_ms: i64, // script timeout in milliseconds (0 = no timeout)
    deadline_ns: i64, // absolute deadline in nanoseconds (set when script starts)

    /// Initialize a new Lua engine with sandboxing
    /// If redis_ctx is provided, registers redis.call() and redis.pcall()
    /// timeout_ms: script timeout in milliseconds (0 = no timeout, default 5000)
    pub fn init(allocator: std.mem.Allocator, redis_ctx: ?*RedisContext, timeout_ms: i64) !LuaEngine {
        const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;

        // Open standard libraries
        lua.luaL_openlibs(L);

        // Register Redis Lua libraries (cjson, cmsgpack, struct)
        // bit module is already built into LuaJIT
        try lua_libraries.registerLibraries(L);

        // Register redis.call() and redis.pcall() BEFORE sandbox — the sandbox sets
        // __newindex on _G that blocks new global creation, so 'redis' must exist first.
        if (redis_ctx) |ctx| {
            try redis_api.registerRedisApi(L, ctx);
        }

        // Apply sandboxing AFTER registering built-in globals (redis, KEYS, ARGV come later)
        applySandbox(L);

        return LuaEngine{
            .L = L,
            .allocator = allocator,
            .redis_ctx = redis_ctx,
            .timeout_ms = timeout_ms,
            .deadline_ns = 0,
        };
    }

    /// Apply Redis-compatible sandboxing to Lua environment
    /// Removes dangerous globals and restricts access to OS/IO operations
    fn applySandbox(L: *lua.lua_State) void {
        // Remove dangerous modules and functions that allow file system/OS access
        const dangerous_modules = [_][:0]const u8{
            "os",      // os.execute, os.exit, os.remove, etc.
            "io",      // io.open, io.write, etc.
            "loadfile", // Load and execute external files
            "dofile",   // Load and execute external files
        };

        for (dangerous_modules) |module_name| {
            lua.lua_pushnil(L);
            lua.lua_setfield(L, lua.LUA_GLOBALSINDEX, module_name.ptr);
        }

        // Restrict require() to only allow safe libraries
        // We'll replace it with a restricted version
        const require_code =
            \\local old_require = require
            \\local allowed_libs = {
            \\  ['math'] = true,
            \\  ['string'] = true,
            \\  ['table'] = true,
            \\  ['cjson'] = true,
            \\  ['cmsgpack'] = true,
            \\  ['struct'] = true,
            \\  ['bit'] = true,
            \\}
            \\require = function(name)
            \\  if not allowed_libs[name] then
            \\    error('ERR require is restricted in Redis scripts. Allowed: math, string, table, cjson, cmsgpack, struct, bit')
            \\  end
            \\  return old_require(name)
            \\end
        ;

        const load_result = lua.luaL_loadstring(L, require_code.ptr);
        if (load_result == lua.LUA_OK) {
            _ = lua.lua_pcall(L, 0, 0, 0);
        }

        // Disable ability to create new global variables (enforce local)
        const globals_code =
            \\local mt = {}
            \\mt.__newindex = function(t, k, v)
            \\  error('ERR Script attempted to create global variable "' .. tostring(k) .. '". Use local variables instead.')
            \\end
            \\mt.__index = function(t, k)
            \\  error('ERR Script attempted to access undefined global variable "' .. tostring(k) .. '"')
            \\end
            \\setmetatable(_G, mt)
        ;

        const globals_result = lua.luaL_loadstring(L, globals_code.ptr);
        if (globals_result == lua.LUA_OK) {
            _ = lua.lua_pcall(L, 0, 0, 0);
        }
    }

    /// Clean up Lua state
    pub fn deinit(self: *LuaEngine) void {
        // redis_ctx is NOT owned by LuaEngine — caller manages its lifetime
        lua.lua_close(self.L);
    }

    /// Lua debug hook to check script timeout and kill requests
    /// Called periodically during script execution (every N instructions)
    fn timeoutHook(L: *lua.lua_State, _: *lua.lua_Debug) callconv(.c) void {
        // Retrieve the LuaEngine pointer from the registry
        lua.lua_pushstring(L, "LUA_ENGINE");
        lua.lua_gettable(L, lua.LUA_REGISTRYINDEX);

        const engine_ptr = lua.lua_touserdata(L, -1);
        lua.lua_pop(L, 1);

        if (engine_ptr) |ptr| {
            const engine: *LuaEngine = @ptrCast(@alignCast(ptr));

            // Check if SCRIPT KILL was requested
            if (engine.redis_ctx) |ctx| {
                if (ctx.script_store.isKillRequested()) {
                    // Clear the kill flag before terminating
                    ctx.script_store.clearKill();
                    // Terminate script with error
                    lua.lua_pushstring(L, "ERR script killed by user");
                    _ = lua.lua_error(L);
                }
            }

            // Check if deadline exceeded
            if (engine.deadline_ns > 0) {
                const now_ns: i64 = @intCast(std.time.nanoTimestamp());
                if (now_ns > engine.deadline_ns) {
                    // Timeout exceeded - raise Lua error
                    lua.lua_pushstring(L, "ERR script execution timeout exceeded");
                    _ = lua.lua_error(L);
                }
            }
        }
    }

    /// Execute a Lua script and return RESP2-formatted response bytes.
    /// Handles compilation errors, runtime errors, and all Lua return types.
    pub fn eval(self: *LuaEngine, script: []const u8, numkeys: usize, keys: []const []const u8, argv: []const []const u8) ![]const u8 {
        // Set up timeout if enabled
        if (self.timeout_ms > 0) {
            const now_ns: i64 = @intCast(std.time.nanoTimestamp());
            const timeout_ns: i64 = @intCast(self.timeout_ms * 1_000_000);
            self.deadline_ns = now_ns + timeout_ns;

            lua.lua_pushstring(self.L, "LUA_ENGINE");
            lua.lua_pushlightuserdata(self.L, @ptrCast(self));
            lua.lua_settable(self.L, lua.LUA_REGISTRYINDEX);

            _ = lua.lua_sethook(self.L, timeoutHook, lua.LUA_MASKCOUNT, 1000);
        }
        defer {
            if (self.timeout_ms > 0) {
                _ = lua.lua_sethook(self.L, null, 0, 0);
                self.deadline_ns = 0;
            }
        }

        const script_z = try self.allocator.dupeZ(u8, script);
        defer self.allocator.free(script_z);

        const load_result = lua.luaL_loadstring(self.L, script_z.ptr);
        if (load_result != lua.LUA_OK) {
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "-ERR Error compiling script: {s}\r\n", .{err_str});
            lua.lua_pop(self.L, 1);
            return result;
        }

        // Use rawset to bypass __newindex sandbox when setting KEYS and ARGV globals.
        // Pattern: push key-string, push table, then lua_rawset on LUA_GLOBALSINDEX.
        lua.lua_pushstring(self.L, "KEYS");
        lua.lua_createtable(self.L, @intCast(keys.len), 0);
        for (keys, 0..) |key, i| {
            lua.lua_pushlstring(self.L, key.ptr, key.len);
            lua.lua_rawseti(self.L, -2, @intCast(i + 1));
        }
        lua.lua_rawset(self.L, lua.LUA_GLOBALSINDEX);

        lua.lua_pushstring(self.L, "ARGV");
        lua.lua_createtable(self.L, @intCast(argv.len), 0);
        for (argv, 0..) |arg, i| {
            lua.lua_pushlstring(self.L, arg.ptr, arg.len);
            lua.lua_rawseti(self.L, -2, @intCast(i + 1));
        }
        lua.lua_rawset(self.L, lua.LUA_GLOBALSINDEX);

        const call_result = lua.lua_pcall(self.L, 0, 1, 0);
        if (call_result != lua.LUA_OK) {
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "-ERR Error running script: {s}\r\n", .{err_str});
            lua.lua_pop(self.L, 1);
            return result;
        }

        var buf = std.ArrayList(u8){};
        errdefer buf.deinit(self.allocator);
        try self.luaToRESP2Buf(&buf, lua.lua_gettop(self.L));
        lua.lua_pop(self.L, 1);

        _ = numkeys;
        return buf.toOwnedSlice(self.allocator);
    }

    /// Convert a Lua value at absolute stack index to RESP2 bytes, appended to buf.
    /// Redis conversion rules:
    ///   number  → integer reply (:N\r\n)
    ///   string  → bulk string reply ($N\r\n...\r\n)
    ///   nil     → null bulk string ($-1\r\n)
    ///   true    → integer 1 (:1\r\n)
    ///   false   → null bulk string ($-1\r\n)
    ///   {err=s} → error reply (-s\r\n)
    ///   {ok=s}  → simple string (+s\r\n)
    ///   table   → array reply (*N\r\n...)
    fn luaToRESP2Buf(self: *LuaEngine, buf: *std.ArrayList(u8), abs_idx: c_int) !void {
        const vtype = lua.lua_type(self.L, abs_idx);
        switch (vtype) {
            lua.LUA_TNIL => {
                try buf.appendSlice(self.allocator, "$-1\r\n");
            },
            lua.LUA_TBOOLEAN => {
                if (lua.lua_toboolean(self.L, abs_idx) != 0) {
                    try buf.appendSlice(self.allocator, ":1\r\n");
                } else {
                    try buf.appendSlice(self.allocator, "$-1\r\n");
                }
            },
            lua.LUA_TNUMBER => {
                const num = lua.lua_tonumber(self.L, abs_idx);
                const int_val: i64 = @intFromFloat(num);
                try std.fmt.format(buf.writer(self.allocator), ":{d}\r\n", .{int_val});
            },
            lua.LUA_TSTRING => {
                var len: usize = 0;
                const str_ptr = lua.lua_tolstring(self.L, abs_idx, &len);
                if (str_ptr) |ptr| {
                    try std.fmt.format(buf.writer(self.allocator), "${d}\r\n", .{len});
                    try buf.appendSlice(self.allocator, ptr[0..len]);
                    try buf.appendSlice(self.allocator, "\r\n");
                } else {
                    try buf.appendSlice(self.allocator, "$-1\r\n");
                }
            },
            lua.LUA_TTABLE => {
                // Check for {err = "..."} — redis.call error table
                lua.lua_pushstring(self.L, "err");
                lua.lua_rawget(self.L, abs_idx);
                if (lua.lua_type(self.L, -1) == lua.LUA_TSTRING) {
                    var err_len: usize = 0;
                    const err_ptr = lua.lua_tolstring(self.L, -1, &err_len);
                    lua.lua_pop(self.L, 1);
                    if (err_ptr) |ptr| {
                        try buf.append(self.allocator, '-');
                        try buf.appendSlice(self.allocator, ptr[0..err_len]);
                        try buf.appendSlice(self.allocator, "\r\n");
                        return;
                    }
                } else {
                    lua.lua_pop(self.L, 1);
                }

                // Check for {ok = "..."} — status reply table
                lua.lua_pushstring(self.L, "ok");
                lua.lua_rawget(self.L, abs_idx);
                if (lua.lua_type(self.L, -1) == lua.LUA_TSTRING) {
                    var ok_len: usize = 0;
                    const ok_ptr = lua.lua_tolstring(self.L, -1, &ok_len);
                    lua.lua_pop(self.L, 1);
                    if (ok_ptr) |ptr| {
                        try buf.append(self.allocator, '+');
                        try buf.appendSlice(self.allocator, ptr[0..ok_len]);
                        try buf.appendSlice(self.allocator, "\r\n");
                        return;
                    }
                } else {
                    lua.lua_pop(self.L, 1);
                }

                // Array table: count sequential integer keys starting at 1
                var count: usize = 0;
                while (true) {
                    lua.lua_rawgeti(self.L, abs_idx, @intCast(count + 1));
                    const elem_type = lua.lua_type(self.L, -1);
                    lua.lua_pop(self.L, 1);
                    if (elem_type == lua.LUA_TNIL) break;
                    count += 1;
                }
                try std.fmt.format(buf.writer(self.allocator), "*{d}\r\n", .{count});
                for (0..count) |i| {
                    lua.lua_rawgeti(self.L, abs_idx, @intCast(i + 1));
                    try self.luaToRESP2Buf(buf, lua.lua_gettop(self.L));
                    lua.lua_pop(self.L, 1);
                }
            },
            else => {
                try buf.appendSlice(self.allocator, "$-1\r\n");
            },
        }
    }

    /// Convert Lua stack value to a raw Zig string (used by callFunction for FCALL).
    fn luaValueToString(self: *LuaEngine, idx: c_int) ![]const u8 {
        const vtype = lua.lua_type(self.L, idx);
        switch (vtype) {
            lua.LUA_TNIL => return try self.allocator.dupe(u8, "nil"),
            lua.LUA_TBOOLEAN => {
                const val = lua.lua_toboolean(self.L, idx);
                return if (val != 0) try self.allocator.dupe(u8, "true") else try self.allocator.dupe(u8, "false");
            },
            lua.LUA_TNUMBER => {
                const num = lua.lua_tonumber(self.L, idx);
                return try std.fmt.allocPrint(self.allocator, "{d}", .{num});
            },
            lua.LUA_TSTRING => {
                var len: usize = 0;
                const str_ptr = lua.lua_tolstring(self.L, idx, &len);
                if (str_ptr) |ptr| {
                    return try self.allocator.dupe(u8, ptr[0..len]);
                } else {
                    return try self.allocator.dupe(u8, "");
                }
            },
            else => {
                const type_name = lua.lua_typename(self.L, vtype);
                const type_str = std.mem.span(type_name);
                return try std.fmt.allocPrint(self.allocator, "[{s}]", .{type_str});
            },
        }
    }

    /// Load a function library and collect registered functions
    /// Returns error if library fails to compile or execute
    /// reg_context must be allocated by caller and will be populated with registered functions
    pub fn loadLibrary(self: *LuaEngine, library_code: []const u8, reg_context: *@import("../storage/functions.zig").FunctionRegistrationContext) !void {
        // Store registration context in Lua registry
        lua.lua_pushstring(self.L, "FUNCTION_REG_CONTEXT");
        lua.lua_pushlightuserdata(self.L, @ptrCast(reg_context));
        lua.lua_settable(self.L, lua.LUA_REGISTRYINDEX);

        // Register redis.register_function() API
        redis_api.registerFunctionsApi(self.L);

        // Load and execute the library code
        const code_z = try self.allocator.dupeZ(u8, library_code);
        defer self.allocator.free(code_z);

        const load_result = lua.luaL_loadstring(self.L, code_z.ptr);
        if (load_result != lua.LUA_OK) {
            _ = lua.lua_tostring(self.L, -1) orelse "unknown error";
            lua.lua_pop(self.L, 1);
            return error.LuaCompileError;
        }

        // Execute library code (calls redis.register_function())
        const call_result = lua.lua_pcall(self.L, 0, 0, 0);
        if (call_result != lua.LUA_OK) {
            _ = lua.lua_tostring(self.L, -1) orelse "unknown error";
            lua.lua_pop(self.L, 1);
            return error.LuaExecutionError;
        }

        // Clean up registry
        lua.lua_pushstring(self.L, "FUNCTION_REG_CONTEXT");
        lua.lua_pushnil(self.L);
        lua.lua_settable(self.L, lua.LUA_REGISTRYINDEX);
    }

    /// Call a registered function by name
    /// Returns the result as a string (caller must free)
    pub fn callFunction(self: *LuaEngine, library_code: []const u8, function_name: []const u8, numkeys: usize, keys: []const []const u8, argv: []const []const u8) ![]const u8 {
        // Set up timeout if enabled
        if (self.timeout_ms > 0) {
            const now_ns: i64 = @intCast(std.time.nanoTimestamp());
            const timeout_ns: i64 = @intCast(self.timeout_ms * 1_000_000);
            self.deadline_ns = now_ns + timeout_ns;

            lua.lua_pushstring(self.L, "LUA_ENGINE");
            lua.lua_pushlightuserdata(self.L, @ptrCast(self));
            lua.lua_settable(self.L, lua.LUA_REGISTRYINDEX);

            _ = lua.lua_sethook(self.L, timeoutHook, lua.LUA_MASKCOUNT, 1000);
        }
        defer {
            if (self.timeout_ms > 0) {
                _ = lua.lua_sethook(self.L, null, 0, 0);
                self.deadline_ns = 0;
            }
        }

        // Load library code
        const code_z = try self.allocator.dupeZ(u8, library_code);
        defer self.allocator.free(code_z);

        const load_result = lua.luaL_loadstring(self.L, code_z.ptr);
        if (load_result != lua.LUA_OK) {
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "ERR Error compiling library: {s}", .{err_str});
            lua.lua_pop(self.L, 1);
            return result;
        }

        // Execute library to define functions
        const exec_result = lua.lua_pcall(self.L, 0, 0, 0);
        if (exec_result != lua.LUA_OK) {
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "ERR Error loading library: {s}", .{err_str});
            lua.lua_pop(self.L, 1);
            return result;
        }

        // Set KEYS using rawset to bypass __newindex sandbox
        lua.lua_pushstring(self.L, "KEYS");
        lua.lua_createtable(self.L, @intCast(keys.len), 0);
        for (keys, 0..) |key, i| {
            lua.lua_pushlstring(self.L, key.ptr, key.len);
            lua.lua_rawseti(self.L, -2, @intCast(i + 1));
        }
        lua.lua_rawset(self.L, lua.LUA_GLOBALSINDEX);

        lua.lua_pushstring(self.L, "ARGV");
        lua.lua_createtable(self.L, @intCast(argv.len), 0);
        for (argv, 0..) |arg, i| {
            lua.lua_pushlstring(self.L, arg.ptr, arg.len);
            lua.lua_rawseti(self.L, -2, @intCast(i + 1));
        }
        lua.lua_rawset(self.L, lua.LUA_GLOBALSINDEX);

        // Get the function from global table
        const func_z = try self.allocator.dupeZ(u8, function_name);
        defer self.allocator.free(func_z);

        lua.lua_getfield(self.L, lua.LUA_GLOBALSINDEX, func_z.ptr);
        if (lua.lua_type(self.L, -1) != lua.LUA_TFUNCTION) {
            lua.lua_pop(self.L, 1);
            return try std.fmt.allocPrint(self.allocator, "ERR Function '{s}' not found", .{function_name});
        }

        // Call the function
        const call_result = lua.lua_pcall(self.L, 0, 1, 0);
        if (call_result != lua.LUA_OK) {
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "ERR Error calling function: {s}", .{err_str});
            lua.lua_pop(self.L, 1);
            return result;
        }

        // Get the return value
        const result = try self.luaValueToString(-1);
        lua.lua_pop(self.L, 1);

        _ = numkeys;
        return result;
    }
};

// Unit tests
test "LuaEngine: basic initialization" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0); // 0 = no timeout
    defer engine.deinit();

    // If we got here, initialization succeeded
    try std.testing.expect(true);
}

test "LuaEngine: simple script evaluation" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return 42", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":42\r\n", result);
}

test "LuaEngine: script with string return" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return 'hello world'", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$11\r\nhello world\r\n", result);
}

test "LuaEngine: script with KEYS access" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return KEYS[1]", 1, &.{"mykey"}, &.{});
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nmykey\r\n", result);
}

test "LuaEngine: script with ARGV access" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return ARGV[1] .. ' ' .. ARGV[2]", 0, &.{}, &.{ "hello", "world" });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$11\r\nhello world\r\n", result);
}

test "LuaEngine: script compilation error" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return 42 +", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR Error compiling script"));
}

test "LuaEngine: script runtime error" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return nil + 1", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR Error running script"));
}

test "LuaEngine: boolean true return value" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return true", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "LuaEngine: boolean false return value" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return false", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "LuaEngine: nil return value" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const result = try engine.eval("return nil", 0, &.{}, &.{});
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

// Tests for redis.call() and redis.pcall()
fn mockExecuteFn(allocator: std.mem.Allocator, cmd: RespValue) anyerror![]const u8 {
    const array = cmd.array;
    if (array.len == 0) {
        const writer_mod = @import("../protocol/writer.zig");
        var w = writer_mod.Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR empty command");
    }

    const cmd_name = array[0].bulk_string;

    // Mock GET command
    if (std.mem.eql(u8, cmd_name, "GET")) {
        const writer_mod = @import("../protocol/writer.zig");
        var w = writer_mod.Writer.init(allocator);
        defer w.deinit();
        return w.writeBulkString("value123");
    }

    // Mock SET command
    if (std.mem.eql(u8, cmd_name, "SET")) {
        const writer_mod = @import("../protocol/writer.zig");
        var w = writer_mod.Writer.init(allocator);
        defer w.deinit();
        return w.writeSimpleString("OK");
    }

    // Mock command that returns error
    if (std.mem.eql(u8, cmd_name, "ERRORTEST")) {
        const writer_mod = @import("../protocol/writer.zig");
        var w = writer_mod.Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR mock error");
    }

    const writer_mod = @import("../protocol/writer.zig");
    var w = writer_mod.Writer.init(allocator);
    defer w.deinit();
    return w.writeError("ERR unknown command");
}

test "LuaEngine: redis.call() basic execution" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, mockExecuteFn, 0);
    defer engine.deinit();

    const script = "return redis.call('GET', 'mykey')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("value123", result);
}

test "LuaEngine: redis.call() with SET command" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, mockExecuteFn, 0);
    defer engine.deinit();

    const script = "return redis.call('SET', 'mykey', 'myvalue')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("OK", result);
}

test "LuaEngine: redis.pcall() error handling" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, mockExecuteFn, 0);
    defer engine.deinit();

    const script = "local result = redis.pcall('ERRORTEST'); if result.err then return 'caught: ' .. result.err else return 'no error' end";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR mock error") != null);
}

// Sandboxing tests
test "LuaEngine: sandbox blocks os module" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "return os.execute('echo hello')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error because os is nil
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR Error running script") != null);
}

test "LuaEngine: sandbox blocks io module" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "return io.open('/etc/passwd', 'r')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error because io is nil
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR Error running script") != null);
}

test "LuaEngine: sandbox blocks loadfile" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "return loadfile('/tmp/evil.lua')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error because loadfile is nil
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR Error running script") != null);
}

test "LuaEngine: sandbox blocks dofile" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "return dofile('/tmp/evil.lua')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error because dofile is nil
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR Error running script") != null);
}

test "LuaEngine: sandbox allows safe libraries" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "return math.abs(-42) + string.len('hello')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("47", result);
}

test "LuaEngine: sandbox blocks require of dangerous modules" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "return require('os')";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error with require restriction message
    try std.testing.expect(std.mem.indexOf(u8, result, "require is restricted") != null);
}

test "LuaEngine: sandbox allows require of safe libraries" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    // math library should be allowed
    const script = "local m = require('math'); return m.pi";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should succeed and return pi
    try std.testing.expect(std.mem.startsWith(u8, result, "3.14"));
}

test "LuaEngine: sandbox blocks global variable creation" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "evil_global = 42; return evil_global";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error with global variable creation message
    try std.testing.expect(std.mem.indexOf(u8, result, "create global variable") != null);
}

test "LuaEngine: sandbox allows local variables" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0);
    defer engine.deinit();

    const script = "local safe_var = 42; return safe_var";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("42", result);
}

// Timeout tests
test "LuaEngine: script without timeout executes normally" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 0); // 0 = no timeout
    defer engine.deinit();

    const script = "local x = 0; for i = 1, 1000000 do x = x + i end; return x";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should complete successfully
    try std.testing.expect(!std.mem.startsWith(u8, result, "ERR"));
}

test "LuaEngine: script with timeout executes normally if within limit" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 5000); // 5 second timeout
    defer engine.deinit();

    const script = "return 42";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("42", result);
}

test "LuaEngine: script exceeding timeout is terminated" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 100); // 100ms timeout
    defer engine.deinit();

    // Infinite loop that will exceed timeout
    const script = "while true do end";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error with timeout message
    try std.testing.expect(std.mem.indexOf(u8, result, "timeout exceeded") != null);
}

test "LuaEngine: script with long computation exceeding timeout" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 50); // 50ms timeout
    defer engine.deinit();

    // Long computation that exceeds timeout
    const script = "local x = 0; for i = 1, 100000000 do x = x + i end; return x";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    // Should error with timeout message
    try std.testing.expect(std.mem.indexOf(u8, result, "timeout exceeded") != null);
}

test "LuaEngine: timeout hook is cleared after execution" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null, 5000); // 5s timeout
    defer engine.deinit();

    const script = "return 'first'";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    // First execution
    {
        const result = try engine.eval(script, 0, keys, argv);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("first", result);
    }

    // Second execution - hook should be re-set properly
    const script2 = "return 'second'";
    {
        const result = try engine.eval(script2, 0, keys, argv);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("second", result);
    }

    // Deadline should be cleared
    try std.testing.expectEqual(@as(i64, 0), engine.deadline_ns);
}
