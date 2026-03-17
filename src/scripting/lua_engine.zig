// Lua scripting engine for Redis EVAL/EVALSHA commands
// Manages Lua state lifecycle, sandboxing, and script execution

const std = @import("std");
const lua = @import("lua_ffi.zig");
const redis_api = @import("redis_api.zig");
const Storage = @import("../storage/memory.zig").Storage;
const protocol = @import("../protocol/parser.zig");
const RespValue = protocol.RespValue;

pub const RedisContext = redis_api.RedisContext;

/// Lua engine instance - manages a single Lua VM
pub const LuaEngine = struct {
    L: *lua.lua_State,
    allocator: std.mem.Allocator,
    redis_ctx: ?*RedisContext,

    /// Initialize a new Lua engine with sandboxing
    /// If redis_ctx is provided, registers redis.call() and redis.pcall()
    pub fn init(allocator: std.mem.Allocator, redis_ctx: ?*RedisContext) !LuaEngine {
        const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;

        // Open standard libraries
        lua.luaL_openlibs(L);

        // Apply sandboxing to restrict dangerous operations
        applySandbox(L);

        const engine = LuaEngine{
            .L = L,
            .allocator = allocator,
            .redis_ctx = redis_ctx,
        };

        // Register redis.call() and redis.pcall() if context provided
        if (redis_ctx) |ctx| {
            try redis_api.registerRedisApi(L, ctx);
        }

        return engine;
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
        if (self.redis_ctx) |ctx| {
            self.allocator.destroy(ctx);
        }
        lua.lua_close(self.L);
    }

    /// Execute a Lua script and return the result
    /// Returns error if script fails to compile or execute
    pub fn eval(self: *LuaEngine, script: []const u8, numkeys: usize, keys: []const []const u8, argv: []const []const u8) ![]const u8 {
        // Load the script
        const script_z = try self.allocator.dupeZ(u8, script);
        defer self.allocator.free(script_z);

        const load_result = lua.luaL_loadstring(self.L, script_z.ptr);
        if (load_result != lua.LUA_OK) {
            // Script failed to compile
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "ERR Error compiling script: {s}", .{err_str});
            lua.lua_pop(self.L, 1); // pop error message
            return result;
        }

        // Create KEYS table
        lua.lua_createtable(self.L, @intCast(keys.len), 0);
        for (keys, 0..) |key, i| {
            lua.lua_pushlstring(self.L, key.ptr, key.len);
            lua.lua_rawseti(self.L, -2, @intCast(i + 1)); // Lua arrays are 1-indexed
        }
        lua.lua_setfield(self.L, lua.LUA_GLOBALSINDEX, "KEYS");

        // Create ARGV table
        lua.lua_createtable(self.L, @intCast(argv.len), 0);
        for (argv, 0..) |arg, i| {
            lua.lua_pushlstring(self.L, arg.ptr, arg.len);
            lua.lua_rawseti(self.L, -2, @intCast(i + 1)); // Lua arrays are 1-indexed
        }
        lua.lua_setfield(self.L, lua.LUA_GLOBALSINDEX, "ARGV");

        // Execute the script (0 args pushed explicitly, script gets KEYS/ARGV from globals)
        const call_result = lua.lua_pcall(self.L, 0, 1, 0);
        if (call_result != lua.LUA_OK) {
            // Script failed to execute
            const err_msg = lua.lua_tostring(self.L, -1) orelse "unknown error";
            const err_str = std.mem.span(err_msg);
            const result = try std.fmt.allocPrint(self.allocator, "ERR Error running script: {s}", .{err_str});
            lua.lua_pop(self.L, 1); // pop error message
            return result;
        }

        // Get the return value
        const result = try self.luaValueToString(-1);
        lua.lua_pop(self.L, 1); // pop return value

        _ = numkeys; // unused for now
        return result;
    }

    /// Convert Lua stack value to Zig string
    fn luaValueToString(self: *LuaEngine, idx: c_int) ![]const u8 {
        const vtype = lua.lua_type(self.L, idx);

        switch (vtype) {
            lua.LUA_TNIL => {
                return try self.allocator.dupe(u8, "nil");
            },
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
            lua.LUA_TTABLE => {
                // For now, return a simple table representation
                return try self.allocator.dupe(u8, "[table]");
            },
            else => {
                const type_name = lua.lua_typename(self.L, vtype);
                const type_str = std.mem.span(type_name);
                return try std.fmt.allocPrint(self.allocator, "[{s}]", .{type_str});
            },
        }
    }
};

// Unit tests
test "LuaEngine: basic initialization" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    // If we got here, initialization succeeded
    try std.testing.expect(true);
}

test "LuaEngine: simple script evaluation" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return 42";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("42", result);
}

test "LuaEngine: script with string return" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return 'hello world'";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("hello world", result);
}

test "LuaEngine: script with KEYS access" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return KEYS[1]";
    const keys: []const []const u8 = &.{"mykey"};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 1, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("mykey", result);
}

test "LuaEngine: script with ARGV access" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return ARGV[1] .. ' ' .. ARGV[2]";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{"hello", "world"};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("hello world", result);
}

test "LuaEngine: script compilation error" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return 42 +"; // Invalid syntax
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR Error compiling script"));
}

test "LuaEngine: script runtime error" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return nil + 1"; // Runtime error: attempt to perform arithmetic on nil
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR Error running script"));
}

test "LuaEngine: boolean return value" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return true";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("true", result);
}

test "LuaEngine: nil return value" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "return nil";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("nil", result);
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
    var engine = try LuaEngine.init(allocator, mockExecuteFn);
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
    var engine = try LuaEngine.init(allocator, mockExecuteFn);
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
    var engine = try LuaEngine.init(allocator, mockExecuteFn);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
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
    var engine = try LuaEngine.init(allocator, null);
    defer engine.deinit();

    const script = "local safe_var = 42; return safe_var";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("42", result);
}
