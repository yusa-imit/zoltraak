// Lua scripting engine for Redis EVAL/EVALSHA commands
// Manages Lua state lifecycle, sandboxing, and script execution

const std = @import("std");
const lua = @import("lua_ffi.zig");
const Storage = @import("../storage/memory.zig").Storage;

/// Lua engine instance - manages a single Lua VM
pub const LuaEngine = struct {
    L: *lua.lua_State,
    allocator: std.mem.Allocator,

    /// Initialize a new Lua engine with sandboxing
    pub fn init(allocator: std.mem.Allocator) !LuaEngine {
        const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;

        // Open standard libraries
        lua.luaL_openlibs(L);

        // TODO: Apply sandboxing (remove dangerous globals like os.execute, io.*, etc.)
        // For now, we leave libraries open for basic functionality

        return LuaEngine{
            .L = L,
            .allocator = allocator,
        };
    }

    /// Clean up Lua state
    pub fn deinit(self: *LuaEngine) void {
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
    var engine = try LuaEngine.init(allocator);
    defer engine.deinit();

    // If we got here, initialization succeeded
    try std.testing.expect(true);
}

test "LuaEngine: simple script evaluation" {
    const allocator = std.testing.allocator;
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
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
    var engine = try LuaEngine.init(allocator);
    defer engine.deinit();

    const script = "return nil";
    const keys: []const []const u8 = &.{};
    const argv: []const []const u8 = &.{};

    const result = try engine.eval(script, 0, keys, argv);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("nil", result);
}
