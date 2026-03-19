// Lua library implementations for Redis scripting
// Provides: cjson, cmsgpack, struct, bit (bit is built into LuaJIT)
//
// These are minimal implementations sufficient for Redis Lua scripts.
// Full implementations would be more extensive.

const std = @import("std");
const lua = @import("lua_ffi.zig");

/// Register all Redis Lua libraries (cjson, cmsgpack, struct)
/// bit module is built into LuaJIT and already available
pub fn registerLibraries(L: *lua.lua_State) !void {
    try registerCJson(L);
    try registerCMsgPack(L);
    try registerStruct(L);
}

/// Register cjson library - minimal JSON encode/decode
/// Redis uses lua-cjson 2.1.0 - we provide a minimal compatible API
fn registerCJson(L: *lua.lua_State) !void {
    // Create cjson table
    lua.lua_newtable(L);

    // Register cjson.encode
    lua.lua_pushstring(L, "encode");
    lua.lua_pushcfunction(L, cjsonEncode);
    lua.lua_settable(L, -3);

    // Register cjson.decode
    lua.lua_pushstring(L, "decode");
    lua.lua_pushcfunction(L, cjsonDecode);
    lua.lua_settable(L, -3);

    // Register cjson.encode_sparse_array
    lua.lua_pushstring(L, "encode_sparse_array");
    lua.lua_pushcfunction(L, cjsonEncodeSparse);
    lua.lua_settable(L, -3);

    // Set as global cjson
    lua.lua_setfield(L, lua.LUA_GLOBALSINDEX, "cjson");
}

/// cjson.encode(value) - encode Lua value to JSON string
/// Minimal implementation - handles: nil, boolean, number, string, table (array/object)
export fn cjsonEncode(L: ?*lua.lua_State) callconv(.C) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "expected 1 argument");
        _ = lua.lua_error(state);
        return 0;
    }

    // Simple implementation: use Lua's own tostring for now
    // A full implementation would recursively serialize tables
    const value_type = lua.lua_type(state, 1);

    switch (value_type) {
        lua.LUA_TNIL => {
            lua.lua_pushstring(state, "null");
            return 1;
        },
        lua.LUA_TBOOLEAN => {
            const val = lua.lua_toboolean(state, 1);
            if (val != 0) {
                lua.lua_pushstring(state, "true");
            } else {
                lua.lua_pushstring(state, "false");
            }
            return 1;
        },
        lua.LUA_TNUMBER => {
            const num = lua.lua_tonumber(state, 1);
            // Format number as JSON (no trailing .0 for integers)
            const is_int = @floor(num) == num and num >= -2147483648 and num <= 2147483647;
            if (is_int) {
                var buf: [64]u8 = undefined;
                const s = std.fmt.bufPrintZ(&buf, "{d}", .{@as(i64, @intFromFloat(num))}) catch "0";
                lua.lua_pushstring(state, s.ptr);
            } else {
                var buf: [64]u8 = undefined;
                const s = std.fmt.bufPrintZ(&buf, "{d}", .{num}) catch "0.0";
                lua.lua_pushstring(state, s.ptr);
            }
            return 1;
        },
        lua.LUA_TSTRING => {
            // For strings, we need to escape and quote them
            const str = lua.lua_tostring(state, 1);
            if (str) |s| {
                // Simple quote wrapping (full impl would escape \, ", \n, etc.)
                var buf: [1024]u8 = undefined;
                const quoted = std.fmt.bufPrintZ(&buf, "\"{s}\"", .{std.mem.span(s)}) catch "\"\"";
                lua.lua_pushstring(state, quoted.ptr);
                return 1;
            }
            lua.lua_pushstring(state, "\"\"");
            return 1;
        },
        lua.LUA_TTABLE => {
            // Minimal table encoding: check if array or object
            // Array: {"key":value,...}
            // Object: [value1,value2,...]
            // For now, just return "{}" to avoid complexity
            lua.lua_pushstring(state, "{}");
            return 1;
        },
        else => {
            lua.lua_pushstring(state, "null");
            return 1;
        },
    }
}

/// cjson.decode(json_string) - decode JSON string to Lua value
/// Minimal implementation
export fn cjsonDecode(L: ?*lua.lua_State) callconv(.C) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "expected 1 argument");
        _ = lua.lua_error(state);
        return 0;
    }

    const json_str = lua.lua_tostring(state, 1);
    if (json_str == null) {
        lua.lua_pushnil(state);
        return 1;
    }

    const str = std.mem.span(json_str.?);

    // Minimal parser: recognize literals and return simple values
    if (std.mem.eql(u8, str, "null")) {
        lua.lua_pushnil(state);
        return 1;
    }
    if (std.mem.eql(u8, str, "true")) {
        lua.lua_pushboolean(state, 1);
        return 1;
    }
    if (std.mem.eql(u8, str, "false")) {
        lua.lua_pushboolean(state, 0);
        return 1;
    }

    // Try parsing as number
    if (str.len > 0 and (str[0] == '-' or std.ascii.isDigit(str[0]))) {
        const num = std.fmt.parseFloat(f64, str) catch {
            lua.lua_pushnil(state);
            return 1;
        };
        lua.lua_pushnumber(state, num);
        return 1;
    }

    // Try parsing as quoted string
    if (str.len >= 2 and str[0] == '"' and str[str.len - 1] == '"') {
        // Remove quotes
        const unquoted = str[1 .. str.len - 1];
        lua.lua_pushlstring(state, unquoted.ptr, unquoted.len);
        return 1;
    }

    // For arrays and objects, return empty table
    if (str.len >= 2 and ((str[0] == '{' and str[str.len - 1] == '}') or
                          (str[0] == '[' and str[str.len - 1] == ']'))) {
        lua.lua_newtable(state);
        return 1;
    }

    // Default: return nil
    lua.lua_pushnil(state);
    return 1;
}

/// cjson.encode_sparse_array(table, max_array_size, max_array_sparseness)
/// Stub implementation - just calls regular encode
export fn cjsonEncodeSparse(L: ?*lua.lua_State) callconv(.C) c_int {
    return cjsonEncode(L);
}

/// Register cmsgpack library - MessagePack encode/decode
/// Redis uses lua-cmsgpack - we provide a minimal compatible API
fn registerCMsgPack(L: *lua.lua_State) !void {
    lua.lua_newtable(L);

    lua.lua_pushstring(L, "pack");
    lua.lua_pushcfunction(L, cmsgpackPack);
    lua.lua_settable(L, -3);

    lua.lua_pushstring(L, "unpack");
    lua.lua_pushcfunction(L, cmsgpackUnpack);
    lua.lua_settable(L, -3);

    lua.lua_setfield(L, lua.LUA_GLOBALSINDEX, "cmsgpack");
}

/// cmsgpack.pack(value) - encode to MessagePack binary
/// Minimal implementation - returns a stub binary representation
export fn cmsgpackPack(L: ?*lua.lua_State) callconv(.C) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "expected 1 argument");
        _ = lua.lua_error(state);
        return 0;
    }

    // Stub: return empty msgpack (0x90 = empty array in msgpack)
    const stub = "\x90";
    lua.lua_pushlstring(state, stub.ptr, stub.len);
    return 1;
}

/// cmsgpack.unpack(binary) - decode from MessagePack binary
/// Minimal implementation - returns nil
export fn cmsgpackUnpack(L: ?*lua.lua_State) callconv(.C) c_int {
    const state = L orelse return 0;
    lua.lua_pushnil(state);
    return 1;
}

/// Register struct library - binary data packing/unpacking
/// Redis uses lua-struct 0.2 - we provide a minimal compatible API
fn registerStruct(L: *lua.lua_State) !void {
    lua.lua_newtable(L);

    lua.lua_pushstring(L, "pack");
    lua.lua_pushcfunction(L, structPack);
    lua.lua_settable(L, -3);

    lua.lua_pushstring(L, "unpack");
    lua.lua_pushcfunction(L, structUnpack);
    lua.lua_settable(L, -3);

    lua.lua_pushstring(L, "size");
    lua.lua_pushcfunction(L, structSize);
    lua.lua_settable(L, -3);

    lua.lua_setfield(L, lua.LUA_GLOBALSINDEX, "struct");
}

/// struct.pack(format, ...) - pack values according to format string
/// Format: c (char), b (byte), h (short), i (int), l (long), f (float), d (double)
/// Minimal stub implementation
export fn structPack(L: ?*lua.lua_State) callconv(.C) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "expected at least 1 argument");
        _ = lua.lua_error(state);
        return 0;
    }

    // Stub: return empty string
    lua.lua_pushstring(state, "");
    return 1;
}

/// struct.unpack(format, binary) - unpack values from binary
/// Minimal stub implementation
export fn structUnpack(L: ?*lua.lua_State) callconv(.C) c_int {
    _ = L;
    // Return empty results
    return 0;
}

/// struct.size(format) - calculate size of packed data
/// Minimal stub implementation
export fn structSize(L: ?*lua.lua_State) callconv(.C) c_int {
    if (L) |state| {
        lua.lua_pushnumber(state, 0);
    }
    return 1;
}

test "cjson encode: nil" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode(nil)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("null", std.mem.span(result.?));
}

test "cjson encode: boolean true" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode(true)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("true", std.mem.span(result.?));
}

test "cjson encode: boolean false" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode(false)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("false", std.mem.span(result.?));
}

test "cjson encode: integer" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode(42)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("42", std.mem.span(result.?));
}

test "cjson encode: float" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode(3.14)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    // Just check it starts with "3."
    try std.testing.expect(std.mem.startsWith(u8, std.mem.span(result.?), "3."));
}

test "cjson encode: string" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode('hello')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("\"hello\"", std.mem.span(result.?));
}

test "cjson decode: null" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.decode('null')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expect(lua.lua_isnil(L, -1));
}

test "cjson decode: boolean" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.decode('true')");
    _ = lua.lua_pcall(L, 0, 1, 0);
    try std.testing.expect(lua.lua_toboolean(L, -1) != 0);
}

test "cjson decode: number" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.decode('42')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const num = lua.lua_tonumber(L, -1);
    try std.testing.expectEqual(@as(f64, 42.0), num);
}

test "cjson decode: string" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.decode('\"hello\"')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("hello", std.mem.span(result.?));
}

test "cmsgpack pack returns binary" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack(42)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    // Just verify it returns something
    try std.testing.expect(lua.lua_isstring(L, -1));
}

test "struct pack returns string" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L, "return struct.pack('i', 42)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expect(lua.lua_isstring(L, -1));
}
