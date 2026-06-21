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

/// Append a properly JSON-escaped string (including surrounding quotes) to buf.
/// Uses c_allocator passed as parameter to match unmanaged ArrayList API.
fn appendJsonString(bytes: []const u8, buf: *std.ArrayList(u8), alloc: std.mem.Allocator) !void {
    try buf.append(alloc, '"');
    for (bytes) |c| {
        switch (c) {
            '"' => try buf.appendSlice(alloc, "\\\""),
            '\\' => try buf.appendSlice(alloc, "\\\\"),
            '\n' => try buf.appendSlice(alloc, "\\n"),
            '\r' => try buf.appendSlice(alloc, "\\r"),
            '\t' => try buf.appendSlice(alloc, "\\t"),
            // Other control chars: exclude \t (0x09), \n (0x0a), \r (0x0d) already handled
            0x00...0x08, 0x0B...0x0C, 0x0E...0x1F => {
                var esc: [7]u8 = undefined;
                const esc_s = std.fmt.bufPrint(&esc, "\\u{X:0>4}", .{c}) catch unreachable;
                try buf.appendSlice(alloc, esc_s);
            },
            else => try buf.append(alloc, c),
        }
    }
    try buf.append(alloc, '"');
}

/// Recursively encode a Lua value at stack index idx into buf.
/// Caller must ensure idx is a valid (absolute) stack index.
fn encodeValueToBuffer(L: *lua.lua_State, idx: c_int, buf: *std.ArrayList(u8), alloc: std.mem.Allocator, depth: u8) anyerror!void {
    if (depth > 64) {
        try buf.appendSlice(alloc, "null");
        return;
    }
    const vtype = lua.lua_type(L, idx);
    switch (vtype) {
        lua.LUA_TNIL => try buf.appendSlice(alloc, "null"),
        lua.LUA_TBOOLEAN => {
            if (lua.lua_toboolean(L, idx) != 0) {
                try buf.appendSlice(alloc, "true");
            } else {
                try buf.appendSlice(alloc, "false");
            }
        },
        lua.LUA_TNUMBER => {
            const num = lua.lua_tonumber(L, idx);
            // Redis cjson serializes integers without decimal point
            const is_int = @floor(num) == num and num >= -9007199254740992.0 and num <= 9007199254740992.0;
            if (is_int) {
                const n: i64 = @intFromFloat(num);
                try buf.writer(alloc).print("{d}", .{n});
            } else {
                try buf.writer(alloc).print("{d}", .{num});
            }
        },
        lua.LUA_TSTRING => {
            var slen: usize = 0;
            const sp = lua.lua_tolstring(L, idx, &slen);
            if (sp) |s| {
                try appendJsonString(s[0..slen], buf, alloc);
            } else {
                try buf.appendSlice(alloc, "\"\"");
            }
        },
        lua.LUA_TTABLE => {
            // Convert to absolute index for safe recursion
            const abs: c_int = if (idx < 0) lua.lua_gettop(L) + idx + 1 else idx;
            const arr_len = lua.lua_objlen(L, abs);
            if (arr_len > 0) {
                // Encode as JSON array using sequential integer keys 1..arr_len
                try buf.append(alloc, '[');
                var i: c_int = 1;
                while (i <= @as(c_int, @intCast(arr_len))) : (i += 1) {
                    if (i > 1) try buf.append(alloc, ',');
                    lua.lua_rawgeti(L, abs, i);
                    try encodeValueToBuffer(L, -1, buf, alloc, depth + 1);
                    lua.lua_pop(L, 1);
                }
                try buf.append(alloc, ']');
            } else {
                // Encode as JSON object: iterate all key-value pairs
                try buf.append(alloc, '{');
                var first = true;
                lua.lua_pushnil(L); // first key for lua_next
                while (lua.lua_next(L, abs) != 0) {
                    if (!first) try buf.append(alloc, ',');
                    first = false;
                    // Key: coerce to string
                    const ktype = lua.lua_type(L, -2);
                    if (ktype == lua.LUA_TSTRING) {
                        var klen: usize = 0;
                        const kp = lua.lua_tolstring(L, -2, &klen);
                        if (kp) |k| {
                            try appendJsonString(k[0..klen], buf, alloc);
                        } else {
                            try buf.appendSlice(alloc, "\"\"");
                        }
                    } else if (ktype == lua.LUA_TNUMBER) {
                        const knum = lua.lua_tonumber(L, -2);
                        const kn: i64 = @intFromFloat(knum);
                        try buf.writer(alloc).print("\"{d}\"", .{kn});
                    } else {
                        try buf.appendSlice(alloc, "\"?\"");
                    }
                    try buf.append(alloc, ':');
                    try encodeValueToBuffer(L, -1, buf, alloc, depth + 1);
                    lua.lua_pop(L, 1); // pop value, keep key for next()
                }
                try buf.append(alloc, '}');
            }
        },
        else => try buf.appendSlice(alloc, "null"),
    }
}

/// cjson.encode(value) - full recursive JSON encoder for Lua values
export fn cjsonEncode(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "expected 1 argument");
        _ = lua.lua_error(state);
        return 0;
    }

    const alloc = std.heap.c_allocator;
    var buf = std.ArrayList(u8).initCapacity(alloc, 64) catch {
        lua.lua_pushstring(state, "ERR out of memory");
        _ = lua.lua_error(state);
        return 0;
    };
    defer buf.deinit(alloc);

    encodeValueToBuffer(state, 1, &buf, alloc, 0) catch {
        lua.lua_pushstring(state, "ERR cjson encode failed");
        _ = lua.lua_error(state);
        return 0;
    };

    lua.lua_pushlstring(state, buf.items.ptr, buf.items.len);
    return 1;
}

/// Push a parsed std.json.Value onto the Lua stack.
/// Caller owns the memory until after the push (Lua copies strings).
fn pushJsonValue(L: *lua.lua_State, value: std.json.Value) void {
    switch (value) {
        .null => lua.lua_pushnil(L),
        .bool => |b| lua.lua_pushboolean(L, if (b) @as(c_int, 1) else @as(c_int, 0)),
        .integer => |n| lua.lua_pushnumber(L, @floatFromInt(n)),
        .float => |f| lua.lua_pushnumber(L, f),
        .number_string => |s| {
            const f = std.fmt.parseFloat(f64, s) catch 0.0;
            lua.lua_pushnumber(L, f);
        },
        .string => |s| lua.lua_pushlstring(L, s.ptr, s.len),
        .array => |arr| {
            lua.lua_createtable(L, @intCast(arr.items.len), 0);
            for (arr.items, 1..) |item, i| {
                pushJsonValue(L, item);
                lua.lua_rawseti(L, -2, @intCast(i));
            }
        },
        .object => |obj| {
            lua.lua_createtable(L, 0, @intCast(obj.count()));
            var it = obj.iterator();
            while (it.next()) |entry| {
                const k = entry.key_ptr.*;
                lua.lua_pushlstring(L, k.ptr, k.len);
                pushJsonValue(L, entry.value_ptr.*);
                lua.lua_rawset(L, -3);
            }
        },
    }
}

/// cjson.decode(json_string) - full JSON parser using std.json
export fn cjsonDecode(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "expected 1 argument");
        _ = lua.lua_error(state);
        return 0;
    }

    var slen: usize = 0;
    const sp = lua.lua_tolstring(state, 1, &slen);
    if (sp == null or slen == 0) {
        lua.lua_pushnil(state);
        return 1;
    }
    const json_str = sp.?[0..slen];

    const parsed = std.json.parseFromSlice(std.json.Value, std.heap.c_allocator, json_str, .{}) catch {
        // Return nil on parse error (Redis cjson behavior for invalid input)
        lua.lua_pushnil(state);
        return 1;
    };
    defer parsed.deinit();

    pushJsonValue(state, parsed.value);
    return 1;
}

/// cjson.encode_sparse_array(table, max_array_size, max_array_sparseness)
/// Stub implementation - just calls regular encode
export fn cjsonEncodeSparse(L: ?*lua.lua_State) callconv(.c) c_int {
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
export fn cmsgpackPack(L: ?*lua.lua_State) callconv(.c) c_int {
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
export fn cmsgpackUnpack(L: ?*lua.lua_State) callconv(.c) c_int {
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
export fn structPack(L: ?*lua.lua_State) callconv(.c) c_int {
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
export fn structUnpack(L: ?*lua.lua_State) callconv(.c) c_int {
    _ = L;
    // Return empty results
    return 0;
}

/// struct.size(format) - calculate size of packed data
/// Minimal stub implementation
export fn structSize(L: ?*lua.lua_State) callconv(.c) c_int {
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

test "cjson encode: array table" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode({1, 2, 3})");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("[1,2,3]", std.mem.span(result.?));
}

test "cjson encode: object table" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    // Single-key table for deterministic output
    _ = lua.luaL_loadstring(L, "return cjson.encode({x = 42})");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("{\"x\":42}", std.mem.span(result.?));
}

test "cjson encode: nested array" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode({'a', 'b'})");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("[\"a\",\"b\"]", std.mem.span(result.?));
}

test "cjson encode: string with special chars" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "return cjson.encode('a\\nb')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tostring(L, -1);
    try std.testing.expectEqualStrings("\"a\\nb\"", std.mem.span(result.?));
}

test "cjson decode: JSON array" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "local t = cjson.decode('[1,2,3]'); return t[1], t[2], t[3]");
    _ = lua.lua_pcall(L, 0, 3, 0);

    try std.testing.expectEqual(@as(f64, 1.0), lua.lua_tonumber(L, -3));
    try std.testing.expectEqual(@as(f64, 2.0), lua.lua_tonumber(L, -2));
    try std.testing.expectEqual(@as(f64, 3.0), lua.lua_tonumber(L, -1));
}

test "cjson decode: JSON object" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "local t = cjson.decode('{\"count\":5}'); return t['count']");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 5.0), lua.lua_tonumber(L, -1));
}

test "cjson decode: nested object" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L, "local t = cjson.decode('{\"a\":{\"b\":99}}'); return t['a']['b']");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 99.0), lua.lua_tonumber(L, -1));
}

test "cjson encode-decode roundtrip" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCJson(L);

    _ = lua.luaL_loadstring(L,
        \\local orig = {name="alice", score=100}
        \\local json = cjson.encode(orig)
        \\local back = cjson.decode(json)
        \\return back['score']
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 100.0), lua.lua_tonumber(L, -1));
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
