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
    registerTableUnpack(L);
}

/// Add table.unpack = unpack for Lua 5.2+ compatibility.
/// LuaJIT/Lua 5.1 provides the global `unpack`, but modern code uses `table.unpack`.
/// Redis scripts expect table.unpack to be available (Redis 7.x Lua compat layer).
fn registerTableUnpack(L: *lua.lua_State) void {
    // Push the `table` global table onto the stack via _G["table"]
    lua.lua_getfield(L, lua.LUA_GLOBALSINDEX, "table");
    if (lua.lua_type(L, -1) != lua.LUA_TTABLE) {
        lua.lua_pop(L, 1);
        return;
    }
    // Push the global `unpack` function via _G["unpack"]
    lua.lua_getfield(L, lua.LUA_GLOBALSINDEX, "unpack");
    if (lua.lua_type(L, -1) == lua.LUA_TFUNCTION) {
        // table["unpack"] = unpack
        lua.lua_setfield(L, -2, "unpack");
    } else {
        lua.lua_pop(L, 1); // pop nil/non-function
    }
    lua.lua_pop(L, 1); // pop table
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

/// Encode a signed 64-bit integer using the smallest applicable MessagePack int type.
fn msgpackEncodeInt(n: i64, buf: *std.ArrayList(u8), alloc: std.mem.Allocator) !void {
    if (n >= 0) {
        const u: u64 = @intCast(n);
        if (u <= 0x7f) {
            try buf.append(alloc, @intCast(u));
        } else if (u <= 0xff) {
            try buf.append(alloc, 0xcc);
            try buf.append(alloc, @intCast(u));
        } else if (u <= 0xffff) {
            try buf.append(alloc, 0xcd);
            var b: [2]u8 = undefined;
            std.mem.writeInt(u16, &b, @intCast(u), .big);
            try buf.appendSlice(alloc, &b);
        } else if (u <= 0xffffffff) {
            try buf.append(alloc, 0xce);
            var b: [4]u8 = undefined;
            std.mem.writeInt(u32, &b, @intCast(u), .big);
            try buf.appendSlice(alloc, &b);
        } else {
            try buf.append(alloc, 0xcf);
            var b: [8]u8 = undefined;
            std.mem.writeInt(u64, &b, u, .big);
            try buf.appendSlice(alloc, &b);
        }
    } else if (n >= -32) {
        try buf.append(alloc, @bitCast(@as(i8, @intCast(n))));
    } else if (n >= -128) {
        try buf.append(alloc, 0xd0);
        try buf.append(alloc, @bitCast(@as(i8, @intCast(n))));
    } else if (n >= -32768) {
        try buf.append(alloc, 0xd1);
        var b: [2]u8 = undefined;
        std.mem.writeInt(i16, &b, @intCast(n), .big);
        try buf.appendSlice(alloc, &b);
    } else if (n >= -2147483648) {
        try buf.append(alloc, 0xd2);
        var b: [4]u8 = undefined;
        std.mem.writeInt(i32, &b, @intCast(n), .big);
        try buf.appendSlice(alloc, &b);
    } else {
        try buf.append(alloc, 0xd3);
        var b: [8]u8 = undefined;
        std.mem.writeInt(i64, &b, n, .big);
        try buf.appendSlice(alloc, &b);
    }
}

/// Encode a Lua number: integral values use the smallest MessagePack int type,
/// non-integral values use float64 (matches lua-cmsgpack's autodetection).
fn msgpackEncodeNumber(num: f64, buf: *std.ArrayList(u8), alloc: std.mem.Allocator) !void {
    const is_int = @floor(num) == num and !std.math.isInf(num) and !std.math.isNan(num) and
        num >= -9223372036854775808.0 and num < 9223372036854775808.0;
    if (is_int) {
        try msgpackEncodeInt(@intFromFloat(num), buf, alloc);
    } else {
        try buf.append(alloc, 0xcb);
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, @bitCast(num), .big);
        try buf.appendSlice(alloc, &b);
    }
}

/// Encode a raw byte string using the smallest applicable MessagePack str type.
fn msgpackEncodeString(s: []const u8, buf: *std.ArrayList(u8), alloc: std.mem.Allocator) !void {
    const len = s.len;
    if (len <= 31) {
        try buf.append(alloc, 0xa0 | @as(u8, @intCast(len)));
    } else if (len <= 0xff) {
        try buf.append(alloc, 0xd9);
        try buf.append(alloc, @intCast(len));
    } else if (len <= 0xffff) {
        try buf.append(alloc, 0xda);
        var b: [2]u8 = undefined;
        std.mem.writeInt(u16, &b, @intCast(len), .big);
        try buf.appendSlice(alloc, &b);
    } else {
        try buf.append(alloc, 0xdb);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @intCast(len), .big);
        try buf.appendSlice(alloc, &b);
    }
    try buf.appendSlice(alloc, s);
}

fn msgpackEncodeArrayHeader(len: usize, buf: *std.ArrayList(u8), alloc: std.mem.Allocator) !void {
    if (len <= 15) {
        try buf.append(alloc, 0x90 | @as(u8, @intCast(len)));
    } else if (len <= 0xffff) {
        try buf.append(alloc, 0xdc);
        var b: [2]u8 = undefined;
        std.mem.writeInt(u16, &b, @intCast(len), .big);
        try buf.appendSlice(alloc, &b);
    } else {
        try buf.append(alloc, 0xdd);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @intCast(len), .big);
        try buf.appendSlice(alloc, &b);
    }
}

fn msgpackEncodeMapHeader(len: usize, buf: *std.ArrayList(u8), alloc: std.mem.Allocator) !void {
    if (len <= 15) {
        try buf.append(alloc, 0x80 | @as(u8, @intCast(len)));
    } else if (len <= 0xffff) {
        try buf.append(alloc, 0xde);
        var b: [2]u8 = undefined;
        std.mem.writeInt(u16, &b, @intCast(len), .big);
        try buf.appendSlice(alloc, &b);
    } else {
        try buf.append(alloc, 0xdf);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @intCast(len), .big);
        try buf.appendSlice(alloc, &b);
    }
}

/// Encode a Lua table at absolute stack index `abs`: arrays (lua_objlen > 0) use
/// MessagePack array format, everything else (including empty tables) uses map format.
fn msgpackEncodeTable(L: *lua.lua_State, abs: c_int, buf: *std.ArrayList(u8), alloc: std.mem.Allocator, depth: u8) anyerror!void {
    const arr_len = lua.lua_objlen(L, abs);
    if (arr_len > 0) {
        try msgpackEncodeArrayHeader(arr_len, buf, alloc);
        var i: c_int = 1;
        while (i <= @as(c_int, @intCast(arr_len))) : (i += 1) {
            lua.lua_rawgeti(L, abs, i);
            try msgpackEncodeValue(L, -1, buf, alloc, depth + 1);
            lua.lua_pop(L, 1);
        }
        return;
    }

    var count: usize = 0;
    lua.lua_pushnil(L);
    while (lua.lua_next(L, abs) != 0) {
        count += 1;
        lua.lua_pop(L, 1);
    }

    try msgpackEncodeMapHeader(count, buf, alloc);
    lua.lua_pushnil(L);
    while (lua.lua_next(L, abs) != 0) {
        try msgpackEncodeValue(L, -2, buf, alloc, depth + 1);
        try msgpackEncodeValue(L, -1, buf, alloc, depth + 1);
        lua.lua_pop(L, 1); // pop value, keep key for next()
    }
}

/// Recursively encode a Lua value at stack index idx as MessagePack binary into buf.
fn msgpackEncodeValue(L: *lua.lua_State, idx: c_int, buf: *std.ArrayList(u8), alloc: std.mem.Allocator, depth: u8) anyerror!void {
    if (depth > 64) {
        try buf.append(alloc, 0xc0); // nil: excessive nesting collapses to nil, mirrors cjson
        return;
    }
    const vtype = lua.lua_type(L, idx);
    switch (vtype) {
        lua.LUA_TNIL => try buf.append(alloc, 0xc0),
        lua.LUA_TBOOLEAN => try buf.append(alloc, if (lua.lua_toboolean(L, idx) != 0) @as(u8, 0xc3) else @as(u8, 0xc2)),
        lua.LUA_TNUMBER => try msgpackEncodeNumber(lua.lua_tonumber(L, idx), buf, alloc),
        lua.LUA_TSTRING => {
            var slen: usize = 0;
            const sp = lua.lua_tolstring(L, idx, &slen);
            const s = if (sp) |p| p[0..slen] else "";
            try msgpackEncodeString(s, buf, alloc);
        },
        lua.LUA_TTABLE => {
            const abs: c_int = if (idx < 0) lua.lua_gettop(L) + idx + 1 else idx;
            try msgpackEncodeTable(L, abs, buf, alloc, depth);
        },
        else => try buf.append(alloc, 0xc0), // functions/userdata/threads have no MessagePack representation
    }
}

/// cmsgpack.pack(v1, v2, ...) - encode each argument to MessagePack binary and concatenate.
/// Redis uses lua-cmsgpack, which supports packing multiple values into one buffer.
export fn cmsgpackPack(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;
    const nargs = lua.lua_gettop(state);

    if (nargs < 1) {
        lua.lua_pushstring(state, "expected at least 1 argument");
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

    var i: c_int = 1;
    while (i <= nargs) : (i += 1) {
        msgpackEncodeValue(state, i, &buf, alloc, 0) catch {
            lua.lua_pushstring(state, "ERR cmsgpack encode failed");
            _ = lua.lua_error(state);
            return 0;
        };
    }

    lua.lua_pushlstring(state, buf.items.ptr, buf.items.len);
    return 1;
}

const MsgpackError = error{ UnexpectedEnd, InvalidFormat, TooDeep };

fn msgpackReadBytes(data: []const u8, pos: usize, n: usize) MsgpackError![]const u8 {
    if (pos + n > data.len) return MsgpackError.UnexpectedEnd;
    return data[pos .. pos + n];
}

fn msgpackDecodeArray(L: *lua.lua_State, data: []const u8, offset: usize, len: usize, depth: u8) MsgpackError!usize {
    lua.lua_createtable(L, @intCast(len), 0);
    var pos = offset;
    var i: usize = 1;
    while (i <= len) : (i += 1) {
        pos = try msgpackDecodeValue(L, data, pos, depth + 1);
        lua.lua_rawseti(L, -2, @intCast(i));
    }
    return pos;
}

fn msgpackDecodeMap(L: *lua.lua_State, data: []const u8, offset: usize, len: usize, depth: u8) MsgpackError!usize {
    lua.lua_createtable(L, 0, @intCast(len));
    var pos = offset;
    var i: usize = 0;
    while (i < len) : (i += 1) {
        pos = try msgpackDecodeValue(L, data, pos, depth + 1); // key
        pos = try msgpackDecodeValue(L, data, pos, depth + 1); // value
        lua.lua_rawset(L, -3);
    }
    return pos;
}

/// Decode one MessagePack value starting at data[offset], pushing it onto the Lua
/// stack. Returns the offset just past the decoded value.
fn msgpackDecodeValue(L: *lua.lua_State, data: []const u8, offset: usize, depth: u8) MsgpackError!usize {
    if (depth > 64) return MsgpackError.TooDeep;
    if (offset >= data.len) return MsgpackError.UnexpectedEnd;
    const b0 = data[offset];
    var pos = offset + 1;

    switch (b0) {
        0x00...0x7f => lua.lua_pushnumber(L, @floatFromInt(b0)),
        0xe0...0xff => lua.lua_pushnumber(L, @floatFromInt(@as(i8, @bitCast(b0)))),
        0xc0 => lua.lua_pushnil(L),
        0xc2 => lua.lua_pushboolean(L, 0),
        0xc3 => lua.lua_pushboolean(L, 1),
        0xcc => {
            const b = try msgpackReadBytes(data, pos, 1);
            lua.lua_pushnumber(L, @floatFromInt(b[0]));
            pos += 1;
        },
        0xcd => {
            const b = try msgpackReadBytes(data, pos, 2);
            lua.lua_pushnumber(L, @floatFromInt(std.mem.readInt(u16, b[0..2], .big)));
            pos += 2;
        },
        0xce => {
            const b = try msgpackReadBytes(data, pos, 4);
            lua.lua_pushnumber(L, @floatFromInt(std.mem.readInt(u32, b[0..4], .big)));
            pos += 4;
        },
        0xcf => {
            const b = try msgpackReadBytes(data, pos, 8);
            lua.lua_pushnumber(L, @floatFromInt(std.mem.readInt(u64, b[0..8], .big)));
            pos += 8;
        },
        0xd0 => {
            const b = try msgpackReadBytes(data, pos, 1);
            lua.lua_pushnumber(L, @floatFromInt(@as(i8, @bitCast(b[0]))));
            pos += 1;
        },
        0xd1 => {
            const b = try msgpackReadBytes(data, pos, 2);
            lua.lua_pushnumber(L, @floatFromInt(std.mem.readInt(i16, b[0..2], .big)));
            pos += 2;
        },
        0xd2 => {
            const b = try msgpackReadBytes(data, pos, 4);
            lua.lua_pushnumber(L, @floatFromInt(std.mem.readInt(i32, b[0..4], .big)));
            pos += 4;
        },
        0xd3 => {
            const b = try msgpackReadBytes(data, pos, 8);
            lua.lua_pushnumber(L, @floatFromInt(std.mem.readInt(i64, b[0..8], .big)));
            pos += 8;
        },
        0xca => {
            const b = try msgpackReadBytes(data, pos, 4);
            const bits = std.mem.readInt(u32, b[0..4], .big);
            lua.lua_pushnumber(L, @as(f32, @bitCast(bits)));
            pos += 4;
        },
        0xcb => {
            const b = try msgpackReadBytes(data, pos, 8);
            const bits = std.mem.readInt(u64, b[0..8], .big);
            lua.lua_pushnumber(L, @as(f64, @bitCast(bits)));
            pos += 8;
        },
        0xa0...0xbf => {
            const len: usize = b0 & 0x1f;
            const s = try msgpackReadBytes(data, pos, len);
            lua.lua_pushlstring(L, s.ptr, s.len);
            pos += len;
        },
        0xc4, 0xd9 => {
            const lb = try msgpackReadBytes(data, pos, 1);
            const len: usize = lb[0];
            pos += 1;
            const s = try msgpackReadBytes(data, pos, len);
            lua.lua_pushlstring(L, s.ptr, s.len);
            pos += len;
        },
        0xc5, 0xda => {
            const lb = try msgpackReadBytes(data, pos, 2);
            const len: usize = std.mem.readInt(u16, lb[0..2], .big);
            pos += 2;
            const s = try msgpackReadBytes(data, pos, len);
            lua.lua_pushlstring(L, s.ptr, s.len);
            pos += len;
        },
        0xc6, 0xdb => {
            const lb = try msgpackReadBytes(data, pos, 4);
            const len: usize = std.mem.readInt(u32, lb[0..4], .big);
            pos += 4;
            const s = try msgpackReadBytes(data, pos, len);
            lua.lua_pushlstring(L, s.ptr, s.len);
            pos += len;
        },
        0x90...0x9f => pos = try msgpackDecodeArray(L, data, pos, b0 & 0x0f, depth),
        0xdc => {
            const lb = try msgpackReadBytes(data, pos, 2);
            const len: usize = std.mem.readInt(u16, lb[0..2], .big);
            pos = try msgpackDecodeArray(L, data, pos + 2, len, depth);
        },
        0xdd => {
            const lb = try msgpackReadBytes(data, pos, 4);
            const len: usize = std.mem.readInt(u32, lb[0..4], .big);
            pos = try msgpackDecodeArray(L, data, pos + 4, len, depth);
        },
        0x80...0x8f => pos = try msgpackDecodeMap(L, data, pos, b0 & 0x0f, depth),
        0xde => {
            const lb = try msgpackReadBytes(data, pos, 2);
            const len: usize = std.mem.readInt(u16, lb[0..2], .big);
            pos = try msgpackDecodeMap(L, data, pos + 2, len, depth);
        },
        0xdf => {
            const lb = try msgpackReadBytes(data, pos, 4);
            const len: usize = std.mem.readInt(u32, lb[0..4], .big);
            pos = try msgpackDecodeMap(L, data, pos + 4, len, depth);
        },
        else => return MsgpackError.InvalidFormat, // 0xc1 (unused) and ext types (unsupported)
    }
    return pos;
}

/// cmsgpack.unpack(binary) - decode every MessagePack value in binary, returning
/// them as multiple values (matches lua-cmsgpack: pack(1,2,3) round-trips via
/// unpack into three return values).
export fn cmsgpackUnpack(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;

    var data_len: usize = 0;
    const data_ptr = lua.lua_tolstring(state, 1, &data_len);
    if (data_ptr == null or data_len == 0) {
        lua.lua_pushnil(state);
        return 1;
    }
    const data = data_ptr.?[0..data_len];

    var pos: usize = 0;
    var count: c_int = 0;
    while (pos < data.len) {
        pos = msgpackDecodeValue(state, data, pos, 0) catch {
            lua.lua_pushstring(state, "ERR Missing bytes in input.");
            _ = lua.lua_error(state);
            return 0;
        };
        count += 1;
    }
    return count;
}

/// Register struct library - binary data packing/unpacking
/// Redis uses lua-struct 0.2 - we provide a compatible API
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

const STRUCT_MAX_PACK = 4096;

// Safe f64 → i64 conversion (clamps instead of panicking on out-of-range)
fn f64ToI64Safe(n: f64) i64 {
    if (std.math.isNan(n) or n >= @as(f64, @floatFromInt(std.math.maxInt(i64)))) return std.math.maxInt(i64);
    if (n <= @as(f64, @floatFromInt(std.math.minInt(i64)))) return std.math.minInt(i64);
    return @intFromFloat(@trunc(n));
}

// Compute the byte size of each format specifier (advances fi past any digit suffix)
fn structSpecSize(fmt: []const u8, fi: *usize) usize {
    const fc = fmt[fi.*];
    switch (fc) {
        'b', 'B', 'x' => return 1,
        'h', 'H' => return 2,
        'i', 'I', 'l', 'L', 'f' => return 4,
        'd', 'q', 'Q' => return 8,
        'c' => {
            fi.* += 1;
            var count: usize = 0;
            while (fi.* < fmt.len and std.ascii.isDigit(fmt[fi.*])) : (fi.* += 1) {
                count = count * 10 + (fmt[fi.*] - '0');
            }
            fi.* -= 1;
            return count;
        },
        else => return 0,
    }
}

/// struct.pack(format, ...) - pack values into binary string.
/// Supported: >, <, =, b, B, h, H, i, I, l, L, f, d, q, Q, x, cN, z
export fn structPack(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 1) {
        lua.lua_pushstring(state, "bad argument #1 to 'pack' (string expected)");
        _ = lua.lua_error(state);
        return 0;
    }

    var fmt_len: usize = 0;
    const fmt_ptr = lua.lua_tolstring(state, 1, &fmt_len) orelse {
        lua.lua_pushstring(state, "bad argument #1 to 'pack' (string expected)");
        _ = lua.lua_error(state);
        return 0;
    };
    const fmt = fmt_ptr[0..fmt_len];

    var buf: [STRUCT_MAX_PACK]u8 = undefined;
    var pos: usize = 0;
    var arg_idx: c_int = 2;
    var big_endian = false;

    var fi: usize = 0;
    while (fi < fmt.len) : (fi += 1) {
        const fc = fmt[fi];
        switch (fc) {
            '>' => big_endian = true,
            '<' => big_endian = false,
            '=' => big_endian = false, // native (x86 = little)
            '!' => big_endian = true,
            'b' => {
                if (pos + 1 > STRUCT_MAX_PACK) break;
                const n: i8 = @truncate(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)));
                buf[pos] = @bitCast(n);
                pos += 1;
                arg_idx += 1;
            },
            'B' => {
                if (pos + 1 > STRUCT_MAX_PACK) break;
                const n: u8 = @truncate(@as(u64, @bitCast(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)))));
                buf[pos] = n;
                pos += 1;
                arg_idx += 1;
            },
            'h' => {
                if (pos + 2 > STRUCT_MAX_PACK) break;
                const n: i16 = @truncate(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)));
                std.mem.writeInt(i16, buf[pos..][0..2], n, if (big_endian) .big else .little);
                pos += 2;
                arg_idx += 1;
            },
            'H' => {
                if (pos + 2 > STRUCT_MAX_PACK) break;
                const n: u16 = @truncate(@as(u64, @bitCast(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)))));
                std.mem.writeInt(u16, buf[pos..][0..2], n, if (big_endian) .big else .little);
                pos += 2;
                arg_idx += 1;
            },
            'i', 'l' => {
                if (pos + 4 > STRUCT_MAX_PACK) break;
                const n: i32 = @truncate(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)));
                std.mem.writeInt(i32, buf[pos..][0..4], n, if (big_endian) .big else .little);
                pos += 4;
                arg_idx += 1;
            },
            'I', 'L' => {
                if (pos + 4 > STRUCT_MAX_PACK) break;
                const n: u32 = @truncate(@as(u64, @bitCast(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)))));
                std.mem.writeInt(u32, buf[pos..][0..4], n, if (big_endian) .big else .little);
                pos += 4;
                arg_idx += 1;
            },
            'f' => {
                if (pos + 4 > STRUCT_MAX_PACK) break;
                const n: f32 = @floatCast(lua.lua_tonumber(state, arg_idx));
                std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(n), if (big_endian) .big else .little);
                pos += 4;
                arg_idx += 1;
            },
            'd' => {
                if (pos + 8 > STRUCT_MAX_PACK) break;
                const n: f64 = lua.lua_tonumber(state, arg_idx);
                std.mem.writeInt(u64, buf[pos..][0..8], @bitCast(n), if (big_endian) .big else .little);
                pos += 8;
                arg_idx += 1;
            },
            'q' => {
                if (pos + 8 > STRUCT_MAX_PACK) break;
                const n: i64 = f64ToI64Safe(lua.lua_tonumber(state, arg_idx));
                std.mem.writeInt(i64, buf[pos..][0..8], n, if (big_endian) .big else .little);
                pos += 8;
                arg_idx += 1;
            },
            'Q' => {
                if (pos + 8 > STRUCT_MAX_PACK) break;
                const n: u64 = @bitCast(f64ToI64Safe(lua.lua_tonumber(state, arg_idx)));
                std.mem.writeInt(u64, buf[pos..][0..8], n, if (big_endian) .big else .little);
                pos += 8;
                arg_idx += 1;
            },
            'x' => {
                if (pos + 1 > STRUCT_MAX_PACK) break;
                buf[pos] = 0;
                pos += 1;
            },
            'c' => {
                fi += 1;
                var count: usize = 0;
                while (fi < fmt.len and std.ascii.isDigit(fmt[fi])) : (fi += 1) {
                    count = count * 10 + (fmt[fi] - '0');
                }
                fi -= 1;
                if (pos + count > STRUCT_MAX_PACK) break;
                var str_len: usize = 0;
                const str_ptr = lua.lua_tolstring(state, arg_idx, &str_len);
                if (str_ptr != null) {
                    const copy_len = @min(count, str_len);
                    @memcpy(buf[pos .. pos + copy_len], str_ptr.?[0..copy_len]);
                    if (copy_len < count) @memset(buf[pos + copy_len .. pos + count], 0);
                } else {
                    @memset(buf[pos .. pos + count], 0);
                }
                pos += count;
                arg_idx += 1;
            },
            'z' => {
                var str_len: usize = 0;
                const str_ptr = lua.lua_tolstring(state, arg_idx, &str_len);
                if (str_ptr != null and pos + str_len + 1 <= STRUCT_MAX_PACK) {
                    @memcpy(buf[pos .. pos + str_len], str_ptr.?[0..str_len]);
                    pos += str_len;
                }
                if (pos + 1 <= STRUCT_MAX_PACK) {
                    buf[pos] = 0;
                    pos += 1;
                }
                arg_idx += 1;
            },
            's' => {
                // 1-byte length-prefixed string
                var str_len: usize = 0;
                const str_ptr = lua.lua_tolstring(state, arg_idx, &str_len);
                const slen: u8 = @truncate(str_len);
                if (pos + 1 + slen <= STRUCT_MAX_PACK) {
                    buf[pos] = slen;
                    pos += 1;
                    if (str_ptr != null) {
                        @memcpy(buf[pos .. pos + slen], str_ptr.?[0..slen]);
                        pos += slen;
                    }
                }
                arg_idx += 1;
            },
            else => {},
        }
    }

    lua.lua_pushlstring(state, &buf, pos);
    return 1;
}

/// struct.unpack(format, binary [, init]) - unpack binary string.
/// Returns unpacked values followed by the next read position (1-indexed).
export fn structUnpack(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;

    if (lua.lua_gettop(state) < 2) {
        lua.lua_pushstring(state, "bad argument to 'unpack'");
        _ = lua.lua_error(state);
        return 0;
    }

    var fmt_len: usize = 0;
    const fmt_ptr = lua.lua_tolstring(state, 1, &fmt_len) orelse {
        lua.lua_pushstring(state, "bad argument #1 to 'unpack' (string expected)");
        _ = lua.lua_error(state);
        return 0;
    };
    const fmt = fmt_ptr[0..fmt_len];

    var data_len: usize = 0;
    const data_ptr = lua.lua_tolstring(state, 2, &data_len) orelse {
        lua.lua_pushstring(state, "bad argument #2 to 'unpack' (string expected)");
        _ = lua.lua_error(state);
        return 0;
    };
    const data = data_ptr[0..data_len];

    // Optional starting position (1-indexed Lua style)
    var data_pos: usize = 0;
    if (lua.lua_gettop(state) >= 3) {
        const start = lua.lua_tointeger(state, 3);
        if (start > 1) data_pos = @intCast(start - 1);
    }

    var return_count: c_int = 0;
    var big_endian = false;

    var fi: usize = 0;
    while (fi < fmt.len) : (fi += 1) {
        const fc = fmt[fi];
        switch (fc) {
            '>' => big_endian = true,
            '<' => big_endian = false,
            '=' => big_endian = false,
            '!' => big_endian = true,
            'b' => {
                if (data_pos + 1 > data.len) break;
                const n: i8 = @bitCast(data[data_pos]);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 1;
                return_count += 1;
            },
            'B' => {
                if (data_pos + 1 > data.len) break;
                lua.lua_pushnumber(state, @floatFromInt(data[data_pos]));
                data_pos += 1;
                return_count += 1;
            },
            'h' => {
                if (data_pos + 2 > data.len) break;
                const n = std.mem.readInt(i16, data[data_pos..][0..2], if (big_endian) .big else .little);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 2;
                return_count += 1;
            },
            'H' => {
                if (data_pos + 2 > data.len) break;
                const n = std.mem.readInt(u16, data[data_pos..][0..2], if (big_endian) .big else .little);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 2;
                return_count += 1;
            },
            'i', 'l' => {
                if (data_pos + 4 > data.len) break;
                const n = std.mem.readInt(i32, data[data_pos..][0..4], if (big_endian) .big else .little);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 4;
                return_count += 1;
            },
            'I', 'L' => {
                if (data_pos + 4 > data.len) break;
                const n = std.mem.readInt(u32, data[data_pos..][0..4], if (big_endian) .big else .little);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 4;
                return_count += 1;
            },
            'f' => {
                if (data_pos + 4 > data.len) break;
                const bits = std.mem.readInt(u32, data[data_pos..][0..4], if (big_endian) .big else .little);
                const n: f32 = @bitCast(bits);
                lua.lua_pushnumber(state, n);
                data_pos += 4;
                return_count += 1;
            },
            'd' => {
                if (data_pos + 8 > data.len) break;
                const bits = std.mem.readInt(u64, data[data_pos..][0..8], if (big_endian) .big else .little);
                const n: f64 = @bitCast(bits);
                lua.lua_pushnumber(state, n);
                data_pos += 8;
                return_count += 1;
            },
            'q' => {
                if (data_pos + 8 > data.len) break;
                const n = std.mem.readInt(i64, data[data_pos..][0..8], if (big_endian) .big else .little);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 8;
                return_count += 1;
            },
            'Q' => {
                if (data_pos + 8 > data.len) break;
                const n = std.mem.readInt(u64, data[data_pos..][0..8], if (big_endian) .big else .little);
                lua.lua_pushnumber(state, @floatFromInt(n));
                data_pos += 8;
                return_count += 1;
            },
            'x' => {
                data_pos += 1;
            },
            'c' => {
                fi += 1;
                var count: usize = 0;
                while (fi < fmt.len and std.ascii.isDigit(fmt[fi])) : (fi += 1) {
                    count = count * 10 + (fmt[fi] - '0');
                }
                fi -= 1;
                if (data_pos + count > data.len) break;
                lua.lua_pushlstring(state, data[data_pos..].ptr, count);
                data_pos += count;
                return_count += 1;
            },
            'z' => {
                // null-terminated string
                const start = data_pos;
                while (data_pos < data.len and data[data_pos] != 0) : (data_pos += 1) {}
                lua.lua_pushlstring(state, data[start..].ptr, data_pos - start);
                if (data_pos < data.len) data_pos += 1; // skip the null
                return_count += 1;
            },
            's' => {
                // 1-byte length-prefixed string
                if (data_pos + 1 > data.len) break;
                const slen: usize = data[data_pos];
                data_pos += 1;
                if (data_pos + slen > data.len) break;
                lua.lua_pushlstring(state, data[data_pos..].ptr, slen);
                data_pos += slen;
                return_count += 1;
            },
            'A' => {
                // remaining bytes
                lua.lua_pushlstring(state, data[data_pos..].ptr, data.len - data_pos);
                data_pos = data.len;
                return_count += 1;
            },
            else => {},
        }
    }

    // Return next position (1-indexed)
    lua.lua_pushnumber(state, @floatFromInt(data_pos + 1));
    return_count += 1;

    return return_count;
}

/// struct.size(format) - calculate the number of bytes required for struct.pack.
export fn structSize(L: ?*lua.lua_State) callconv(.c) c_int {
    const state = L orelse return 0;

    var fmt_len: usize = 0;
    const fmt_ptr = lua.lua_tolstring(state, 1, &fmt_len) orelse {
        lua.lua_pushnumber(state, 0);
        return 1;
    };
    const fmt = fmt_ptr[0..fmt_len];

    var total: usize = 0;
    var fi: usize = 0;
    while (fi < fmt.len) : (fi += 1) {
        const fc = fmt[fi];
        switch (fc) {
            '>', '<', '=', '!' => {},
            'b', 'B', 'x' => total += 1,
            'h', 'H' => total += 2,
            'i', 'I', 'l', 'L', 'f' => total += 4,
            'd', 'q', 'Q' => total += 8,
            'c' => {
                fi += 1;
                var count: usize = 0;
                while (fi < fmt.len and std.ascii.isDigit(fmt[fi])) : (fi += 1) {
                    count = count * 10 + (fmt[fi] - '0');
                }
                fi -= 1;
                total += count;
            },
            else => {},
        }
    }

    lua.lua_pushnumber(state, @floatFromInt(total));
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

test "cmsgpack pack: positive fixint" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack(42)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x2a}, s.?[0..len]);
}

test "cmsgpack pack: negative fixint" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack(-1)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualSlices(u8, &[_]u8{0xff}, s.?[0..len]);
}

test "cmsgpack pack: nil, true, false" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack(nil, true, false)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xc0, 0xc3, 0xc2 }, s.?[0..len]);
}

test "cmsgpack pack: fixstr" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack('hi')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xa2, 'h', 'i' }, s.?[0..len]);
}

test "cmsgpack pack: fixarray" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack({1, 2, 3})");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x93, 0x01, 0x02, 0x03 }, s.?[0..len]);
}

test "cmsgpack pack: uint8 boundary (200)" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.pack(200)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xcc, 200 }, s.?[0..len]);
}

test "cmsgpack unpack: scalar roundtrip" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.unpack(cmsgpack.pack(42))");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 42.0), lua.lua_tonumber(L, -1));
}

test "cmsgpack unpack: string roundtrip" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.unpack(cmsgpack.pack('hello world'))");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualStrings("hello world", s.?[0..len]);
}

test "cmsgpack unpack: negative float roundtrip" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.unpack(cmsgpack.pack(-3.5))");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, -3.5), lua.lua_tonumber(L, -1));
}

test "cmsgpack unpack: array roundtrip preserves order" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L,
        \\local t = cmsgpack.unpack(cmsgpack.pack({10, 20, 30}))
        \\return t[1], t[2], t[3], #t
    );
    _ = lua.lua_pcall(L, 0, 4, 0);

    try std.testing.expectEqual(@as(f64, 10.0), lua.lua_tonumber(L, -4));
    try std.testing.expectEqual(@as(f64, 20.0), lua.lua_tonumber(L, -3));
    try std.testing.expectEqual(@as(f64, 30.0), lua.lua_tonumber(L, -2));
    try std.testing.expectEqual(@as(f64, 3.0), lua.lua_tonumber(L, -1));
}

test "cmsgpack unpack: map roundtrip" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L,
        \\local t = cmsgpack.unpack(cmsgpack.pack({foo = 'bar'}))
        \\return t.foo
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const s = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(s != null);
    try std.testing.expectEqualStrings("bar", s.?[0..len]);
}

test "cmsgpack pack/unpack: multiple values round-trip as multiple returns" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    _ = lua.luaL_loadstring(L, "return cmsgpack.unpack(cmsgpack.pack(1, 2, 3))");
    _ = lua.lua_pcall(L, 0, 3, 0);

    try std.testing.expectEqual(@as(f64, 1.0), lua.lua_tonumber(L, -3));
    try std.testing.expectEqual(@as(f64, 2.0), lua.lua_tonumber(L, -2));
    try std.testing.expectEqual(@as(f64, 3.0), lua.lua_tonumber(L, -1));
}

test "cmsgpack unpack: truncated input raises an error" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerCMsgPack(L);

    // 0xd2 (int32) header with no payload bytes
    _ = lua.luaL_loadstring(L, "return pcall(cmsgpack.unpack, string.char(0xd2))");
    _ = lua.lua_pcall(L, 0, 2, 0);

    // pcall returns (false, err) on failure
    try std.testing.expectEqual(@as(c_int, 0), lua.lua_toboolean(L, -2));
}

test "struct pack: i (little-endian 4-byte int)" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L, "return struct.pack('<i', 1)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const ptr = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(ptr != null);
    try std.testing.expectEqual(@as(usize, 4), len);
    const bytes = ptr.?[0..len];
    // 1 in little-endian = 0x01 0x00 0x00 0x00
    try std.testing.expectEqual(@as(u8, 1), bytes[0]);
    try std.testing.expectEqual(@as(u8, 0), bytes[1]);
    try std.testing.expectEqual(@as(u8, 0), bytes[2]);
    try std.testing.expectEqual(@as(u8, 0), bytes[3]);
}

test "struct pack: >i (big-endian 4-byte int)" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L, "return struct.pack('>i', 1)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const ptr = lua.lua_tolstring(L, -1, &len);
    try std.testing.expect(ptr != null);
    try std.testing.expectEqual(@as(usize, 4), len);
    const bytes = ptr.?[0..len];
    // 1 in big-endian = 0x00 0x00 0x00 0x01
    try std.testing.expectEqual(@as(u8, 0), bytes[0]);
    try std.testing.expectEqual(@as(u8, 0), bytes[1]);
    try std.testing.expectEqual(@as(u8, 0), bytes[2]);
    try std.testing.expectEqual(@as(u8, 1), bytes[3]);
}

test "struct pack/unpack: double roundtrip" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L,
        \\local s = struct.pack('d', 3.14)
        \\local v, _ = struct.unpack('d', s)
        \\return v
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    const result = lua.lua_tonumber(L, -1);
    try std.testing.expect(@abs(result - 3.14) < 0.0001);
}

test "struct pack/unpack: big-endian short" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L,
        \\local s = struct.pack('>h', 256)
        \\local v, _ = struct.unpack('>h', s)
        \\return v
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 256.0), lua.lua_tonumber(L, -1));
}

test "struct pack/unpack: multiple values" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L,
        \\local s = struct.pack('<bBh', -1, 255, 1000)
        \\local a, b, c, _ = struct.unpack('<bBh', s)
        \\return a, b, c
    );
    _ = lua.lua_pcall(L, 0, 3, 0);

    try std.testing.expectEqual(@as(f64, -1.0), lua.lua_tonumber(L, -3));
    try std.testing.expectEqual(@as(f64, 255.0), lua.lua_tonumber(L, -2));
    try std.testing.expectEqual(@as(f64, 1000.0), lua.lua_tonumber(L, -1));
}

test "struct size: format size calculation" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L, "return struct.size('<bhi d')");
    _ = lua.lua_pcall(L, 0, 1, 0);

    // b=1 + h=2 + i=4 + d=8 = 15
    try std.testing.expectEqual(@as(f64, 15.0), lua.lua_tonumber(L, -1));
}

test "struct pack/unpack: c8 fixed string" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L,
        \\local s = struct.pack('c5', 'hello')
        \\local v, _ = struct.unpack('c5', s)
        \\return v
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const ptr = lua.lua_tolstring(L, -1, &len);
    try std.testing.expectEqual(@as(usize, 5), len);
    try std.testing.expectEqualSlices(u8, "hello", ptr.?[0..5]);
}

test "struct pack/unpack: z (null-terminated string)" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    _ = lua.luaL_loadstring(L,
        \\local s = struct.pack('z', 'world')
        \\local v, _ = struct.unpack('z', s)
        \\return v
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const ptr = lua.lua_tolstring(L, -1, &len);
    try std.testing.expectEqual(@as(usize, 5), len);
    try std.testing.expectEqualSlices(u8, "world", ptr.?[0..5]);
}

test "struct unpack: init position" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);

    try registerStruct(L);

    // Pack two ints, then unpack the second one using init=5
    _ = lua.luaL_loadstring(L,
        \\local s = struct.pack('<ii', 111, 222)
        \\local v, _ = struct.unpack('<i', s, 5)
        \\return v
    );
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 222.0), lua.lua_tonumber(L, -1));
}

// ── table.unpack unit tests ───────────────────────────────────────────────────

test "registerTableUnpack: table.unpack is a function" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);
    lua.luaL_openlibs(L);

    registerTableUnpack(L);

    // table.unpack should be a function
    _ = lua.luaL_loadstring(L, "return type(table.unpack)");
    _ = lua.lua_pcall(L, 0, 1, 0);

    var len: usize = 0;
    const ptr = lua.lua_tolstring(L, -1, &len);
    try std.testing.expectEqualSlices(u8, "function", ptr.?[0..len]);
}

test "registerTableUnpack: table.unpack unpacks array elements" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);
    lua.luaL_openlibs(L);

    registerTableUnpack(L);

    _ = lua.luaL_loadstring(L, "local a,b,c = table.unpack({10,20,30}); return a+b+c");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 60.0), lua.lua_tonumber(L, -1));
}

test "registerTableUnpack: table.unpack with start index" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);
    lua.luaL_openlibs(L);

    registerTableUnpack(L);

    // table.unpack({10,20,30}, 2) skips first element
    _ = lua.luaL_loadstring(L, "local a,b = table.unpack({10,20,30}, 2); return a+b");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expectEqual(@as(f64, 50.0), lua.lua_tonumber(L, -1));
}

test "registerTableUnpack: table.unpack is same as global unpack" {
    const L = lua.luaL_newstate() orelse return error.LuaStateCreateFailed;
    defer lua.lua_close(L);
    lua.luaL_openlibs(L);

    registerTableUnpack(L);

    // table.unpack should be identical to global unpack
    _ = lua.luaL_loadstring(L, "return table.unpack == unpack");
    _ = lua.lua_pcall(L, 0, 1, 0);

    try std.testing.expect(lua.lua_toboolean(L, -1) != 0);
}
