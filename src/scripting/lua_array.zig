//! Length of the leading run of non-nil array slots in a Lua table.
//!
//! Redis converts a Lua table reply to a RESP array that stops at the first nil, so a table
//! like `{1, 2, nil, 4}` becomes two elements. The scan is bounded by the table's own
//! `lua_objlen` border, which is never smaller than the first nil index, so the bound never
//! changes the result and a table can no longer drive an open-ended loop.
//!
//! Allocation: none. The Lua stack is left exactly as it was found.

const std = @import("std");
const lua = @import("lua_ffi.zig");
const assert = std.debug.assert;

/// Upper bound on the slots inspected, so the index always fits `lua_rawgeti`'s `c_int`.
pub const sequence_len_max: u32 = std.math.maxInt(i32);

/// Count slots 1, 2, 3, ... of the table at absolute stack index `abs_idx` until the first
/// nil. Precondition: `abs_idx` is positive and holds a table.
pub fn sequence_len(L: *lua.lua_State, abs_idx: c_int) u32 {
    assert(abs_idx > 0);
    assert(lua.lua_type(L, abs_idx) == lua.LUA_TTABLE);

    const top_before = lua.lua_gettop(L);
    defer assert(lua.lua_gettop(L) == top_before);

    const border = lua.lua_objlen(L, abs_idx);
    const scan_max: u32 = @intCast(@min(border, sequence_len_max));

    var count: u32 = 0;
    for (0..scan_max) |slot_index| {
        lua.lua_rawgeti(L, abs_idx, @intCast(slot_index + 1));
        const slot_type = lua.lua_type(L, -1);
        lua.lua_pop(L, 1);
        if (slot_type == lua.LUA_TNIL) break;
        count += 1;
    }
    assert(count <= scan_max);
    return count;
}

fn push_table(L: *lua.lua_State, slots: []const ?f64) void {
    lua.lua_createtable(L, @intCast(slots.len), 0);
    for (slots, 1..) |slot, index| {
        const value = slot orelse continue;
        lua.lua_pushnumber(L, value);
        lua.lua_rawseti(L, -2, @intCast(index));
    }
}

test "sequence_len: empty table has no elements" {
    const L = lua.luaL_newstate() orelse return error.SkipZigTest;
    defer lua.lua_close(L);

    push_table(L, &.{});
    try std.testing.expectEqual(@as(u32, 0), sequence_len(L, 1));
}

test "sequence_len: dense table counts every slot" {
    const L = lua.luaL_newstate() orelse return error.SkipZigTest;
    defer lua.lua_close(L);

    push_table(L, &.{ 10, 20, 30 });
    try std.testing.expectEqual(@as(u32, 3), sequence_len(L, 1));
}

test "sequence_len: stops at the first nil" {
    const L = lua.luaL_newstate() orelse return error.SkipZigTest;
    defer lua.lua_close(L);

    push_table(L, &.{ 1, 2, null, 4 });
    try std.testing.expectEqual(@as(u32, 2), sequence_len(L, 1));
}

test "sequence_len: leading nil gives zero" {
    const L = lua.luaL_newstate() orelse return error.SkipZigTest;
    defer lua.lua_close(L);

    push_table(L, &.{ null, 2, 3 });
    try std.testing.expectEqual(@as(u32, 0), sequence_len(L, 1));
}

test "sequence_len: table with only string keys has no elements" {
    const L = lua.luaL_newstate() orelse return error.SkipZigTest;
    defer lua.lua_close(L);

    lua.lua_createtable(L, 0, 1);
    lua.lua_pushnumber(L, 7);
    lua.lua_setfield(L, -2, "name");
    try std.testing.expectEqual(@as(u32, 0), sequence_len(L, 1));
}

test "sequence_len: large dense table and stack is left unchanged" {
    const L = lua.luaL_newstate() orelse return error.SkipZigTest;
    defer lua.lua_close(L);

    lua.lua_createtable(L, 4096, 0);
    for (1..4097) |index| {
        lua.lua_pushnumber(L, @floatFromInt(index));
        lua.lua_rawseti(L, -2, @intCast(index));
    }
    try std.testing.expectEqual(@as(u32, 4096), sequence_len(L, 1));
    try std.testing.expectEqual(@as(c_int, 1), lua.lua_gettop(L));
}
