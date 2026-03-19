// LuaJIT FFI bindings for Zig
// Provides Lua 5.1 C API wrapper for scripting support

const std = @import("std");

// Opaque Lua state type
pub const lua_State = opaque {};

// Basic Lua constants
pub const LUA_MULTRET: c_int = -1;
pub const LUA_REGISTRYINDEX: c_int = -10000;
pub const LUA_ENVIRONINDEX: c_int = -10001;
pub const LUA_GLOBALSINDEX: c_int = -10002;

// Lua types
pub const LUA_TNONE: c_int = -1;
pub const LUA_TNIL: c_int = 0;
pub const LUA_TBOOLEAN: c_int = 1;
pub const LUA_TLIGHTUSERDATA: c_int = 2;
pub const LUA_TNUMBER: c_int = 3;
pub const LUA_TSTRING: c_int = 4;
pub const LUA_TTABLE: c_int = 5;
pub const LUA_TFUNCTION: c_int = 6;
pub const LUA_TUSERDATA: c_int = 7;
pub const LUA_TTHREAD: c_int = 8;

// Lua status codes
pub const LUA_OK: c_int = 0;
pub const LUA_YIELD: c_int = 1;
pub const LUA_ERRRUN: c_int = 2;
pub const LUA_ERRSYNTAX: c_int = 3;
pub const LUA_ERRMEM: c_int = 4;
pub const LUA_ERRERR: c_int = 5;

// LuaJIT-specific function (creates a new Lua state)
pub extern "c" fn luaL_newstate() ?*lua_State;

// Lua state manipulation
pub extern "c" fn lua_close(L: *lua_State) void;

// Basic stack manipulation
pub extern "c" fn lua_gettop(L: *lua_State) c_int;
pub extern "c" fn lua_settop(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_pushvalue(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_remove(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_insert(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_replace(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_checkstack(L: *lua_State, sz: c_int) c_int;

// Access functions (stack -> C)
pub extern "c" fn lua_isnumber(L: *lua_State, idx: c_int) c_int;
pub extern "c" fn lua_isstring(L: *lua_State, idx: c_int) c_int;
pub extern "c" fn lua_iscfunction(L: *lua_State, idx: c_int) c_int;
pub extern "c" fn lua_isuserdata(L: *lua_State, idx: c_int) c_int;
pub extern "c" fn lua_type(L: *lua_State, idx: c_int) c_int;
pub extern "c" fn lua_typename(L: *lua_State, tp: c_int) [*:0]const u8;

pub extern "c" fn lua_tonumber(L: *lua_State, idx: c_int) f64;
pub extern "c" fn lua_tointeger(L: *lua_State, idx: c_int) i32;
pub extern "c" fn lua_toboolean(L: *lua_State, idx: c_int) c_int;
pub extern "c" fn lua_tolstring(L: *lua_State, idx: c_int, len: ?*usize) ?[*:0]const u8;

// Push functions (C -> stack)
pub extern "c" fn lua_pushnil(L: *lua_State) void;
pub extern "c" fn lua_pushnumber(L: *lua_State, n: f64) void;
pub extern "c" fn lua_pushinteger(L: *lua_State, n: i32) void;
pub extern "c" fn lua_pushlstring(L: *lua_State, s: [*]const u8, len: usize) void;
pub extern "c" fn lua_pushstring(L: *lua_State, s: [*:0]const u8) void;
pub extern "c" fn lua_pushboolean(L: *lua_State, b: c_int) void;
pub extern "c" fn lua_pushcclosure(L: *lua_State, f: *const fn (*lua_State) callconv(.c) c_int, n: c_int) void;
pub extern "c" fn lua_pushlightuserdata(L: *lua_State, p: ?*anyopaque) void;
pub extern "c" fn lua_touserdata(L: *lua_State, idx: c_int) ?*anyopaque;

// Get functions (Lua -> stack)
pub extern "c" fn lua_getfield(L: *lua_State, idx: c_int, k: [*:0]const u8) void;
pub extern "c" fn lua_setfield(L: *lua_State, idx: c_int, k: [*:0]const u8) void;
pub extern "c" fn lua_gettable(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_settable(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_rawget(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_rawgeti(L: *lua_State, idx: c_int, n: c_int) void;
pub extern "c" fn lua_rawset(L: *lua_State, idx: c_int) void;
pub extern "c" fn lua_rawseti(L: *lua_State, idx: c_int, n: c_int) void;

// Table manipulation
pub extern "c" fn lua_createtable(L: *lua_State, narr: c_int, nrec: c_int) void;

// lua_newtable is a macro: #define lua_newtable(L) lua_createtable(L, 0, 0)
pub inline fn lua_newtable(L: *lua_State) void {
    lua_createtable(L, 0, 0);
}

// Auxiliary library functions
pub extern "c" fn luaL_openlibs(L: *lua_State) void;
pub extern "c" fn luaL_loadstring(L: *lua_State, s: [*:0]const u8) c_int;
pub extern "c" fn luaL_loadbuffer(L: *lua_State, buff: [*]const u8, sz: usize, name: [*:0]const u8) c_int;

// Call/execute
pub extern "c" fn lua_pcall(L: *lua_State, nargs: c_int, nresults: c_int, errfunc: c_int) c_int;
pub extern "c" fn lua_call(L: *lua_State, nargs: c_int, nresults: c_int) void;

// Error handling
pub extern "c" fn lua_error(L: *lua_State) c_int;

// Debug hooks for timeout support
pub const lua_Hook = *const fn (L: *lua_State, ar: *lua_Debug) callconv(.c) void;
pub const lua_Debug = opaque {};

// Hook event masks
pub const LUA_MASKCALL: c_int = 1 << 0;
pub const LUA_MASKRET: c_int = 1 << 1;
pub const LUA_MASKLINE: c_int = 1 << 2;
pub const LUA_MASKCOUNT: c_int = 1 << 3;

pub extern "c" fn lua_sethook(L: *lua_State, func: ?lua_Hook, mask: c_int, count: c_int) c_int;

// Helpers
pub inline fn lua_pop(L: *lua_State, n: c_int) void {
    lua_settop(L, -n - 1);
}

pub inline fn lua_register(L: *lua_State, name: [*:0]const u8, f: *const fn (*lua_State) callconv(.c) c_int) void {
    lua_pushcclosure(L, f, 0);
    lua_setfield(L, LUA_GLOBALSINDEX, name);
}

pub inline fn lua_isfunction(L: *lua_State, idx: c_int) bool {
    return lua_type(L, idx) == LUA_TFUNCTION;
}

pub inline fn lua_istable(L: *lua_State, idx: c_int) bool {
    return lua_type(L, idx) == LUA_TTABLE;
}

pub inline fn lua_isnil(L: *lua_State, idx: c_int) bool {
    return lua_type(L, idx) == LUA_TNIL;
}

pub inline fn lua_isboolean(L: *lua_State, idx: c_int) bool {
    return lua_type(L, idx) == LUA_TBOOLEAN;
}

pub inline fn lua_tostring(L: *lua_State, idx: c_int) ?[*:0]const u8 {
    return lua_tolstring(L, idx, null);
}

pub inline fn lua_pushcfunction(L: *lua_State, f: *const fn (*lua_State) callconv(.c) c_int) void {
    lua_pushcclosure(L, f, 0);
}
