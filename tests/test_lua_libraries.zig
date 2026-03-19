// Integration tests for Lua libraries (cjson, cmsgpack, struct, bit)
// Tests that require() works for allowed libraries and scripts can use them

const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const ScriptStore = @import("../src/storage/scripting.zig").ScriptStore;
const cmdEval = @import("../src/commands/scripting.zig").cmdEval;
const Aof = @import("../src/storage/aof.zig").Aof;
const PubSub = @import("../src/storage/pubsub.zig").PubSub;
const TxState = @import("../src/commands/transactions.zig").TxState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;

test "Lua libraries: cjson is available via require" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Script that uses cjson
    const script =
        \\local cjson = require('cjson')
        \\return 'ok'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should not error on require('cjson')
    try testing.expect(std.mem.indexOf(u8, response, "ERR") == null);
}

test "Lua libraries: cmsgpack is available via require" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local cmsgpack = require('cmsgpack')
        \\return 'ok'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "ERR") == null);
}

test "Lua libraries: struct is available via require" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local struct = require('struct')
        \\return 'ok'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "ERR") == null);
}

test "Lua libraries: bit is available via require (built into LuaJIT)" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local bit = require('bit')
        \\return bit.bor(1, 2)
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should return 3 (1 | 2)
    try testing.expect(std.mem.indexOf(u8, response, "3") != null);
}

test "Lua libraries: cjson.encode works" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local cjson = require('cjson')
        \\return cjson.encode(42)
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should return JSON-encoded 42
    try testing.expect(std.mem.indexOf(u8, response, "42") != null);
}

test "Lua libraries: cjson.decode works" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local cjson = require('cjson')
        \\return cjson.decode('42')
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should return 42 as number
    try testing.expect(std.mem.indexOf(u8, response, "42") != null);
}

test "Lua libraries: cmsgpack.pack works" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local cmsgpack = require('cmsgpack')
        \\local packed = cmsgpack.pack(42)
        \\return 'ok'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should not error
    try testing.expect(std.mem.indexOf(u8, response, "ERR") == null);
}

test "Lua libraries: struct.pack works" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local struct = require('struct')
        \\local packed = struct.pack('i', 42)
        \\return 'ok'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should not error
    try testing.expect(std.mem.indexOf(u8, response, "ERR") == null);
}

test "Lua libraries: disallowed library is blocked" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local forbidden = require('unknown_lib')
        \\return 'fail'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should error
    try testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
    try testing.expect(std.mem.indexOf(u8, response, "require is restricted") != null);
}

test "Lua libraries: math, string, table libraries are allowed" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var pubsub = try PubSub.init(allocator);
    defer pubsub.deinit();

    var tx_state = try TxState.init(allocator);
    defer tx_state.deinit();

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script =
        \\local math = require('math')
        \\local string = require('string')
        \\local table = require('table')
        \\return 'ok'
    ;

    const args = &[_][]const u8{ "0", script, "0" };
    const response = try cmdEval(allocator, &storage, &script_store, &pubsub, &tx_state, &client_registry, 1, args);
    defer allocator.free(response);

    // Should not error
    try testing.expect(std.mem.indexOf(u8, response, "ERR") == null);
}
