// Iteration 394: RESP3 map/set types for FUNCTION LIST and FUNCTION STATS
//
// Redis protocol behavior:
// - FUNCTION LIST: RESP2 returns array of flat arrays (key-value pairs per library)
//   RESP3 returns array of maps; each function sub-entry is a map; flags is a set (~0)
// - FUNCTION STATS: RESP2 returns flat array with null bulk string for no-running-script
//   RESP3 returns map with boolean false (#f) for no-running-script; engines is a map of maps
//
// Affected commands: FUNCTION LIST, FUNCTION STATS

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const transactions_mod = zoltraak.transactions_commands;

fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| resp_args[i] = .{ .bulk_string = a };
    const cmd = RespValue{ .array = resp_args };
    var tx = transactions_mod.TxState.init(allocator);
    defer tx.deinit();
    var script_store = scripting.ScriptStore.init(allocator);
    defer script_store.deinit();
    var databases = [_]Storage{storage.*};
    return commands.executeCommand(
        allocator,
        storage,
        cmd,
        null,
        ps,
        0,
        &tx,
        null,
        6379,
        null,
        null,
        client_registry,
        client_id,
        &script_store,
        null,
        &databases,
        1,
    );
}

fn setup(allocator: std.mem.Allocator, port_str: []const u8) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient(port_str, 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// Lua library code used for testing
const TEST_LIB = "#!lua name=testlib\nredis.register_function('testfunc', function(keys, args) return args[1] end)";

// ─── FUNCTION LIST RESP2 ─────────────────────────────────────────────────────

test "iter394 - FUNCTION LIST RESP2 returns flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9810");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST" });
    defer allocator.free(resp);

    // RESP2: outer *1\r\n, library entry *8\r\n (flat array of 4 pairs)
    try testing.expect(resp[0] == '*');
    // flags as array (*0\r\n)
    try testing.expect(std.mem.indexOf(u8, resp, "*0\r\n") != null);
    // Must NOT contain map type (%)
    try testing.expect(resp[0] != '%');
    // Must NOT contain set type (~)
    try testing.expect(std.mem.indexOf(u8, resp, "~0\r\n") == null);
    // Must contain library_name
    try testing.expect(std.mem.indexOf(u8, resp, "library_name") != null);
    // Must contain testlib
    try testing.expect(std.mem.indexOf(u8, resp, "testlib") != null);
}

test "iter394 - FUNCTION LIST RESP2 empty returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9811");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST" });
    defer allocator.free(resp);

    // Empty: *0\r\n
    try testing.expectEqualStrings("*0\r\n", resp);
}

// ─── FUNCTION LIST RESP3 ─────────────────────────────────────────────────────

test "iter394 - FUNCTION LIST RESP3 each library is a map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9812");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST" });
    defer allocator.free(resp);

    // Outer: *1\r\n (array of 1 library)
    try testing.expect(resp[0] == '*');
    // Library entry: %3\r\n map
    try testing.expect(std.mem.indexOf(u8, resp, "%3\r\n") != null);
    // flags as RESP3 set (~0\r\n)
    try testing.expect(std.mem.indexOf(u8, resp, "~0\r\n") != null);
    // Must NOT contain old flat array flags (*0\r\n) at the point where flags are
    // Note: there may be *1\r\n for outer or *N for function list — just check set type present
    // Function entry: %3\r\n map (name, description, flags)
    // The response contains two %3\r\n — one for library, one per function
    const map_count = countOccurrences(resp, "%3\r\n");
    try testing.expect(map_count >= 2); // library map + function map
    // library_name present
    try testing.expect(std.mem.indexOf(u8, resp, "library_name") != null);
    // testlib present
    try testing.expect(std.mem.indexOf(u8, resp, "testlib") != null);
}

test "iter394 - FUNCTION LIST RESP3 empty returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9813");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST" });
    defer allocator.free(resp);

    // Empty: *0\r\n (outer array is still array type even in RESP3)
    try testing.expectEqualStrings("*0\r\n", resp);
}

test "iter394 - FUNCTION LIST RESP3 WITHCODE includes library_code" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9814");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST", "WITHCODE" });
    defer allocator.free(resp);

    // With WITHCODE: library map is %4\r\n (4 fields: library_name, engine, functions, library_code)
    try testing.expect(std.mem.indexOf(u8, resp, "%4\r\n") != null);
    // library_code field present
    try testing.expect(std.mem.indexOf(u8, resp, "library_code") != null);
    // Actual code present
    try testing.expect(std.mem.indexOf(u8, resp, "testlib") != null);
}

test "iter394 - FUNCTION LIST RESP2 WITHCODE includes library_code" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9815");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST", "WITHCODE" });
    defer allocator.free(resp);

    // RESP2 WITHCODE: *10\r\n (10 elements = 5 pairs)
    try testing.expect(std.mem.indexOf(u8, resp, "*10\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "library_code") != null);
}

// ─── FUNCTION STATS RESP2 ────────────────────────────────────────────────────

test "iter394 - FUNCTION STATS RESP2 no running script returns null bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9816");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "STATS" });
    defer allocator.free(resp);

    // RESP2: *4\r\n (flat array)
    try testing.expect(std.mem.indexOf(u8, resp, "*4\r\n") != null);
    // running_script key
    try testing.expect(std.mem.indexOf(u8, resp, "running_script") != null);
    // null bulk string ($-1\r\n) for not running
    try testing.expect(std.mem.indexOf(u8, resp, "$-1\r\n") != null);
    // engines key
    try testing.expect(std.mem.indexOf(u8, resp, "engines") != null);
    // LUA engine present
    try testing.expect(std.mem.indexOf(u8, resp, "LUA") != null);
    // libraries_count key
    try testing.expect(std.mem.indexOf(u8, resp, "libraries_count") != null);
    // functions_count key
    try testing.expect(std.mem.indexOf(u8, resp, "functions_count") != null);
}

test "iter394 - FUNCTION STATS RESP2 with library shows counts" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9817");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "STATS" });
    defer allocator.free(resp);

    // libraries_count = 1, functions_count = 1
    try testing.expect(std.mem.indexOf(u8, resp, ":1\r\n") != null);
}

// ─── FUNCTION STATS RESP3 ────────────────────────────────────────────────────

test "iter394 - FUNCTION STATS RESP3 returns map with boolean false" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9818");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "STATS" });
    defer allocator.free(resp);

    // RESP3: %2\r\n (outer map with 2 keys)
    try testing.expect(std.mem.indexOf(u8, resp, "%2\r\n") != null);
    // running_script key
    try testing.expect(std.mem.indexOf(u8, resp, "running_script") != null);
    // boolean false (#f\r\n) for not running
    try testing.expect(std.mem.indexOf(u8, resp, "#f\r\n") != null);
    // Must NOT contain null bulk string
    try testing.expect(std.mem.indexOf(u8, resp, "$-1\r\n") == null);
    // engines key
    try testing.expect(std.mem.indexOf(u8, resp, "engines") != null);
    // LUA present
    try testing.expect(std.mem.indexOf(u8, resp, "LUA") != null);
}

test "iter394 - FUNCTION STATS RESP3 engines is a map of maps" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9819");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "STATS" });
    defer allocator.free(resp);

    // Engines: %1\r\n (map with 1 key: LUA)
    // LUA value: %2\r\n (map with libraries_count, functions_count)
    // Total %2\r\n occurrences: outer map + LUA stats map = 2
    const map2_count = countOccurrences(resp, "%2\r\n");
    try testing.expect(map2_count >= 2);
    // %1\r\n for engines map
    try testing.expect(std.mem.indexOf(u8, resp, "%1\r\n") != null);
    // libraries_count and functions_count
    try testing.expect(std.mem.indexOf(u8, resp, "libraries_count") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "functions_count") != null);
}

test "iter394 - FUNCTION STATS RESP3 counts update after LOAD" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9820");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    // Before loading: counts are 0
    const resp_before = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "STATS" });
    defer allocator.free(resp_before);
    try testing.expect(std.mem.indexOf(u8, resp_before, ":0\r\n") != null);

    // Load a library
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    // After loading: counts are 1
    const resp_after = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "STATS" });
    defer allocator.free(resp_after);
    try testing.expect(std.mem.indexOf(u8, resp_after, ":1\r\n") != null);
}

// ─── FUNCTION LIST LIBRARYNAME filter ────────────────────────────────────────

test "iter394 - FUNCTION LIST LIBRARYNAME filter RESP2" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9821");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LOAD", TEST_LIB }));

    // Filter by exact name — should return 1 library
    const resp_match = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST", "LIBRARYNAME", "testlib" });
    defer allocator.free(resp_match);
    try testing.expect(std.mem.indexOf(u8, resp_match, "testlib") != null);

    // Filter by non-existent name — should return empty
    const resp_miss = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "FUNCTION", "LIST", "LIBRARYNAME", "nonexistent" });
    defer allocator.free(resp_miss);
    try testing.expectEqualStrings("*0\r\n", resp_miss);
}

// ─── Helper ──────────────────────────────────────────────────────────────────

fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    var count: usize = 0;
    var idx: usize = 0;
    while (std.mem.indexOf(u8, haystack[idx..], needle)) |pos| {
        count += 1;
        idx += pos + needle.len;
    }
    return count;
}
