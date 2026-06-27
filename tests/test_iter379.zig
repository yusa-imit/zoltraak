// Iteration 379: RESP3 map type for XREAD and XREADGROUP
//
// In RESP2, XREAD/XREADGROUP return:
//   *N\r\n                     (array of N streams)
//   *2\r\n                     (each stream is [key, entries] pair)
//   $K\r\n<key>\r\n
//   *M\r\n...                  (array of M entries)
//
// In RESP3, XREAD/XREADGROUP return:
//   %N\r\n                     (map of N stream_name -> entries)
//   $K\r\n<key>\r\n            (key — no *2\r\n wrapper)
//   *M\r\n...                  (array of M entries)

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

// ─── XREAD RESP2 ─────────────────────────────────────────────────────────────

test "iter379 - XREAD RESP2 returns array with *2 stream wrapper" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9601");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("xr1", "1000-1", &.{ "name", "alice" }, null, .{});

    // RESP2: XREAD STREAMS xr1 0-0
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "xr1", "0-0" });
    defer allocator.free(result);

    // RESP2 format: *1\r\n*2\r\n$3\r\nxr1\r\n*1\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    // The *2\r\n wrapper for [key, entries] must be present
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Key "xr1" present as bulk string
    try testing.expect(std.mem.indexOf(u8, result, "$3\r\nxr1\r\n") != null);
}

test "iter379 - XREAD RESP2 no data returns null array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9602");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("xr2", "1000-1", &.{ "f", "v" }, null, .{});

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "xr2", "9999-0" });
    defer allocator.free(result);

    try testing.expectEqualStrings("*-1\r\n", result);
}

// ─── XREAD RESP3 ─────────────────────────────────────────────────────────────

test "iter379 - XREAD RESP3 returns map with % prefix" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9603");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("xr3", "2000-1", &.{ "field1", "val1" }, null, .{});

    // Switch to RESP3
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "xr3", "0-0" });
    defer allocator.free(result);

    // RESP3 map format: %1\r\n
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    // Key "xr3" present
    try testing.expect(std.mem.indexOf(u8, result, "$3\r\nxr3\r\n") != null);
    // The key is immediately after %1\r\n (no *2\r\n stream wrapper before the key)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n$3\r\nxr3\r\n"));
}

test "iter379 - XREAD RESP3 no data still returns null array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9604");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("xr4", "1000-1", &.{ "f", "v" }, null, .{});

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "xr4", "9999-0" });
    defer allocator.free(result);

    try testing.expectEqualStrings("*-1\r\n", result);
}

test "iter379 - XREAD RESP3 multiple streams returns map with N entries" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9605");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("s1", "1-0", &.{ "k", "v1" }, null, .{});
    _ = try s.storage.xadd("s2", "2-0", &.{ "k", "v2" }, null, .{});

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "s1", "s2", "0-0", "0-0" });
    defer allocator.free(result);

    // RESP3 map with 2 entries
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "$2\r\ns1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$2\r\ns2\r\n") != null);
}

// ─── XREADGROUP RESP2 ────────────────────────────────────────────────────────

test "iter379 - XREADGROUP RESP2 returns array with *2 stream wrapper" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9606");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("grp1", "5000-1", &.{ "f", "v" }, null, .{});
    try s.storage.xgroupCreate("grp1", "g1", "0", null);

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "grp1", ">" });
    defer allocator.free(result);

    // RESP2: *1\r\n*2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$4\r\ngrp1\r\n") != null);
}

// ─── XREADGROUP RESP3 ────────────────────────────────────────────────────────

test "iter379 - XREADGROUP RESP3 returns map with % prefix" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9607");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("grp2", "6000-1", &.{ "x", "y" }, null, .{});
    try s.storage.xgroupCreate("grp2", "g2", "0", null);

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREADGROUP", "GROUP", "g2", "c2", "STREAMS", "grp2", ">" });
    defer allocator.free(result);

    // RESP3 map: %1\r\n
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "$4\r\ngrp2\r\n") != null);
    // Key is immediately after %1\r\n (no *2\r\n stream wrapper before the key)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n$4\r\ngrp2\r\n"));
}

test "iter379 - XREADGROUP RESP3 no messages returns null array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9608");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("grp3", "7000-1", &.{ "x", "y" }, null, .{});
    try s.storage.xgroupCreate("grp3", "g3", "$", null);

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HELLO", "3" }));

    // No new messages after "$"
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREADGROUP", "GROUP", "g3", "c3", "STREAMS", "grp3", ">" });
    defer allocator.free(result);

    try testing.expectEqualStrings("*-1\r\n", result);
}

test "iter379 - XREAD RESP2 entry fields are correct" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9609");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("ef1", "100-0", &.{ "color", "blue" }, null, .{});

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "ef1", "0-0" });
    defer allocator.free(result);

    // Fields "color" and "blue" must appear in the response
    try testing.expect(std.mem.indexOf(u8, result, "color") != null);
    try testing.expect(std.mem.indexOf(u8, result, "blue") != null);
    // Entry ID "100-0" must appear
    try testing.expect(std.mem.indexOf(u8, result, "100-0") != null);
}

test "iter379 - XREAD RESP3 entry fields are correct" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9610");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    _ = try s.storage.xadd("ef2", "200-0", &.{ "city", "seoul" }, null, .{});

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "XREAD", "STREAMS", "ef2", "0-0" });
    defer allocator.free(result);

    // RESP3 map + correct entry data
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "city") != null);
    try testing.expect(std.mem.indexOf(u8, result, "seoul") != null);
    try testing.expect(std.mem.indexOf(u8, result, "200-0") != null);
}

