// Iteration 396: RESP3 map type for SLOWLOG GET entries + sailor v2.74.0
//
// Redis protocol behavior:
// - SLOWLOG GET RESP2: each entry is a *6 array [id, timestamp, duration, args, client-addr, client-name]
// - SLOWLOG GET RESP3: each entry is a %6 map with named keys
//   Keys: "id", "timestamp", "duration", "args", "client-addr", "client-name"
//
// sailor v2.74.0: ChordDiagram widget added (library update, no API changes)

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

// ─── SLOWLOG GET RESP2 ─────────────────────────────────────────────────────────

test "iter396 - SLOWLOG GET RESP2 returns flat arrays" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9830");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Inject a slow log entry directly
    _ = try ctx.storage.slowlog.logCommand(15000, "SET mykey myval", "127.0.0.1:55000", "myapp");

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET",
    });
    defer allocator.free(resp);

    // RESP2: outer *N array; each entry is *6 array (not map)
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, "*6\r\n") != null);
    // Must NOT be RESP3 map format
    try testing.expect(std.mem.indexOf(u8, resp, "%6\r\n") == null);
    // Command present
    try testing.expect(std.mem.indexOf(u8, resp, "SET") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "mykey") != null);
}

test "iter396 - SLOWLOG GET RESP2 empty returns *0" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9831");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET",
    });
    defer allocator.free(resp);

    try testing.expectEqualStrings("*0\r\n", resp);
}

test "iter396 - SLOWLOG LEN returns correct count" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9832");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    _ = try ctx.storage.slowlog.logCommand(11000, "GET k1", "127.0.0.1:1", "");
    _ = try ctx.storage.slowlog.logCommand(12000, "GET k2", "127.0.0.1:2", "");

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "LEN",
    });
    defer allocator.free(resp);

    try testing.expectEqualStrings(":2\r\n", resp);
}

test "iter396 - SLOWLOG RESET clears the log" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9833");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    _ = try ctx.storage.slowlog.logCommand(11000, "GET k1", "127.0.0.1:1", "");

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "RESET",
    }));

    const len_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "LEN",
    });
    defer allocator.free(len_resp);

    try testing.expectEqualStrings(":0\r\n", len_resp);
}

// ─── SLOWLOG GET RESP3 ─────────────────────────────────────────────────────────

test "iter396 - SLOWLOG GET RESP3 returns maps" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9834");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    _ = try ctx.storage.slowlog.logCommand(25000, "HSET myhash f1 v1", "127.0.0.1:9000", "agent");

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET",
    });
    defer allocator.free(resp);

    // RESP3: outer array, each entry is %6 map
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, "%6\r\n") != null);
    // Must NOT contain RESP2 entry array
    try testing.expect(std.mem.indexOf(u8, resp, "*6\r\n") == null);
    // Map keys present as bulk strings
    try testing.expect(std.mem.indexOf(u8, resp, "id") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "timestamp") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "duration") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "args") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "client-addr") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "client-name") != null);
    // Values present
    try testing.expect(std.mem.indexOf(u8, resp, "HSET") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "127.0.0.1:9000") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "agent") != null);
}

test "iter396 - SLOWLOG GET RESP3 empty returns *0" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9835");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET",
    });
    defer allocator.free(resp);

    // Empty: no entries regardless of protocol
    try testing.expectEqualStrings("*0\r\n", resp);
}

test "iter396 - SLOWLOG GET RESP3 duration integer value" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9836");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    _ = try ctx.storage.slowlog.logCommand(99999, "GET slowkey", "127.0.0.1:7777", "");

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET",
    });
    defer allocator.free(resp);

    // Duration 99999 µs should appear as integer after duration key
    try testing.expect(std.mem.indexOf(u8, resp, "duration") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ":99999\r\n") != null);
}

test "iter396 - SLOWLOG GET with COUNT limit works in RESP2" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9837");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    _ = try ctx.storage.slowlog.logCommand(10000, "GET k1", "127.0.0.1:1", "");
    _ = try ctx.storage.slowlog.logCommand(20000, "SET k2 v2", "127.0.0.1:2", "");
    _ = try ctx.storage.slowlog.logCommand(30000, "DEL k3", "127.0.0.1:3", "");

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET", "1",
    });
    defer allocator.free(resp);

    // Only 1 entry returned
    try testing.expect(std.mem.startsWith(u8, resp, "*1\r\n"));
}

test "iter396 - SLOWLOG GET with COUNT limit works in RESP3" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9838");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    _ = try ctx.storage.slowlog.logCommand(10000, "GET k1", "127.0.0.1:1", "");
    _ = try ctx.storage.slowlog.logCommand(20000, "SET k2 v2", "127.0.0.1:2", "");

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "GET", "1",
    });
    defer allocator.free(resp);

    // Only 1 entry returned, in map format
    try testing.expect(std.mem.startsWith(u8, resp, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, resp, "%6\r\n") != null);
}

test "iter396 - SLOWLOG HELP returns help text" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9839");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SLOWLOG", "HELP",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "SLOWLOG GET") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "SLOWLOG LEN") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "SLOWLOG RESET") != null);
}
