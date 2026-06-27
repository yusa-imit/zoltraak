// Iteration 380: sailor v2.60.0 + RESP3 map type for XRANGE/XREVRANGE entry fields
//                + RESP3 map type for HRANDFIELD WITHVALUES
//
// In RESP2, XRANGE/XREVRANGE entry fields are returned as:
//   *2N\r\n field0 val0 field1 val1 ...   (flat array)
//
// In RESP3, XRANGE/XREVRANGE entry fields are returned as:
//   %N\r\n field0 val0 field1 val1 ...    (map)
//
// In RESP2, HRANDFIELD key count WITHVALUES returns:
//   *2N\r\n field0 val0 field1 val1 ...   (flat array)
//
// In RESP3, HRANDFIELD key count WITHVALUES returns:
//   %N\r\n field0 val0 field1 val1 ...    (map)

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

// ─── XRANGE RESP2 ────────────────────────────────────────────────────────────

test "iter380 - XRANGE RESP2 entry fields are flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6001");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "field1", "val1", "field2", "val2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XRANGE", "s", "-", "+" });
    defer allocator.free(result);

    // RESP2: outer *1, entry *2, fields *4 (2 pairs = 4 items)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    // Entry fields: *4\r\n (flat array of 4 items)
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") != null);
    // Must NOT contain map header %2\r\n in RESP2
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "val1") != null);
}

// ─── XRANGE RESP3 ────────────────────────────────────────────────────────────

test "iter380 - XRANGE RESP3 entry fields are map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6002");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "field1", "val1", "field2", "val2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XRANGE", "s", "-", "+" });
    defer allocator.free(result);

    // RESP3: outer *1, entry *2, fields %2 (map with 2 pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    // Entry fields: %2\r\n (map of 2 key-value pairs)
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") != null);
    // Must NOT contain flat array *4\r\n for fields in RESP3
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "val2") != null);
}

// ─── XRANGE RESP3 empty stream ───────────────────────────────────────────────

test "iter380 - XRANGE RESP3 non-existent key returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6003");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XRANGE", "nokey", "-", "+" });
    defer allocator.free(result);

    try testing.expectEqualStrings("*0\r\n", result);
}

// ─── XREVRANGE RESP2 ─────────────────────────────────────────────────────────

test "iter380 - XREVRANGE RESP2 entry fields are flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6004");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "2000-0", "f", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XREVRANGE", "s", "+", "-" });
    defer allocator.free(result);

    // RESP2: outer *2, each entry *2, fields *2 (1 pair = 2 items)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Should NOT have map type
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") == null);
    // Reverse order: 2000-0 before 1000-0
    const pos2000 = std.mem.indexOf(u8, result, "2000-0").?;
    const pos1000 = std.mem.indexOf(u8, result, "1000-0").?;
    try testing.expect(pos2000 < pos1000);
}

// ─── XREVRANGE RESP3 ─────────────────────────────────────────────────────────

test "iter380 - XREVRANGE RESP3 entry fields are map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6005");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "sensor", "temp" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "2000-0", "sensor", "humidity" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XREVRANGE", "s", "+", "-" });
    defer allocator.free(result);

    // RESP3: outer *2, each entry *2, fields %1 (1 pair as map)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "sensor") != null);
    // 2000-0 before 1000-0
    const pos2000 = std.mem.indexOf(u8, result, "2000-0").?;
    const pos1000 = std.mem.indexOf(u8, result, "1000-0").?;
    try testing.expect(pos2000 < pos1000);
}

// ─── XRANGE RESP3 multiple entries ───────────────────────────────────────────

test "iter380 - XRANGE RESP3 multiple entries each have map fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6006");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "a", "1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "2000-0", "b", "2" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "3000-0", "c", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XRANGE", "s", "-", "+" });
    defer allocator.free(result);

    // All 3 entries should have %1\r\n map headers
    var count: usize = 0;
    var idx: usize = 0;
    while (std.mem.indexOf(u8, result[idx..], "%1\r\n")) |pos| {
        count += 1;
        idx += pos + 4;
    }
    try testing.expectEqual(@as(usize, 3), count);
}

// ─── XRANGE RESP3 with COUNT ─────────────────────────────────────────────────

test "iter380 - XRANGE RESP3 with COUNT returns map fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6007");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "f", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "2000-0", "f", "v2" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "3000-0", "f", "v3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XRANGE", "s", "-", "+", "COUNT", "2" });
    defer allocator.free(result);

    // 2 entries, each with %1\r\n map
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "3000-0") == null);
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") != null);
}

// ─── HRANDFIELD WITHVALUES RESP2 ─────────────────────────────────────────────

test "iter380 - HRANDFIELD WITHVALUES RESP2 returns flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6008");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "h", "f1", "v1", "f2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HRANDFIELD", "h", "2", "WITHVALUES" });
    defer allocator.free(result);

    // RESP2: flat array *4\r\n (2 field-value pairs = 4 items)
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
    // Must NOT have map type in RESP2
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "v1") != null or std.mem.indexOf(u8, result, "v2") != null);
}

// ─── HRANDFIELD WITHVALUES RESP3 ─────────────────────────────────────────────

test "iter380 - HRANDFIELD WITHVALUES RESP3 returns map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6009");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "h", "f1", "v1", "f2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HRANDFIELD", "h", "2", "WITHVALUES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n (2 pairs)
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    // Must NOT have flat array *4\r\n in RESP3
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "v1") != null or std.mem.indexOf(u8, result, "v2") != null);
}

// ─── HRANDFIELD without WITHVALUES RESP3 ─────────────────────────────────────

test "iter380 - HRANDFIELD without WITHVALUES RESP3 still returns array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6010");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "h", "f1", "v1", "f2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HRANDFIELD", "h", "2" });
    defer allocator.free(result);

    // Without WITHVALUES, always returns array even in RESP3
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%") == null);
}

// ─── HRANDFIELD WITHVALUES RESP3 empty hash ──────────────────────────────────

test "iter380 - HRANDFIELD WITHVALUES RESP3 nonexistent key returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "6011");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HRANDFIELD", "nokey", "3", "WITHVALUES" });
    defer allocator.free(result);

    // Non-existent key with count → empty array (not map, matches Redis behavior)
    try testing.expectEqualStrings("*0\r\n", result);
}
