// Iteration 384: sailor v2.64.0 + RESP3 map type for XINFO GROUPS and XINFO CONSUMERS
//
// In RESP2, XINFO GROUPS returns each group as a flat array (*12\r\n for 6 key-value pairs).
// In RESP3, XINFO GROUPS returns each group as a map (%6\r\n — 6 key-value pairs).
//
// In RESP2, XINFO CONSUMERS returns each consumer as a flat array (*8\r\n for 4 key-value pairs).
// In RESP3, XINFO CONSUMERS returns each consumer as a map (%4\r\n — 4 key-value pairs).

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

// ─── XINFO GROUPS RESP2 flat array ──────────────────────────────────────────

test "iter384 - XINFO GROUPS RESP2 returns flat array per group" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8400");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Create stream with one entry and one consumer group
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "GROUPS", "s" });
    defer allocator.free(result);

    // RESP2: outer *1 (one group), each group is *12\r\n flat array
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*12\r\n") != null);
    // Must NOT use map type
    try testing.expect(std.mem.indexOf(u8, result, "%6\r\n") == null);
    // Must contain field names
    try testing.expect(std.mem.indexOf(u8, result, "name") != null);
    try testing.expect(std.mem.indexOf(u8, result, "consumers") != null);
    try testing.expect(std.mem.indexOf(u8, result, "last-delivered-id") != null);
}

// ─── XINFO GROUPS RESP3 map type ────────────────────────────────────────────

test "iter384 - XINFO GROUPS RESP3 returns map per group" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8401");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "GROUPS", "s" });
    defer allocator.free(result);

    // RESP3: outer *1 (one group), each group is %6\r\n map (6 key-value pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%6\r\n") != null);
    // Must NOT use flat array type for the group entry
    try testing.expect(std.mem.indexOf(u8, result, "*12\r\n") == null);
    // Must contain all 6 field names
    try testing.expect(std.mem.indexOf(u8, result, "name") != null);
    try testing.expect(std.mem.indexOf(u8, result, "consumers") != null);
    try testing.expect(std.mem.indexOf(u8, result, "pending") != null);
    try testing.expect(std.mem.indexOf(u8, result, "last-delivered-id") != null);
    try testing.expect(std.mem.indexOf(u8, result, "entries-read") != null);
    try testing.expect(std.mem.indexOf(u8, result, "lag") != null);
}

// ─── XINFO CONSUMERS RESP2 flat array ───────────────────────────────────────

test "iter384 - XINFO CONSUMERS RESP2 returns flat array per consumer" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8402");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Create stream, group, and consumer
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "CONSUMERS", "s", "g" });
    defer allocator.free(result);

    // RESP2: outer *1 (one consumer), each consumer is *8\r\n flat array
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*8\r\n") != null);
    // Must NOT use map type
    try testing.expect(std.mem.indexOf(u8, result, "%4\r\n") == null);
    // Must contain consumer name and field names
    try testing.expect(std.mem.indexOf(u8, result, "c1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "idle") != null);
}

// ─── XINFO CONSUMERS RESP3 map type ─────────────────────────────────────────

test "iter384 - XINFO CONSUMERS RESP3 returns map per consumer" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8403");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // Create stream, group, and consumer
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "CONSUMERS", "s", "g" });
    defer allocator.free(result);

    // RESP3: outer *1 (one consumer), each consumer is %4\r\n map (4 key-value pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%4\r\n") != null);
    // Must NOT use flat array type for the consumer entry
    try testing.expect(std.mem.indexOf(u8, result, "*8\r\n") == null);
    // Must contain all 4 field names
    try testing.expect(std.mem.indexOf(u8, result, "name") != null);
    try testing.expect(std.mem.indexOf(u8, result, "pending") != null);
    try testing.expect(std.mem.indexOf(u8, result, "idle") != null);
    try testing.expect(std.mem.indexOf(u8, result, "inactive") != null);
    // Must contain consumer name value
    try testing.expect(std.mem.indexOf(u8, result, "c1") != null);
}

// ─── XINFO GROUPS RESP3 with null lag ───────────────────────────────────────

test "iter384 - XINFO GROUPS RESP3 null lag still present in map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8404");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // Create stream with two entries
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1000-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "2000-0", "f", "v" }));

    // Create group at arbitrary position (1500-0) — lag will be null
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "1500-0" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "GROUPS", "s" });
    defer allocator.free(result);

    // RESP3: map per group, null lag ($-1) still present
    try testing.expect(std.mem.indexOf(u8, result, "%6\r\n") != null);
    // Null lag should be preserved as $-1
    try testing.expect(std.mem.indexOf(u8, result, "$-1\r\n") != null);
}

// ─── XINFO GROUPS RESP3 empty stream ────────────────────────────────────────

test "iter384 - XINFO GROUPS RESP3 empty stream returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8405");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    // No groups created

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "GROUPS", "s" });
    defer allocator.free(result);

    // No groups → outer *0\r\n empty array
    try testing.expectEqualStrings("*0\r\n", result);
}

// ─── XINFO GROUPS RESP3 multiple groups ─────────────────────────────────────

test "iter384 - XINFO GROUPS RESP3 multiple groups each as map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8406");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g1", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g2", "0" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "GROUPS", "s" });
    defer allocator.free(result);

    // 2 groups, each as %6\r\n map
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Two map headers
    var count: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, result, pos, "%6\r\n")) |idx| {
        count += 1;
        pos = idx + 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
}

// ─── XINFO CONSUMERS RESP3 no consumers ─────────────────────────────────────

test "iter384 - XINFO CONSUMERS RESP3 no consumers returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8407");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));
    // No XREADGROUP — no consumers yet

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "CONSUMERS", "s", "g" });
    defer allocator.free(result);

    // No consumers → *0\r\n
    try testing.expectEqualStrings("*0\r\n", result);
}

// ─── XINFO GROUPS RESP2 lag present ─────────────────────────────────────────

test "iter384 - XINFO GROUPS RESP2 computed lag is present" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8408");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // RESP2 (default)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "2-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));
    // Read 1 entry
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "GROUPS", "s" });
    defer allocator.free(result);

    // RESP2 flat array: *12\r\n
    try testing.expect(std.mem.indexOf(u8, result, "*12\r\n") != null);
    // entries_added=2, entries_read=1, lag=1
    try testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
}
