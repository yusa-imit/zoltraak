// Iteration 382: sailor v2.62.0 + RESP3 map type for ZSCAN
//
// In RESP2, ZSCAN returns [cursor, [member, score, member, score, ...]] (flat array)
// In RESP3, ZSCAN returns [cursor, %N map{member: double_score, ...}]

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

// ─── ZSCAN RESP2 ────────────────────────────────────────────────────────────

test "iter382 - ZSCAN RESP2 returns flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8201");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "myzset", "1.5", "alpha", "2.5", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "myzset", "0" });
    defer allocator.free(result);

    // RESP2: outer *2, second element *4 (flat array: alpha,1.5,beta,2.5)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") != null);
    // Must NOT contain map header in RESP2
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "alpha") != null);
    try testing.expect(std.mem.indexOf(u8, result, "1.5") != null);
}

// ─── ZSCAN RESP3 ────────────────────────────────────────────────────────────

test "iter382 - ZSCAN RESP3 returns map with double scores" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8202");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "myzset", "1.5", "alpha", "2.5", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "myzset", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %2 (map with 2 member-score pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") != null);
    // Must NOT contain flat array in RESP3
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "alpha") != null);
    // Scores as double type (,score\r\n)
    try testing.expect(std.mem.indexOf(u8, result, ",1.5\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",2.5\r\n") != null);
}

// ─── ZSCAN RESP3 empty set ───────────────────────────────────────────────────

test "iter382 - ZSCAN RESP3 non-existent key returns empty map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8203");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "nonexistent", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %0 (empty map)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%0\r\n") != null);
}

// ─── ZSCAN RESP3 MATCH ───────────────────────────────────────────────────────

test "iter382 - ZSCAN RESP3 MATCH returns filtered map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8204");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "myzset", "1.0", "apple", "2.0", "apricot", "3.0", "banana" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "myzset", "0", "MATCH", "ap*" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %2 (map with 2 ap* members)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "apple") != null);
    try testing.expect(std.mem.indexOf(u8, result, "apricot") != null);
    // banana filtered out
    try testing.expect(std.mem.indexOf(u8, result, "banana") == null);
}

// ─── ZSCAN RESP3 COUNT=0 ─────────────────────────────────────────────────────

test "iter382 - ZSCAN RESP3 COUNT 0 returns all members as map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8205");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "myzset", "10.0", "x", "20.0", "y", "30.0", "z" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "myzset", "0", "COUNT", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %3 (all 3 members as map)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",10\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",20\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",30\r\n") != null);
}

// ─── ZSCAN RESP2 still returns flat array for backward compat ────────────────

test "iter382 - ZSCAN RESP2 cursor format is bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8206");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "myzset", "99.0", "member1" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "myzset", "0" });
    defer allocator.free(result);

    // RESP2: cursor returned as bulk string ($N\r\n...), not integer (:N\r\n)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n$"));
    try testing.expect(!std.mem.startsWith(u8, result, "*2\r\n:"));
}

// ─── ZSCAN RESP3 integer scores shown as doubles ──────────────────────────────

test "iter382 - ZSCAN RESP3 integer scores use double type notation" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8207");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "myzset", "5", "member1" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "myzset", "0" });
    defer allocator.free(result);

    // RESP3: map with double type score (,5\r\n)
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") != null);
    // Score as RESP3 double type: ,5\r\n
    try testing.expect(std.mem.indexOf(u8, result, ",5\r\n") != null);
}

// ─── ZSCAN RESP3 single member ────────────────────────────────────────────────

test "iter382 - ZSCAN RESP3 single member returns map of 1" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8208");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "solo", "3.14", "pi" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZSCAN", "solo", "0" });
    defer allocator.free(result);

    // RESP3: %1 map
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "pi") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",3.14\r\n") != null);
}
