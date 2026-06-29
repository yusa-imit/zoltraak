// Iteration 389: sailor v2.68.0 + RESP3 double type for sorted set pop commands
//
// ZPOPMIN/ZPOPMAX RESP2: [member, score_bulk_string, ...]
// ZPOPMIN/ZPOPMAX RESP3: [member, score_double (,val\r\n), ...]
//
// BZPOPMIN/BZPOPMAX RESP2: [key, member, score_bulk_string]
// BZPOPMIN/BZPOPMAX RESP3: [key, member, score_double (,val\r\n)]
//
// ZMPOP/BZMPOP inner scores RESP2: bulk strings
// ZMPOP/BZMPOP inner scores RESP3: double (,val\r\n)

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

// ─── ZPOPMIN ─────────────────────────────────────────────────────────────────

test "iter389 - ZPOPMIN RESP2 returns score as bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8900");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zkey389a", "1.5", "alpha" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZPOPMIN", "zkey389a" });
    defer allocator.free(result);

    // RESP2: score as bulk string $N\r\n1.5\r\n, NOT double type
    try testing.expect(std.mem.indexOf(u8, result, "$3\r\n1.5\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",1.5\r\n") == null);
}

test "iter389 - ZPOPMIN RESP3 returns score as double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8901");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zkey389b", "2.5", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZPOPMIN", "zkey389b" });
    defer allocator.free(result);

    // RESP3: score as double ,2.5\r\n
    try testing.expect(std.mem.indexOf(u8, result, ",2.5\r\n") != null);
    // Must NOT be bulk string format for score
    try testing.expect(std.mem.indexOf(u8, result, "$3\r\n2.5\r\n") == null);
}

test "iter389 - ZPOPMIN RESP3 with +inf score" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8902");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zkey389c", "+inf", "gamma" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZPOPMIN", "zkey389c" });
    defer allocator.free(result);

    // RESP3: +inf score as double ",inf\r\n"
    try testing.expect(std.mem.indexOf(u8, result, ",inf\r\n") != null);
}

// ─── ZPOPMAX ─────────────────────────────────────────────────────────────────

test "iter389 - ZPOPMAX RESP2 returns score as bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8903");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zkey389d", "10", "delta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZPOPMAX", "zkey389d" });
    defer allocator.free(result);

    // RESP2: score as bulk string
    try testing.expect(std.mem.indexOf(u8, result, "$2\r\n10\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",10\r\n") == null);
}

test "iter389 - ZPOPMAX RESP3 returns score as double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8904");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zkey389e", "7", "epsilon" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZPOPMAX", "zkey389e" });
    defer allocator.free(result);

    // RESP3: score as double ,7\r\n
    try testing.expect(std.mem.indexOf(u8, result, ",7\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n7\r\n") == null);
}

// ─── BZPOPMIN ────────────────────────────────────────────────────────────────

test "iter389 - BZPOPMIN RESP2 returns score as bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8905");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "bzkey389a", "3.14", "pi" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "BZPOPMIN", "bzkey389a", "0.1" });
    defer allocator.free(result);

    // RESP2: [key, member, score_bulk]
    try testing.expect(std.mem.indexOf(u8, result, "bzkey389a") != null);
    try testing.expect(std.mem.indexOf(u8, result, "pi") != null);
    // Score as bulk string, not double
    try testing.expect(std.mem.indexOf(u8, result, ",3.14\r\n") == null);
}

test "iter389 - BZPOPMIN RESP3 returns score as double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8906");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "bzkey389b", "5", "five" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "BZPOPMIN", "bzkey389b", "0.1" });
    defer allocator.free(result);

    // RESP3: [key, member, score_double]
    try testing.expect(std.mem.indexOf(u8, result, "bzkey389b") != null);
    try testing.expect(std.mem.indexOf(u8, result, "five") != null);
    // Score as double ,5\r\n
    try testing.expect(std.mem.indexOf(u8, result, ",5\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n5\r\n") == null);
}

// ─── BZPOPMAX ────────────────────────────────────────────────────────────────

test "iter389 - BZPOPMAX RESP3 returns score as double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8907");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "bzkey389c", "100", "hundred" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "BZPOPMAX", "bzkey389c", "0.1" });
    defer allocator.free(result);

    // RESP3: score as double ,100\r\n
    try testing.expect(std.mem.indexOf(u8, result, ",100\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "hundred") != null);
}

// ─── ZMPOP ───────────────────────────────────────────────────────────────────

test "iter389 - ZMPOP RESP2 returns scores as bulk strings" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8908");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zmkey389a", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZMPOP", "1", "zmkey389a", "MIN" });
    defer allocator.free(result);

    // RESP2: scores as bulk strings (not doubles)
    try testing.expect(std.mem.indexOf(u8, result, "zmkey389a") != null);
    // No double prefix
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") == null);
    // Bulk string score
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") != null);
}

test "iter389 - ZMPOP RESP3 returns scores as doubles" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8909");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zmkey389b", "4", "d", "8", "h" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZMPOP", "1", "zmkey389b", "MIN" });
    defer allocator.free(result);

    // RESP3: scores as doubles ,4\r\n
    try testing.expect(std.mem.indexOf(u8, result, "zmkey389b") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",4\r\n") != null);
    // Not bulk string
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n4\r\n") == null);
}

test "iter389 - ZMPOP RESP3 empty keys returns null array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8910");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZMPOP", "1", "noexist389", "MIN" });
    defer allocator.free(result);

    // Null array regardless of RESP version
    try testing.expectEqualStrings("*-1\r\n", result);
}

// ─── BZMPOP ──────────────────────────────────────────────────────────────────

test "iter389 - BZMPOP RESP3 returns scores as doubles" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8911");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "bmkey389a", "9.9", "x" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "BZMPOP", "0.1", "1", "bmkey389a", "MIN" });
    defer allocator.free(result);

    // RESP3: score as double ,9.9\r\n
    try testing.expect(std.mem.indexOf(u8, result, "bmkey389a") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",9.9\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$3\r\n9.9\r\n") == null);
}
