// Iteration 393: sailor v2.72.0 + RESP3 double type for ZRANK/ZREVRANK WITHSCORE
//
// Redis protocol behavior:
// - ZRANK key member WITHSCORE: RESP2 returns [rank, score_bulk_string], RESP3 returns [rank, ,score\r\n]
// - ZREVRANK key member WITHSCORE: same as ZRANK WITHSCORE
// - ZRANK/ZREVRANK without WITHSCORE: always integer reply (unchanged)
// - Non-existent member: always null reply (unchanged)
//
// Affected commands: ZRANK, ZREVRANK

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

// ─── ZRANK WITHSCORE RESP2 ────────────────────────────────────────────────────

test "iter393 - ZRANK WITHSCORE RESP2 returns bulk string score" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9800");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "1.5", "alpha" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "2.5", "beta" }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANK", "zs", "alpha", "WITHSCORE" });
    defer allocator.free(resp);

    // RESP2: [rank_int, score_bulk_string] = *2\r\n:0\r\n$3\r\n1.5\r\n
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
    // Score as bulk string ($3\r\n1.5\r\n)
    try testing.expect(std.mem.indexOf(u8, resp, "$") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "1.5") != null);
    // Must NOT be double type
    try testing.expect(std.mem.indexOf(u8, resp, ",1.5") == null);
}

// ─── ZRANK WITHSCORE RESP3 ────────────────────────────────────────────────────

test "iter393 - ZRANK WITHSCORE RESP3 returns double score" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9801");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "1.5", "alpha" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "2.5", "beta" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANK", "zs", "alpha", "WITHSCORE" });
    defer allocator.free(resp);

    // RESP3: [rank_int, double_score] = *2\r\n:0\r\n,1.5\r\n
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
    // Score as RESP3 double type: ,val\r\n
    try testing.expect(std.mem.indexOf(u8, resp, ",1.5\r\n") != null);
    // Must NOT be bulk string
    try testing.expect(std.mem.indexOf(u8, resp, "$3\r\n1.5") == null);
}

test "iter393 - ZRANK WITHSCORE RESP3 integer score" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9802");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "3", "member" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANK", "zs", "member", "WITHSCORE" });
    defer allocator.free(resp);

    // Rank 0, score 3 as double: ,3\r\n
    try testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",3\r\n") != null);
}

// ─── ZREVRANK WITHSCORE ───────────────────────────────────────────────────────

test "iter393 - ZREVRANK WITHSCORE RESP2 returns bulk string score" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9803");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "10", "a" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "20", "b" }));

    // b has score 20, so in descending order it's rank 0
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANK", "zs", "b", "WITHSCORE" });
    defer allocator.free(resp);

    // RESP2: *2\r\n:0\r\n$2\r\n20\r\n
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "$") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "20") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",20") == null);
}

test "iter393 - ZREVRANK WITHSCORE RESP3 returns double score" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9804");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "10", "a" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "20", "b" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    // b has score 20 → RESP3: *2\r\n:0\r\n,20\r\n
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANK", "zs", "b", "WITHSCORE" });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",20\r\n") != null);
    // Must NOT be bulk string
    try testing.expect(std.mem.indexOf(u8, resp, "$2\r\n20") == null);
}

// ─── Without WITHSCORE — unchanged behavior ───────────────────────────────────

test "iter393 - ZRANK without WITHSCORE RESP3 still returns integer" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9805");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "5", "x" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANK", "zs", "x" });
    defer allocator.free(resp);

    // No WITHSCORE: just integer :0\r\n
    try testing.expectEqualStrings(":0\r\n", resp);
}

test "iter393 - ZREVRANK without WITHSCORE RESP3 still returns integer" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9806");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "5", "x" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "10", "y" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    // x has score 5, lower than y=10, so in descending order x is rank 1
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANK", "zs", "x" });
    defer allocator.free(resp);

    try testing.expectEqualStrings(":1\r\n", resp);
}

// ─── Non-existent member — null reply unchanged ───────────────────────────────

test "iter393 - ZRANK WITHSCORE RESP3 non-existent member returns null" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9807");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANK", "nokey", "nomember", "WITHSCORE" });
    defer allocator.free(resp);

    // Null reply
    try testing.expectEqualStrings("$-1\r\n", resp);
}

test "iter393 - ZREVRANK WITHSCORE RESP3 non-existent member returns null" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9808");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANK", "nokey", "nomember", "WITHSCORE" });
    defer allocator.free(resp);

    // Null reply
    try testing.expectEqualStrings("$-1\r\n", resp);
}

// ─── Floating point score precision ──────────────────────────────────────────

test "iter393 - ZRANK WITHSCORE RESP3 fractional score precision" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9809");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "3.14", "pi" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANK", "zs", "pi", "WITHSCORE" });
    defer allocator.free(resp);

    // Score as double: ,3.14\r\n
    try testing.expect(std.mem.indexOf(u8, resp, ",3.14\r\n") != null);
}

test "iter393 - ZREVRANK WITHSCORE RESP3 second rank" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9810");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "1", "a" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "2", "b" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zs", "3", "c" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    // b has score 2, in descending order it's rank 1
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANK", "zs", "b", "WITHSCORE" });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, ":1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",2\r\n") != null);
}
