// Iteration 390: sailor v2.69.0 + RESP3 native double type for WITHSCORES maps
//
// In RESP2, sorted set WITHSCORES commands return flat arrays where scores are bulk strings.
// In RESP3, WITHSCORES maps should use native double type (,val\r\n) for score values.
//
// Affected commands: ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE,
//                    ZRANDMEMBER, ZUNION, ZINTER, ZDIFF

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

// ─── ZRANGE WITHSCORES ────────────────────────────────────────────────────────

test "iter390 - ZRANGE WITHSCORES RESP2 score is bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9600");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zr390a", "1", "alpha", "2", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANGE", "zr390a", "0", "-1", "WITHSCORES" });
    defer allocator.free(result);

    // RESP2: flat array *4\r\n with bulk string scores
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") == null);
}

test "iter390 - ZRANGE WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9601");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zr390b", "1", "alpha", "2", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANGE", "zr390b", "0", "-1", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n with double scores ,1\r\n and ,2\r\n
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",2\r\n") != null);
    // No bulk string scores
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") == null);
}

test "iter390 - ZRANGE BYSCORE WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9602");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zr390c", "1.5", "alpha", "2.5", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANGE", "zr390c", "-inf", "+inf", "BYSCORE", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map with double scores
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1.5\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",2.5\r\n") != null);
}

// ─── ZREVRANGE WITHSCORES ─────────────────────────────────────────────────────

test "iter390 - ZREVRANGE WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9603");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zrv390a", "1", "alpha", "2", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANGE", "zrv390a", "0", "-1", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n with double scores
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") == null);
}

// ─── ZRANGEBYSCORE WITHSCORES ─────────────────────────────────────────────────

test "iter390 - ZRANGEBYSCORE WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9604");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zbs390a", "3", "alpha", "5.5", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANGEBYSCORE", "zbs390a", "-inf", "+inf", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n with double scores ,3\r\n and ,5.5\r\n
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",5.5\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n3\r\n") == null);
}

// ─── ZREVRANGEBYSCORE WITHSCORES ──────────────────────────────────────────────

test "iter390 - ZREVRANGEBYSCORE WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9605");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zrbs390a", "1", "alpha", "2", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZREVRANGEBYSCORE", "zrbs390a", "+inf", "-inf", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n with double scores
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",2\r\n") != null);
}

// ─── ZRANDMEMBER WITHSCORES ───────────────────────────────────────────────────

test "iter390 - ZRANDMEMBER WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9606");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zrm390a", "1.5", "alpha" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANDMEMBER", "zrm390a", "1", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3 positive count + WITHSCORES: map %1\r\n with double score
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1.5\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$3\r\n1.5\r\n") == null);
}

// ─── ZUNION WITHSCORES ────────────────────────────────────────────────────────

test "iter390 - ZUNION WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9607");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zu390a", "1", "alpha" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zu390b", "2", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZUNION", "2", "zu390a", "zu390b", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3 WITHSCORES: map %2\r\n with double scores
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") == null);
}

// ─── ZINTER WITHSCORES ────────────────────────────────────────────────────────

test "iter390 - ZINTER WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9608");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zi390a", "1", "alpha", "2", "beta" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zi390b", "3", "alpha" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZINTER", "2", "zi390a", "zi390b", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3 WITHSCORES: map %1\r\n (only "alpha" is in both sets, sum=4)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",4\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n4\r\n") == null);
}

// ─── ZDIFF WITHSCORES ─────────────────────────────────────────────────────────

test "iter390 - ZDIFF WITHSCORES RESP3 score is double" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9609");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zd390a", "1", "alpha", "2", "beta" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zd390b", "3", "beta" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZDIFF", "2", "zd390a", "zd390b", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3 WITHSCORES: map %1\r\n (only "alpha" in diff, score=1)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") == null);
}

// ─── inf scores in RESP3 ──────────────────────────────────────────────────────

test "iter390 - ZRANGE WITHSCORES RESP3 handles +inf score as double inf" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9610");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zinf390", "+inf", "alpha" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZRANGE", "zinf390", "0", "-1", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map with ,inf\r\n for positive infinity
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, ",inf\r\n") != null);
}

// ─── RESP2 regression: scores still bulk strings ──────────────────────────────

test "iter390 - ZUNION WITHSCORES RESP2 scores still bulk strings (regression)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9611");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZADD", "zu390r", "1", "alpha" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ZUNION", "1", "zu390r", "WITHSCORES" });
    defer allocator.free(result);

    // RESP2: flat array *2\r\n with bulk string score
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, ",1\r\n") == null);
}
