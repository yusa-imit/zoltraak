// Iteration 376: sailor v2.58.0 migration + RESP3 double type for score commands
//
// Two improvements:
// 1. Sailor v2.58.0 migration (SplitText Widget — no breaking changes).
// 2. RESP3 double type for score-returning commands: ZSCORE, ZINCRBY, ZADD INCR,
//    ZMSCORE, and HINCRBYFLOAT. In RESP2 these return bulk strings; in RESP3 they
//    now return the RESP3 double type (,val\r\n). This matches Redis 7.0+ behavior
//    and fixes compatibility with strict RESP3 clients (redis-py v5+, etc.).

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
const RespProtocol = zoltraak.client.RespProtocol;

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

// ─── ZSCORE RESP3 double type ─────────────────────────────────────────────────

test "iter376 - ZSCORE RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9301", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376a", "1.5", "member" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZSCORE", "zs376a", "member" });
    defer allocator.free(result);

    // RESP2: bulk string "$3\r\n1.5\r\n"
    try testing.expectEqualStrings("$3\r\n1.5\r\n", result);
}

test "iter376 - ZSCORE RESP3 returns double type" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9302", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376b", "1.5", "member" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZSCORE", "zs376b", "member" });
    defer allocator.free(result);

    // RESP3: double ",1.5\r\n"
    try testing.expectEqualStrings(",1.5\r\n", result);
}

test "iter376 - ZSCORE RESP3 non-existent member returns null" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9303", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZSCORE", "zs376c", "nonexistent" });
    defer allocator.free(result);

    // null remains $-1\r\n in both RESP2 and RESP3
    try testing.expectEqualStrings("$-1\r\n", result);
}

// ─── ZINCRBY RESP3 double type ────────────────────────────────────────────────

test "iter376 - ZINCRBY RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9304", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376d", "1", "member" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZINCRBY", "zs376d", "2", "member" });
    defer allocator.free(result);

    // RESP2: bulk string "$1\r\n3\r\n"
    try testing.expectEqualStrings("$1\r\n3\r\n", result);
}

test "iter376 - ZINCRBY RESP3 returns double type" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9305", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376e", "1", "member" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZINCRBY", "zs376e", "2", "member" });
    defer allocator.free(result);

    // RESP3: double ",3\r\n"
    try testing.expectEqualStrings(",3\r\n", result);
}

// ─── ZADD INCR RESP3 double type ─────────────────────────────────────────────

test "iter376 - ZADD INCR RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9306", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376f", "5", "m" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376f", "INCR", "3", "m" });
    defer allocator.free(result);

    // RESP2: bulk string "$1\r\n8\r\n"
    try testing.expectEqualStrings("$1\r\n8\r\n", result);
}

test "iter376 - ZADD INCR RESP3 returns double type" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9307", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376g", "5", "m" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376g", "INCR", "3", "m" });
    defer allocator.free(result);

    // RESP3: double ",8\r\n"
    try testing.expectEqualStrings(",8\r\n", result);
}

// ─── ZMSCORE RESP3 double type ────────────────────────────────────────────────

test "iter376 - ZMSCORE RESP2 returns bulk string array" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9308", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376h", "1.5", "m1", "2.5", "m2" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZMSCORE", "zs376h", "m1", "m2", "missing" });
    defer allocator.free(result);

    // RESP2: array of bulk strings and null
    try testing.expectEqualStrings("*3\r\n$3\r\n1.5\r\n$3\r\n2.5\r\n$-1\r\n", result);
}

test "iter376 - ZMSCORE RESP3 returns double type array" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9309", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs376i", "1.5", "m1", "2.5", "m2" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZMSCORE", "zs376i", "m1", "m2", "missing" });
    defer allocator.free(result);

    // RESP3: array of doubles and null
    try testing.expectEqualStrings("*3\r\n,1.5\r\n,2.5\r\n$-1\r\n", result);
}

// ─── HINCRBYFLOAT RESP3 double type ──────────────────────────────────────────

test "iter376 - HINCRBYFLOAT RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9310", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HSET", "h376a", "f", "10.5" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HINCRBYFLOAT", "h376a", "f", "0.1" });
    defer allocator.free(result);

    // RESP2: bulk string "$4\r\n10.6\r\n"
    try testing.expectEqualStrings("$4\r\n10.6\r\n", result);
}

test "iter376 - HINCRBYFLOAT RESP3 returns double type" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9311", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HSET", "h376b", "f", "10.5" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HINCRBYFLOAT", "h376b", "f", "0.1" });
    defer allocator.free(result);

    // RESP3: double ",10.6\r\n"
    try testing.expectEqualStrings(",10.6\r\n", result);
}

test "iter376 - HINCRBYFLOAT RESP3 whole number result" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9312", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HSET", "h376c", "counter", "10" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HINCRBYFLOAT", "h376c", "counter", "5" });
    defer allocator.free(result);

    // RESP3: double ",15\r\n"
    try testing.expectEqualStrings(",15\r\n", result);
}
