// Iteration 359: Add 15 missing commands to ALL_COMMANDS + sailor v2.43.0
//
// Commands implemented but missing from ALL_COMMANDS (COMMAND INFO returned nil):
//   Geo: georadius_ro, georadiusbymember_ro
//   Hash field TTL (Redis 7.4+): hexpire, hpexpire, hexpireat, hpexpireat,
//     hpersist, httl, hpttl, hexpiretime, hpexpiretime
//   Hash: hstrlen
//   Sorted set: zremrangebyrank, zremrangebyscore, zremrangebylex
//
// ALL_COMMANDS grew from 231 to 246 entries.
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
    for (args, 0..) |a, i| {
        resp_args[i] = .{ .bulk_string = a };
    }
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

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9200", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// COMMAND INFO tests — verify COMMAND INFO returns non-nil for newly added commands

test "iter359 - COMMAND INFO georadius_ro returns non-nil entry" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "georadius_ro" });
    defer allocator.free(result);
    // Should return *1\r\n (array of 1 entry), not *1\r\n$-1\r\n (nil entry)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "georadius_ro") != null);
}

test "iter359 - COMMAND INFO georadiusbymember_ro returns non-nil entry" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "georadiusbymember_ro" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "georadiusbymember_ro") != null);
}

test "iter359 - COMMAND INFO hstrlen returns non-nil entry" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "hstrlen" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "hstrlen") != null);
}

test "iter359 - COMMAND INFO hexpire returns non-nil entry" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "hexpire" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "hexpire") != null);
}

test "iter359 - COMMAND INFO hpersist returns non-nil entry" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "hpersist" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "hpersist") != null);
}

test "iter359 - COMMAND INFO httl and hpttl return non-nil entries" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const httl_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "httl" });
    defer allocator.free(httl_result);
    try testing.expect(std.mem.startsWith(u8, httl_result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, httl_result, "httl") != null);

    const hpttl_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "hpttl" });
    defer allocator.free(hpttl_result);
    try testing.expect(std.mem.startsWith(u8, hpttl_result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, hpttl_result, "hpttl") != null);
}

test "iter359 - COMMAND INFO zremrangebyrank/score/lex return non-nil entries" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const rank = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "zremrangebyrank" });
    defer allocator.free(rank);
    try testing.expect(std.mem.startsWith(u8, rank, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, rank, "zremrangebyrank") != null);

    const score = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "zremrangebyscore" });
    defer allocator.free(score);
    try testing.expect(std.mem.startsWith(u8, score, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, score, "zremrangebyscore") != null);

    const lex = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "zremrangebylex" });
    defer allocator.free(lex);
    try testing.expect(std.mem.startsWith(u8, lex, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, lex, "zremrangebylex") != null);
}

test "iter359 - COMMAND COUNT increased to 269" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{"COMMAND", "COUNT"});
    defer allocator.free(result);
    try testing.expectEqualStrings(":269\r\n", result);
}

test "iter359 - ZREMRANGEBYRANK actually removes elements" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // Add 5 members with scores 1-5
    const zadd1 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zset1", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e" });
    defer allocator.free(zadd1);

    // ZREMRANGEBYRANK removes rank 0..1 (removes 2 lowest-score elements)
    const removed = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZREMRANGEBYRANK", "zset1", "0", "1" });
    defer allocator.free(removed);
    try testing.expectEqualStrings(":2\r\n", removed);

    // Verify 3 remain
    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZCARD", "zset1" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":3\r\n", card);
}

test "iter359 - ZREMRANGEBYSCORE actually removes elements" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const zadd2 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zset2", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e" });
    defer allocator.free(zadd2);

    // Remove scores between 2 and 4 inclusive
    const removed = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZREMRANGEBYSCORE", "zset2", "2", "4" });
    defer allocator.free(removed);
    try testing.expectEqualStrings(":3\r\n", removed);

    // Only score 1 (a) and 5 (e) remain
    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZCARD", "zset2" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":2\r\n", card);
}

test "iter359 - ZREMRANGEBYLEX actually removes elements" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // All same score (lex ordering)
    const zadd3 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zset3", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e" });
    defer allocator.free(zadd3);

    // Remove [b, d] lex range
    const removed = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZREMRANGEBYLEX", "zset3", "[b", "[d" });
    defer allocator.free(removed);
    try testing.expectEqualStrings(":3\r\n", removed);

    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZCARD", "zset3" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":2\r\n", card);
}

test "iter359 - HSTRLEN returns field value length" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const hset_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HSET", "myhash", "name", "Redis", "empty", "" });
    defer allocator.free(hset_result);

    const len_name = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HSTRLEN", "myhash", "name" });
    defer allocator.free(len_name);
    try testing.expectEqualStrings(":5\r\n", len_name);

    const len_empty = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HSTRLEN", "myhash", "empty" });
    defer allocator.free(len_empty);
    try testing.expectEqualStrings(":0\r\n", len_empty);

    // Nonexistent field returns 0
    const len_missing = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HSTRLEN", "myhash", "nosuchfield" });
    defer allocator.free(len_missing);
    try testing.expectEqualStrings(":0\r\n", len_missing);
}
