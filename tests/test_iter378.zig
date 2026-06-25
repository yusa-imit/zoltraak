// Iteration 378: RESP3 set type for SPOP/SRANDMEMBER and map type for
// ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANDMEMBER WITHSCORES
//
// In RESP2, these commands return flat arrays. In RESP3:
//   SPOP key count             → ~N\r\n... (set — popped elements are unique)
//   SRANDMEMBER key +count     → ~N\r\n... (set — positive count yields unique elements)
//   SRANDMEMBER key -count     → *N\r\n... (array — negative count may repeat)
//   ZRANGEBYSCORE WITHSCORES   → %N\r\n... (map — member → score)
//   ZREVRANGEBYSCORE WITHSCORES→ %N\r\n... (map — member → score)
//   ZRANDMEMBER +count WSCORES → %N\r\n... (map — positive count: unique members)
//   ZRANDMEMBER -count WSCORES → *N\r\n... (array — negative count may repeat)

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

// ─── SPOP RESP3 set type ──────────────────────────────────────────────────────

test "iter378 - SPOP count RESP2 returns array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9501");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sp1", "a", "b", "c" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "sp1", "2" });
    defer allocator.free(result);

    // RESP2: array *2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "iter378 - SPOP count RESP3 returns set type" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9502");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sp2", "a", "b", "c" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "sp2", "2" });
    defer allocator.free(result);

    // RESP3: set ~2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "~2\r\n"));
}

test "iter378 - SPOP without count RESP3 still returns bulk string" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9503");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sp3", "x" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "sp3" });
    defer allocator.free(result);

    // Single-pop always returns bulk string regardless of protocol
    try testing.expect(std.mem.startsWith(u8, result, "$"));
    try testing.expect(std.mem.indexOf(u8, result, "x") != null);
}

test "iter378 - SPOP count 0 RESP3 returns empty set" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9504");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sp4", "a", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "sp4", "0" });
    defer allocator.free(result);

    // count=0 → empty array (not set) for RESP2 compat; checked in test_iter356
    // In RESP3, count=0 returns empty array *0\r\n (early-return before RESP3 check)
    try testing.expectEqualStrings("*0\r\n", result);
}

// ─── SRANDMEMBER RESP3 set type ───────────────────────────────────────────────

test "iter378 - SRANDMEMBER positive count RESP2 returns array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9505");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sr1", "a", "b", "c" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SRANDMEMBER", "sr1", "2" });
    defer allocator.free(result);

    // RESP2: array *2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "iter378 - SRANDMEMBER positive count RESP3 returns set type" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9506");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sr2", "a", "b", "c" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SRANDMEMBER", "sr2", "2" });
    defer allocator.free(result);

    // RESP3 positive count: set ~2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "~2\r\n"));
}

test "iter378 - SRANDMEMBER negative count RESP3 returns array (may repeat)" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9507");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sr3", "a", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SRANDMEMBER", "sr3", "-3" });
    defer allocator.free(result);

    // Negative count: elements may repeat → always array in RESP3 too
    try testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
}

test "iter378 - SRANDMEMBER no count RESP3 returns bulk string" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9508");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "sr4", "only" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SRANDMEMBER", "sr4" });
    defer allocator.free(result);

    // No count: single member as bulk string
    try testing.expect(std.mem.startsWith(u8, result, "$"));
}

// ─── ZRANGEBYSCORE RESP3 map type ────────────────────────────────────────────

test "iter378 - ZRANGEBYSCORE WITHSCORES RESP2 returns flat array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9509");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zbs1", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANGEBYSCORE", "zbs1", "-inf", "+inf", "WITHSCORES" });
    defer allocator.free(result);

    // RESP2: flat array *4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
}

test "iter378 - ZRANGEBYSCORE WITHSCORES RESP3 returns map" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9510");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zbs2", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANGEBYSCORE", "zbs2", "-inf", "+inf", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n... member→score pairs
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\na\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\nb\r\n") != null);
}

test "iter378 - ZRANGEBYSCORE no WITHSCORES RESP3 returns array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9511");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zbs3", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANGEBYSCORE", "zbs3", "-inf", "+inf" });
    defer allocator.free(result);

    // Without WITHSCORES: always array
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

// ─── ZREVRANGEBYSCORE RESP3 map type ─────────────────────────────────────────

test "iter378 - ZREVRANGEBYSCORE WITHSCORES RESP2 returns flat array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9512");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zrbs1", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZREVRANGEBYSCORE", "zrbs1", "+inf", "-inf", "WITHSCORES" });
    defer allocator.free(result);

    // RESP2: flat array *4\r\n (b first since descending)
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "b") != null);
}

test "iter378 - ZREVRANGEBYSCORE WITHSCORES RESP3 returns map" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9513");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zrbs2", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZREVRANGEBYSCORE", "zrbs2", "+inf", "-inf", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3: map %2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\na\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$1\r\nb\r\n") != null);
}

// ─── ZRANDMEMBER RESP3 map type ───────────────────────────────────────────────

test "iter378 - ZRANDMEMBER positive count WITHSCORES RESP2 returns flat array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9514");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zrm1", "1", "a", "2", "b", "3", "c" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANDMEMBER", "zrm1", "2", "WITHSCORES" });
    defer allocator.free(result);

    // RESP2: flat array *4\r\n (2 pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
}

test "iter378 - ZRANDMEMBER positive count WITHSCORES RESP3 returns map" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9515");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zrm2", "1", "a", "2", "b", "3", "c" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANDMEMBER", "zrm2", "2", "WITHSCORES" });
    defer allocator.free(result);

    // RESP3 positive count with WITHSCORES: map %2\r\n...
    try testing.expect(std.mem.startsWith(u8, result, "%2\r\n"));
}

test "iter378 - ZRANDMEMBER negative count WITHSCORES RESP3 returns flat array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9516");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zrm3", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANDMEMBER", "zrm3", "-3", "WITHSCORES" });
    defer allocator.free(result);

    // Negative count: elements may repeat → always array even in RESP3
    try testing.expect(std.mem.startsWith(u8, result, "*6\r\n"));
}

test "iter378 - ZRANDMEMBER positive count no WITHSCORES RESP3 returns array" {
    const allocator = testing.allocator;
    var s = try setup(allocator, "127.0.0.1:9517");
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    s.registry.setProtocol(s.client_id, .RESP3);
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "zrm4", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZRANDMEMBER", "zrm4", "2" });
    defer allocator.free(result);

    // Without WITHSCORES: array regardless of protocol
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}
