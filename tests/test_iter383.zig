// Iteration 383: sailor v2.63.0 + RESP3 set type for SCAN
//
// In RESP2, SCAN returns [cursor, [key1, key2, ...]] (array)
// In RESP3, SCAN returns [cursor, ~N set{key1, key2, ...}] (unordered set type)
// Keys are unique by definition, so the set type is semantically correct.

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

// ─── SCAN RESP2 ─────────────────────────────────────────────────────────────

test "iter383 - SCAN RESP2 returns plain array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8300");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "k1", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "k2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0" });
    defer allocator.free(result);

    // RESP2: outer *2, second element *N array (not ~N set)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null or
        std.mem.indexOf(u8, result, "*1\r\n") != null or
        std.mem.indexOf(u8, result, "*0\r\n") != null);
    // Must NOT use set type prefix
    try testing.expect(std.mem.indexOf(u8, result, "~") == null);
}

// ─── SCAN RESP3 returns set type ────────────────────────────────────────────

test "iter383 - SCAN RESP3 returns set type for keys" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8301");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "alpha", "1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "beta", "2" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "gamma", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0", "COUNT", "100" });
    defer allocator.free(result);

    // RESP3: outer *2, second element ~N set
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "~3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "alpha") != null);
    try testing.expect(std.mem.indexOf(u8, result, "beta") != null);
    try testing.expect(std.mem.indexOf(u8, result, "gamma") != null);
}

// ─── SCAN RESP3 empty keyspace ───────────────────────────────────────────────

test "iter383 - SCAN RESP3 empty keyspace returns ~0 set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8302");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0" });
    defer allocator.free(result);

    // RESP3: ~0 set for empty result
    try testing.expect(std.mem.indexOf(u8, result, "~0\r\n") != null);
}

// ─── SCAN RESP3 with MATCH pattern ───────────────────────────────────────────

test "iter383 - SCAN RESP3 MATCH pattern returns filtered set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8303");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "user:1", "a" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "user:2", "b" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "session:1", "c" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0", "MATCH", "user:*", "COUNT", "100" });
    defer allocator.free(result);

    // RESP3: ~2 set with only user:* keys
    try testing.expect(std.mem.indexOf(u8, result, "~2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "user:1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "user:2") != null);
    try testing.expect(std.mem.indexOf(u8, result, "session:1") == null);
}

// ─── SCAN RESP3 with TYPE filter ─────────────────────────────────────────────

test "iter383 - SCAN RESP3 TYPE filter returns set of matching type" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8304");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "str1", "hello" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "LPUSH", "list1", "item" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SADD", "set1", "member" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0", "TYPE", "string", "COUNT", "100" });
    defer allocator.free(result);

    // RESP3: only string-type keys in set
    try testing.expect(std.mem.indexOf(u8, result, "~1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "str1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "list1") == null);
    try testing.expect(std.mem.indexOf(u8, result, "set1") == null);
}

// ─── SCAN RESP3 single key ────────────────────────────────────────────────────

test "iter383 - SCAN RESP3 single key returns ~1 set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8305");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "only", "value" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0" });
    defer allocator.free(result);

    // RESP3: ~1 set with the single key
    try testing.expect(std.mem.indexOf(u8, result, "~1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "only") != null);
}

// ─── SCAN RESP2 cursor format unchanged ──────────────────────────────────────

test "iter383 - SCAN RESP2 cursor is still bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8306");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // RESP2 (no HELLO 3)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "mykey", "val" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0" });
    defer allocator.free(result);

    // RESP2 cursor is bulk string ($1\r\n0\r\n) not integer
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n$"));
    try testing.expect(!std.mem.startsWith(u8, result, "*2\r\n:"));
}

// ─── SCAN RESP3 cursor format ─────────────────────────────────────────────────

test "iter383 - SCAN RESP3 cursor is still bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8307");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "akey", "val" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, cursor as bulk string, then ~N set
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n$"));
    try testing.expect(std.mem.indexOf(u8, result, "~1\r\n") != null);
}

// ─── SCAN RESP3 COUNT 0 returns all as set ───────────────────────────────────

test "iter383 - SCAN RESP3 COUNT 0 returns all keys as set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8308");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "x", "1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "y", "2" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "z", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SCAN", "0", "COUNT", "0" });
    defer allocator.free(result);

    // COUNT 0 = return all → RESP3 ~3 set
    try testing.expect(std.mem.indexOf(u8, result, "~3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "x") != null);
    try testing.expect(std.mem.indexOf(u8, result, "y") != null);
    try testing.expect(std.mem.indexOf(u8, result, "z") != null);
}
