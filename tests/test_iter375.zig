// Iteration 375: sailor v2.57.0 migration + Redis compatibility fixes
//
// Four bugs fixed:
// 1. INCRBYFLOAT: response was formatted with {d} which may not strip trailing
//    zeros. Now applies the same formatFloat logic as storage (strip trailing
//    zeros after decimal point and trailing dot).
// 2. HRANDFIELD key count WITHVALUES: in RESP3 mode, was returning a Map reply
//    instead of an Array reply. Redis always returns a flat array for HRANDFIELD
//    regardless of protocol version. Removed the RESP3 map branch.
// 3. ZUNION/ZINTER/ZDIFF/STORE variants: numkeys=0 was silently accepted,
//    returning an empty result instead of the Redis error
//    "ERR at least 1 input key is needed for '<cmd>' command".
// 4. ZRANGESTORE: WITHSCORES was silently accepted as a valid option but it is
//    not a valid option for ZRANGESTORE. Now returns ERR syntax error.

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

// ─── INCRBYFLOAT format fix ───────────────────────────────────────────────────

test "iter375 - INCRBYFLOAT whole number result has no trailing decimal" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9201", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // SET k 5000; INCRBYFLOAT k 200 => 5200 (not "5200.0")
    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "k375a", "5000" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "k375a", "200" });
    defer allocator.free(result);

    // Should be $4\r\n5200\r\n (not $6\r\n5200.0\r\n)
    try testing.expectEqualStrings("$4\r\n5200\r\n", result);
}

test "iter375 - INCRBYFLOAT decimal result strips trailing zeros" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9202", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "k375b", "10.50" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "k375b", "0.1" });
    defer allocator.free(result);

    // 10.50 + 0.1 = 10.6 (not "10.60")
    try testing.expectEqualStrings("$4\r\n10.6\r\n", result);
}

test "iter375 - INCRBYFLOAT non-integer increment preserves precision" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9203", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "k375c", "1.5" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "k375c", "1" });
    defer allocator.free(result);

    // 1.5 + 1 = 2.5
    try testing.expectEqualStrings("$3\r\n2.5\r\n", result);
}

// ─── HRANDFIELD WITHVALUES RESP3 fix ─────────────────────────────────────────

test "iter375 - HRANDFIELD WITHVALUES returns array not map" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9204", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HSET", "h375", "f1", "v1", "f2", "v2" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HRANDFIELD", "h375", "2", "WITHVALUES" });
    defer allocator.free(result);

    // Must start with '*' (array), not '%' (map)
    try testing.expect(result[0] == '*');
    // 2 fields + 2 values = 4 elements total
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
}

test "iter375 - HRANDFIELD WITHVALUES negative count returns array" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9205", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HSET", "h375b", "field", "value" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "HRANDFIELD", "h375b", "-3", "WITHVALUES" });
    defer allocator.free(result);

    // Negative count allows repeats; always returns array
    try testing.expect(result[0] == '*');
    // -3 means 3 random (with repetition); 3 pairs = 6 elements
    try testing.expect(std.mem.startsWith(u8, result, "*6\r\n"));
}

// ─── ZUNION/ZINTER/ZDIFF numkeys=0 fix ────────────────────────────────────────

test "iter375 - ZUNION numkeys=0 returns error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9206", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // ZUNION 0 dummy — numkeys=0 with a trailing word (passes arity check)
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZUNION", "0", "dummy" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "at least 1 input key") != null);
}

test "iter375 - ZINTER numkeys=0 returns error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9207", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZINTER", "0", "dummy" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "at least 1 input key") != null);
}

test "iter375 - ZDIFF numkeys=0 returns error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9208", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZDIFF", "0", "dummy" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "at least 1 input key") != null);
}

test "iter375 - ZUNIONSTORE numkeys=0 returns error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9209", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // ZUNIONSTORE requires at least 4 args; pass a trailing dummy to bypass arity
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZUNIONSTORE", "dst375", "0", "phantom" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "at least 1 input key") != null);
}

test "iter375 - ZINTERSTORE numkeys=0 returns error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9210", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZINTERSTORE", "dst375", "0", "phantom" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "at least 1 input key") != null);
}

test "iter375 - ZDIFFSTORE numkeys=0 returns error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9211", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZDIFFSTORE", "dst375", "0", "phantom" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "at least 1 input key") != null);
}

test "iter375 - ZUNION numkeys=1 valid still works" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9212", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zs375", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZUNION", "1", "zs375" });
    defer allocator.free(result);

    try testing.expect(result[0] == '*');
    try testing.expect(std.mem.indexOf(u8, result, "a") != null);
}

// ─── ZRANGESTORE WITHSCORES fix ──────────────────────────────────────────────

test "iter375 - ZRANGESTORE WITHSCORES returns syntax error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9213", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "src375", "1", "a", "2", "b" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZRANGESTORE", "dst375x", "src375", "0", "-1", "WITHSCORES" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "syntax error") != null);
}

test "iter375 - ZRANGESTORE without options works normally" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9214", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZADD", "zrs375src", "1", "a", "2", "b", "3", "c" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ZRANGESTORE", "zrs375dst", "zrs375src", "0", "1" });
    defer allocator.free(result);

    // Returns count of elements stored
    try testing.expectEqualStrings(":2\r\n", result);
}
