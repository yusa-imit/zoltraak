// Iteration 366: DEBUG OBJECT output format fix
//
// Fixed DEBUG OBJECT to return proper Redis-compatible format:
//   - "Value at:0x0" (hex address) instead of "Value at:{type_name}"
//   - Actual encoding (embstr/int/raw for strings; intset/hashtable for sets; etc.)
//   - type: field at end (Redis 7.4+ format)
//   - lru_seconds_idle: from actual idle time tracking
//
// Previously "Value at:string" was used (type name as address), which is invalid.

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

test "iter366 - DEBUG OBJECT uses hex address format (not type name)" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SET", "mykey", "hello" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "mykey" });
    defer allocator.free(result);

    // Must use hex address format, NOT "Value at:string"
    try testing.expect(std.mem.indexOf(u8, result, "Value at:0x0") != null);
    try testing.expect(std.mem.indexOf(u8, result, "Value at:string") == null);
}

test "iter366 - DEBUG OBJECT includes type: field" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SET", "strkey", "world" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "strkey" });
    defer allocator.free(result);

    // Redis 7.4+ includes type: field at end
    try testing.expect(std.mem.indexOf(u8, result, "type:string") != null);
}

test "iter366 - DEBUG OBJECT reports embstr for short strings" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // Short string (<=44 bytes) → embstr encoding
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SET", "shortkey", "hi" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "shortkey" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "encoding:embstr") != null);
}

test "iter366 - DEBUG OBJECT reports int for integer strings" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SET", "intkey", "12345" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "intkey" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "encoding:int") != null);
    try testing.expect(std.mem.indexOf(u8, result, "type:string") != null);
}

test "iter366 - DEBUG OBJECT reports raw for long strings" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // Long string (>44 bytes) → raw encoding
    const long_val = "a" ** 45;
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SET", "longkey", long_val }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "longkey" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "encoding:raw") != null);
}

test "iter366 - DEBUG OBJECT returns error for non-existent key" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "noexist" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try testing.expect(std.mem.indexOf(u8, result, "no such key") != null);
}

test "iter366 - DEBUG OBJECT list reports type:list" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "RPUSH", "mylist", "a", "b", "c" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "mylist" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "type:list") != null);
    try testing.expect(std.mem.indexOf(u8, result, "Value at:0x0") != null);
}

test "iter366 - DEBUG OBJECT hash reports type:hash and listpack encoding" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "HSET", "myhash", "f1", "v1" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "myhash" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "type:hash") != null);
    try testing.expect(std.mem.indexOf(u8, result, "encoding:listpack") != null);
}

test "iter366 - DEBUG OBJECT zset reports type:zset" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "ZADD", "myzset", "1.0", "member1" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "myzset" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "type:zset") != null);
    try testing.expect(std.mem.indexOf(u8, result, "Value at:0x0") != null);
}

test "iter366 - DEBUG OBJECT set with integer members reports intset" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // Small integer-only set → intset encoding
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "intset_key", "1", "2", "3" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "intset_key" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "type:set") != null);
    try testing.expect(std.mem.indexOf(u8, result, "encoding:intset") != null);
}

test "iter366 - DEBUG OBJECT includes lru_seconds_idle field" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SET", "idlekey", "value" }));
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "DEBUG", "OBJECT", "idlekey" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "lru_seconds_idle:") != null);
    try testing.expect(std.mem.indexOf(u8, result, "refcount:1") != null);
}
