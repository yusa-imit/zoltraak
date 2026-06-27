// Iteration 381: sailor v2.61.0 + RESP3 map type for HSCAN + RESP3 set type for SSCAN
//
// In RESP2, HSCAN returns [cursor, [field, value, field, value, ...]] (flat array)
// In RESP3, HSCAN returns [cursor, %N map{field: value, ...}]
//
// In RESP2, HSCAN NOVALUES returns [cursor, [field, field, ...]] (array)
// In RESP3, HSCAN NOVALUES returns [cursor, ~N set{field, ...}]
//
// In RESP2, SSCAN returns [cursor, [member, member, ...]] (array)
// In RESP3, SSCAN returns [cursor, ~N set{member, ...}]

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

// ─── HSCAN RESP2 ────────────────────────────────────────────────────────────

test "iter381 - HSCAN RESP2 returns flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7001");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "myhash", "field1", "val1", "field2", "val2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSCAN", "myhash", "0" });
    defer allocator.free(result);

    // RESP2: outer *2, second element *4 (flat array of 4 items: f1,v1,f2,v2)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must contain a flat array for fields+values (not a map)
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") != null);
    // Must NOT contain map header %2\r\n in RESP2
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "val1") != null);
}

// ─── HSCAN RESP3 ────────────────────────────────────────────────────────────

test "iter381 - HSCAN RESP3 returns map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7002");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "myhash", "field1", "val1", "field2", "val2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSCAN", "myhash", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %2 (map of 2 field-value pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must contain map header %2\r\n for 2 field-value pairs
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") != null);
    // Must NOT contain flat array *4\r\n in RESP3 for fields+values
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "val2") != null);
}

// ─── HSCAN RESP3 empty hash ──────────────────────────────────────────────────

test "iter381 - HSCAN RESP3 empty hash returns empty map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7003");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSCAN", "nonexistent", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %0 (empty map)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%0\r\n") != null);
}

// ─── HSCAN RESP3 NOVALUES ───────────────────────────────────────────────────

test "iter381 - HSCAN RESP3 NOVALUES returns set of fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7004");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "myhash", "field1", "val1", "field2", "val2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSCAN", "myhash", "0", "NOVALUES" });
    defer allocator.free(result);

    // RESP3 + NOVALUES: outer *2, second element ~2 (set of 2 field names)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must contain set header ~2\r\n for 2 fields
    try testing.expect(std.mem.indexOf(u8, result, "~2\r\n") != null);
    // Must NOT contain map or flat array
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "field1") != null);
    // Values must NOT appear (NOVALUES)
    try testing.expect(std.mem.indexOf(u8, result, "val1") == null);
}

// ─── SSCAN RESP2 ────────────────────────────────────────────────────────────

test "iter381 - SSCAN RESP2 returns array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7005");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SADD", "myset", "member1", "member2", "member3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SSCAN", "myset", "0" });
    defer allocator.free(result);

    // RESP2: outer *2, second element *3 (array of 3 members)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must contain an array for members (not a set)
    try testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
    // Must NOT contain set header ~3\r\n in RESP2
    try testing.expect(std.mem.indexOf(u8, result, "~3\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "member1") != null);
}

// ─── SSCAN RESP3 ────────────────────────────────────────────────────────────

test "iter381 - SSCAN RESP3 returns set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7006");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SADD", "myset", "member1", "member2", "member3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SSCAN", "myset", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element ~3 (set of 3 members)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must contain set header ~3\r\n for 3 members
    try testing.expect(std.mem.indexOf(u8, result, "~3\r\n") != null);
    // Must NOT contain flat array *3\r\n in RESP3
    try testing.expect(std.mem.indexOf(u8, result, "*3\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "member1") != null);
}

// ─── SSCAN RESP3 empty set ──────────────────────────────────────────────────

test "iter381 - SSCAN RESP3 empty set returns empty set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7007");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SSCAN", "nonexistent", "0" });
    defer allocator.free(result);

    // RESP3: outer *2, second element ~0 (empty set)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "~0\r\n") != null);
}

// ─── HSCAN RESP2 NOVALUES ───────────────────────────────────────────────────

test "iter381 - HSCAN RESP2 NOVALUES still returns flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7008");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "myhash", "field1", "val1", "field2", "val2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSCAN", "myhash", "0", "NOVALUES" });
    defer allocator.free(result);

    // RESP2 + NOVALUES: outer *2, second element *2 (array of 2 field names)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Must NOT contain set or map
    try testing.expect(std.mem.indexOf(u8, result, "~2\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    // Values must NOT appear
    try testing.expect(std.mem.indexOf(u8, result, "val1") == null);
}

// ─── HSCAN RESP3 with MATCH filter ──────────────────────────────────────────

test "iter381 - HSCAN RESP3 MATCH returns filtered map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7009");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSET", "myhash", "foo1", "val1", "foo2", "val2", "bar1", "val3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HSCAN", "myhash", "0", "MATCH", "foo*" });
    defer allocator.free(result);

    // RESP3: outer *2, second element %2 (map with 2 foo* pairs)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") != null);
    // foo fields and values present
    try testing.expect(std.mem.indexOf(u8, result, "foo1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "val1") != null);
    // bar field filtered out
    try testing.expect(std.mem.indexOf(u8, result, "bar1") == null);
}

// ─── SSCAN RESP3 with MATCH filter ──────────────────────────────────────────

test "iter381 - SSCAN RESP3 MATCH returns filtered set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7010");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    defer allocator.free(hello);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SADD", "myset", "apple", "apricot", "banana" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SSCAN", "myset", "0", "MATCH", "ap*" });
    defer allocator.free(result);

    // RESP3: outer *2, second element ~2 (set with 2 ap* members)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "~2\r\n") != null);
    // ap* members present
    try testing.expect(std.mem.indexOf(u8, result, "apple") != null);
    try testing.expect(std.mem.indexOf(u8, result, "apricot") != null);
    // banana filtered out
    try testing.expect(std.mem.indexOf(u8, result, "banana") == null);
}
