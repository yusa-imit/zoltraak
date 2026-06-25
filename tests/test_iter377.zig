// Iteration 377: sailor v2.59.0 migration + RESP3 double type for INCRBYFLOAT and GEODIST
//
// Two improvements:
// 1. Sailor v2.59.0 migration (StopWatch Widget — no breaking changes).
// 2. RESP3 double type for INCRBYFLOAT and GEODIST. In RESP2 these return bulk
//    strings; in RESP3 they now return the RESP3 double type (,val\r\n). This
//    matches Redis 7.0+ behavior and fixes compatibility with strict RESP3 clients.

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

// ─── INCRBYFLOAT RESP3 double type ───────────────────────────────────────────

test "iter377 - INCRBYFLOAT RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9401", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "ibf377a", "10.5" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "ibf377a", "0.1" });
    defer allocator.free(result);

    // RESP2: bulk string "$4\r\n10.6\r\n"
    try testing.expectEqualStrings("$4\r\n10.6\r\n", result);
}

test "iter377 - INCRBYFLOAT RESP3 returns double type" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9402", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "ibf377b", "10.5" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "ibf377b", "0.1" });
    defer allocator.free(result);

    // RESP3: double ",10.6\r\n"
    try testing.expectEqualStrings(",10.6\r\n", result);
}

test "iter377 - INCRBYFLOAT RESP3 whole number result" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9403", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "ibf377c", "10" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "ibf377c", "5" });
    defer allocator.free(result);

    // RESP3: double ",15\r\n"
    try testing.expectEqualStrings(",15\r\n", result);
}

test "iter377 - INCRBYFLOAT RESP3 new key starts at increment" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9404", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "ibf377d_new", "3.14" });
    defer allocator.free(result);

    // RESP3: double ",3.14\r\n"
    try testing.expectEqualStrings(",3.14\r\n", result);
}

test "iter377 - INCRBYFLOAT RESP3 negative increment" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9405", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "ibf377e", "5.0" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "ibf377e", "-1.5" });
    defer allocator.free(result);

    // RESP3: double ",3.5\r\n"
    try testing.expectEqualStrings(",3.5\r\n", result);
}

test "iter377 - INCRBYFLOAT RESP2 error still returns error string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9406", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "SET", "ibf377f", "notanumber" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "INCRBYFLOAT", "ibf377f", "1.0" });
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

// ─── GEODIST RESP3 double type ────────────────────────────────────────────────

test "iter377 - GEODIST RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9407", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEOADD", "geo377a", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEODIST", "geo377a", "Palermo", "Catania", "km" });
    defer allocator.free(result);

    // RESP2: bulk string starting with "$"
    try testing.expect(std.mem.startsWith(u8, result, "$"));
    // Distance should be approximately 166.2742 km
    try testing.expect(std.mem.indexOf(u8, result, "166") != null);
}

test "iter377 - GEODIST RESP3 returns double type" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9408", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEOADD", "geo377b", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEODIST", "geo377b", "Palermo", "Catania", "km" });
    defer allocator.free(result);

    // RESP3: double starting with ","
    try testing.expect(std.mem.startsWith(u8, result, ","));
    // Distance should be approximately 166.2742 km
    try testing.expect(std.mem.indexOf(u8, result, "166") != null);
}

test "iter377 - GEODIST RESP3 nonexistent member returns null" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9409", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEOADD", "geo377c", "13.361389", "38.115556", "Palermo" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEODIST", "geo377c", "Palermo", "NonExistent" });
    defer allocator.free(result);

    // Nonexistent member returns null even in RESP3
    try testing.expectEqualStrings("$-1\r\n", result);
}

test "iter377 - GEODIST RESP2 default meters" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9410", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEOADD", "geo377d", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEODIST", "geo377d", "Palermo", "Catania" });
    defer allocator.free(result);

    // RESP2 default unit is meters, bulk string with numeric value
    try testing.expect(std.mem.startsWith(u8, result, "$"));
    try testing.expect(!std.mem.startsWith(u8, result, "-"));
}

test "iter377 - GEODIST RESP3 meters unit" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9411", 10, "127.0.0.1:6379");
    registry.setProtocol(client_id, .RESP3);
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEOADD", "geo377e", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GEODIST", "geo377e", "Palermo", "Catania", "m" });
    defer allocator.free(result);

    // RESP3: double starting with "," with numeric value (meters)
    try testing.expect(std.mem.startsWith(u8, result, ","));
    try testing.expect(!std.mem.startsWith(u8, result, ",inf"));
    try testing.expect(!std.mem.startsWith(u8, result, ",-inf"));
}
