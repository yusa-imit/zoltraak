// Iteration 358: Subscription Mode Enforcement + PING in Subscription Mode
//
// Two Redis compatibility improvements:
// 1. When a client is in subscription mode (active pub/sub subscriptions), only
//    SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/SSUBSCRIBE/SUNSUBSCRIBE/PING/RESET/QUIT
//    are allowed. Other commands return: "ERR Can't call '<cmd>' in subscription mode".
// 2. PING in subscription mode returns a 3-element array ["pong", ""] or ["pong", message]
//    instead of the simple "+PONG" response — matching Redis behavior.
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

fn execCmdSub(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    subscriber_id: u64,
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
        subscriber_id,
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

test "iter358 - GET blocked in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7001", 1, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe to a channel (subscriber_id=1)
    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 1, &.{ "SUBSCRIBE", "channel1" });
    defer allocator.free(sub_result);
    try testing.expect(std.mem.startsWith(u8, sub_result, "*3\r\n"));

    // GET should be blocked
    const get_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 1, &.{ "GET", "mykey" });
    defer allocator.free(get_result);
    try testing.expect(std.mem.indexOf(u8, get_result, "ERR Can't call 'GET' in subscription mode") != null);
}

test "iter358 - SET blocked in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7002", 2, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 2, &.{ "SUBSCRIBE", "chan" });
    defer allocator.free(sub_result);

    const set_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 2, &.{ "SET", "key", "value" });
    defer allocator.free(set_result);
    try testing.expect(std.mem.indexOf(u8, set_result, "ERR Can't call 'SET' in subscription mode") != null);
}

test "iter358 - HSET blocked in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7003", 3, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 3, &.{ "PSUBSCRIBE", "pat*" });
    defer allocator.free(sub_result);

    const hset_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 3, &.{ "HSET", "myhash", "field", "val" });
    defer allocator.free(hset_result);
    try testing.expect(std.mem.indexOf(u8, hset_result, "ERR Can't call 'HSET' in subscription mode") != null);
}

test "iter358 - SUBSCRIBE allowed in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7004", 4, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // First subscribe
    const sub1 = try execCmdSub(allocator, storage, &registry, client_id, &ps, 4, &.{ "SUBSCRIBE", "chan1" });
    defer allocator.free(sub1);
    try testing.expect(std.mem.startsWith(u8, sub1, "*3\r\n"));

    // SUBSCRIBE again (to another channel) should work
    const sub2 = try execCmdSub(allocator, storage, &registry, client_id, &ps, 4, &.{ "SUBSCRIBE", "chan2" });
    defer allocator.free(sub2);
    try testing.expect(std.mem.startsWith(u8, sub2, "*3\r\n"));
    try testing.expect(std.mem.indexOf(u8, sub2, "chan2") != null);
}

test "iter358 - UNSUBSCRIBE allowed in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7005", 5, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 5, &.{ "SUBSCRIBE", "chan1" });
    defer allocator.free(sub_result);

    // UNSUBSCRIBE should work
    const unsub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 5, &.{ "UNSUBSCRIBE", "chan1" });
    defer allocator.free(unsub_result);
    try testing.expect(std.mem.startsWith(u8, unsub_result, "*3\r\n"));
    try testing.expect(std.mem.indexOf(u8, unsub_result, "unsubscribe") != null);
}

test "iter358 - PING returns 3-element array in subscription mode RESP2" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7006", 6, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe first (subscriber_id=6)
    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 6, &.{ "SUBSCRIBE", "chan" });
    defer allocator.free(sub_result);

    // PING in subscription mode: returns *3\r\n$4\r\npong\r\n$0\r\n\r\n
    const ping_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 6, &.{"PING"});
    defer allocator.free(ping_result);
    try testing.expectEqualStrings("*3\r\n$4\r\npong\r\n$0\r\n\r\n", ping_result);
}

test "iter358 - PING with message returns 3-element array in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7007", 7, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 7, &.{ "SUBSCRIBE", "chan" });
    defer allocator.free(sub_result);

    // PING "hello" in subscription mode: returns *3\r\n$4\r\npong\r\n$5\r\nhello\r\n
    const ping_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 7, &.{ "PING", "hello" });
    defer allocator.free(ping_result);
    try testing.expectEqualStrings("*3\r\n$4\r\npong\r\n$5\r\nhello\r\n", ping_result);
}

test "iter358 - PING returns +PONG when NOT in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7008", 8, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // No subscriptions — PING should return +PONG
    const ping_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 8, &.{"PING"});
    defer allocator.free(ping_result);
    try testing.expectEqualStrings("+PONG\r\n", ping_result);
}

test "iter358 - commands unblocked after UNSUBSCRIBE all channels" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7009", 9, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe then unsubscribe
    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 9, &.{ "SUBSCRIBE", "chan" });
    defer allocator.free(sub_result);

    const unsub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 9, &.{ "UNSUBSCRIBE", "chan" });
    defer allocator.free(unsub_result);

    // After unsubscribing, normal commands should work again
    const set_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 9, &.{ "SET", "key", "value" });
    defer allocator.free(set_result);
    try testing.expectEqualStrings("+OK\r\n", set_result);
}

test "iter358 - QUIT allowed in subscription mode" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7010", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const sub_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 10, &.{ "SUBSCRIBE", "chan" });
    defer allocator.free(sub_result);

    const quit_result = try execCmdSub(allocator, storage, &registry, client_id, &ps, 10, &.{"QUIT"});
    defer allocator.free(quit_result);
    try testing.expectEqualStrings("+OK\r\n", quit_result);
}
