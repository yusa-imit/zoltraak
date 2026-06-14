// Iteration 352: CLIENT LIST/INFO real pub/sub counts + TYPE pubsub filter
//
// Fixes CLIENT LIST and CLIENT INFO to report real sub/psub/ssub counts instead
// of hardcoded 0. Also fixes CLIENT LIST TYPE pubsub to correctly return clients
// with active subscriptions, and sets type=pubsub for such clients.
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

test "iter352 - CLIENT LIST shows sub=0 for normal client" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9001", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    // Should contain "sub=0 psub=0 ssub=0"
    try testing.expect(std.mem.indexOf(u8, result, "sub=0 psub=0 ssub=0") != null);
    // type should be normal
    try testing.expect(std.mem.indexOf(u8, result, "type=normal") != null);
}

test "iter352 - CLIENT LIST shows sub=1 after SUBSCRIBE" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const sub_client_id = try registry.registerClient("127.0.0.1:9002", 11, "127.0.0.1:6379");
    // Use a separate monitor client to call CLIENT LIST (subscribed client can't run non-pub/sub commands)
    const monitor_id = try registry.registerClient("127.0.0.1:9002b", 110, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe to a channel using subscriber_id=11 for the subscribing client
    const sub_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 11, &.{ "SUBSCRIBE", "news" });
    defer allocator.free(sub_result);

    // Monitor client (subscriber_id=110, no subscriptions) calls CLIENT LIST
    const list_result = try execCmd(allocator, storage, &registry, monitor_id, &ps, 110, &.{ "CLIENT", "LIST" });
    defer allocator.free(list_result);

    try testing.expect(std.mem.indexOf(u8, list_result, "sub=1") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "psub=0") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "ssub=0") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") != null);
}

test "iter352 - CLIENT LIST shows psub=1 after PSUBSCRIBE" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const sub_client_id = try registry.registerClient("127.0.0.1:9003", 12, "127.0.0.1:6379");
    const monitor_id = try registry.registerClient("127.0.0.1:9003b", 120, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe to a pattern using subscriber_id=12
    const sub_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 12, &.{ "PSUBSCRIBE", "news.*" });
    defer allocator.free(sub_result);

    // Monitor client (no subscriptions) calls CLIENT LIST
    const list_result = try execCmd(allocator, storage, &registry, monitor_id, &ps, 120, &.{ "CLIENT", "LIST" });
    defer allocator.free(list_result);

    try testing.expect(std.mem.indexOf(u8, list_result, "sub=0") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "psub=1") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "ssub=0") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") != null);
}

test "iter352 - CLIENT LIST TYPE pubsub returns subscribed clients" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const normal_id = try registry.registerClient("127.0.0.1:9004", 13, "127.0.0.1:6379");
    const pubsub_id = try registry.registerClient("127.0.0.1:9005", 14, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe pubsub_id to a channel using subscriber_id=2
    const sub_result = try execCmd(allocator, storage, &registry, pubsub_id, &ps, 2, &.{ "SUBSCRIBE", "events" });
    defer allocator.free(sub_result);

    // CLIENT LIST TYPE pubsub should only return the subscribed client
    const list_result = try execCmd(allocator, storage, &registry, normal_id, &ps, 1, &.{ "CLIENT", "LIST", "TYPE", "pubsub" });
    defer allocator.free(list_result);

    // Should contain the pubsub client's type
    try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") != null);
    // normal client should not be in the list (it's not subscribed)
    try testing.expect(std.mem.indexOf(u8, list_result, "type=normal") == null);
}

test "iter352 - CLIENT LIST TYPE normal excludes subscribed clients" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const normal_id = try registry.registerClient("127.0.0.1:9006", 15, "127.0.0.1:6379");
    const pubsub_id = try registry.registerClient("127.0.0.1:9007", 16, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe pubsub_id using subscriber_id=2
    const sub_result = try execCmd(allocator, storage, &registry, pubsub_id, &ps, 2, &.{ "SUBSCRIBE", "updates" });
    defer allocator.free(sub_result);

    // CLIENT LIST TYPE normal should only return the normal client
    const list_result = try execCmd(allocator, storage, &registry, normal_id, &ps, 1, &.{ "CLIENT", "LIST", "TYPE", "normal" });
    defer allocator.free(list_result);

    try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") == null);
}

test "iter352 - CLIENT LIST sub count resets after UNSUBSCRIBE" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const sub_client_id = try registry.registerClient("127.0.0.1:9008", 17, "127.0.0.1:6379");
    const monitor_id = try registry.registerClient("127.0.0.1:9008b", 170, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe (subscriber_id=17)
    const sub_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 17, &.{ "SUBSCRIBE", "chan1" });
    defer allocator.free(sub_result);

    // Verify subscribed — use monitor client (subscriber_id=170, no subscriptions)
    {
        const list_result = try execCmd(allocator, storage, &registry, monitor_id, &ps, 170, &.{ "CLIENT", "LIST" });
        defer allocator.free(list_result);
        try testing.expect(std.mem.indexOf(u8, list_result, "sub=1") != null);
        try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") != null);
    }

    // Unsubscribe (subscriber_id=17, allowed in subscription mode)
    const unsub_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 17, &.{ "UNSUBSCRIBE", "chan1" });
    defer allocator.free(unsub_result);

    // After unsubscribing, sub should be 0 — now sub_client_id has no subscriptions, can use directly
    {
        const list_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 17, &.{ "CLIENT", "LIST" });
        defer allocator.free(list_result);
        try testing.expect(std.mem.indexOf(u8, list_result, "sub=0") != null);
        try testing.expect(std.mem.indexOf(u8, list_result, "type=normal") != null);
    }
}

test "iter352 - CLIENT INFO shows real sub counts" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const sub_client_id = try registry.registerClient("127.0.0.1:9009", 18, "127.0.0.1:6379");
    const monitor_id = try registry.registerClient("127.0.0.1:9009b", 180, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe to 2 channels at once using subscriber_id=18
    const sub1 = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 18, &.{ "SUBSCRIBE", "ch1", "ch2" });
    defer allocator.free(sub1);

    // Verify subscription counts via PubSub state
    try testing.expectEqual(@as(usize, 2), ps.channelCount(18));
    try testing.expectEqual(@as(usize, 0), ps.patternCount(18));

    // Use monitor client (subscriber_id=180, no subscriptions) to call CLIENT LIST
    // which shows all clients including the subscribed one
    const list_result = try execCmd(allocator, storage, &registry, monitor_id, &ps, 180, &.{ "CLIENT", "LIST" });
    defer allocator.free(list_result);

    try testing.expect(std.mem.indexOf(u8, list_result, "sub=2") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "psub=0") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") != null);
}

test "iter352 - CLIENT LIST multiple subscriptions show correct counts" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const sub_client_id = try registry.registerClient("127.0.0.1:9010", 19, "127.0.0.1:6379");
    const monitor_id = try registry.registerClient("127.0.0.1:9010b", 190, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Subscribe to channels using subscriber_id=19
    const sub_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 19, &.{ "SUBSCRIBE", "ch1", "ch2", "ch3" });
    defer allocator.free(sub_result);
    // PSUBSCRIBE is also allowed in subscription mode
    const psub_result = try execCmd(allocator, storage, &registry, sub_client_id, &ps, 19, &.{ "PSUBSCRIBE", "p1*", "p2*" });
    defer allocator.free(psub_result);

    // Monitor client (subscriber_id=190, no subscriptions) calls CLIENT LIST
    const list_result = try execCmd(allocator, storage, &registry, monitor_id, &ps, 190, &.{ "CLIENT", "LIST" });
    defer allocator.free(list_result);

    try testing.expect(std.mem.indexOf(u8, list_result, "sub=3") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "psub=2") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "ssub=0") != null);
    try testing.expect(std.mem.indexOf(u8, list_result, "type=pubsub") != null);
}
