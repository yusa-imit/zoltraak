const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const PubSub = zoltraak.pubsub.PubSub;
const ClientRegistry = zoltraak.ClientRegistry;
const RespValue = zoltraak.protocol.RespValue;
const lists_cmds = zoltraak.lists_commands;
const sets_cmds = zoltraak.sets_commands;

// Iteration 410: LPUSH and SADD computed CLIENT TRACKING invalidation messages
// via getInvalidationMessages() but then threw them away (dead code marked
// "TODO: Send invalidation push messages to RESP3 clients" — the messages were
// generated, then immediately deinit'd/freed without ever being queued for
// delivery). This meant a client with `CLIENT TRACKING ON` caching a key never
// received an invalidation push when another client mutated that key via
// LPUSH/SADD, even though the identical SET/DEL path (client.zig's
// notifyInvalidation()) worked correctly. Fixed by routing both commands
// through the same notifyInvalidation() helper SET/DEL already use.

fn makeArgs(comptime parts: []const []const u8) [parts.len]RespValue {
    var args: [parts.len]RespValue = undefined;
    inline for (parts, 0..) |p, i| {
        args[i] = RespValue{ .bulk_string = p };
    }
    return args;
}

test "iter410 - LPUSH delivers invalidation push to a tracking client" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Tracking client reads (tracks) "mylist" first.
    const tracker = try client_registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    client_registry.setProtocol(tracker, .RESP3);
    try client_registry.setTracking(tracker, true, -1, false, false, false, false, &[_][]const u8{});
    try client_registry.trackKeyAccess(tracker, "mylist");

    // A different client performs LPUSH on the tracked key.
    const writer_client = try client_registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    var args = makeArgs(&.{ "LPUSH", "mylist", "hello" });
    const result = try lists_cmds.cmdLpush(allocator, storage, &args, &pubsub, 0, &client_registry, writer_client);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    const pending = client_registry.takePendingInvalidations(tracker) orelse {
        try testing.expect(false); // expected a queued invalidation push message
        return;
    };
    defer {
        for (pending) |msg| allocator.free(msg);
        allocator.free(pending);
    }

    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "invalidate") != null);
    try testing.expect(std.mem.indexOf(u8, pending[0], "mylist") != null);

    // Key must be removed from the tracking table after invalidation: a
    // second lookup for the same key should now yield zero messages.
    const followup = try client_registry.getInvalidationMessages("mylist", writer_client, allocator);
    defer allocator.free(followup);
    try testing.expectEqual(@as(usize, 0), followup.len);
}

test "iter410 - LPUSH with NOLOOP does not notify the client that performed the write" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // NOLOOP=true: this client should not receive invalidations for keys it
    // modifies itself, even though it tracks them.
    const tracker = try client_registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    client_registry.setProtocol(tracker, .RESP3);
    try client_registry.setTracking(tracker, true, -1, false, false, false, true, &[_][]const u8{});
    try client_registry.trackKeyAccess(tracker, "mylist");

    var args = makeArgs(&.{ "LPUSH", "mylist", "hello" });
    const result = try lists_cmds.cmdLpush(allocator, storage, &args, &pubsub, 0, &client_registry, tracker);
    defer allocator.free(result);

    // NOLOOP suppresses self-delivery, but the tracking entry is still cleared.
    try testing.expect(client_registry.takePendingInvalidations(tracker) == null);
    const followup = try client_registry.getInvalidationMessages("mylist", tracker, allocator);
    defer allocator.free(followup);
    try testing.expectEqual(@as(usize, 0), followup.len);
}

test "iter410 - SADD delivers invalidation push to a tracking client" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const tracker = try client_registry.registerClient("127.0.0.1:2000", 2000, "127.0.0.1:6379");
    client_registry.setProtocol(tracker, .RESP3);
    try client_registry.setTracking(tracker, true, -1, false, false, false, false, &[_][]const u8{});
    try client_registry.trackKeyAccess(tracker, "myset");

    const writer_client = try client_registry.registerClient("127.0.0.1:2001", 2001, "127.0.0.1:6379");

    var args = makeArgs(&.{ "SADD", "myset", "member1" });
    const result = try sets_cmds.cmdSadd(allocator, storage, &args, &pubsub, 0, &client_registry, writer_client);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    const pending = client_registry.takePendingInvalidations(tracker) orelse {
        try testing.expect(false); // expected a queued invalidation push message
        return;
    };
    defer {
        for (pending) |msg| allocator.free(msg);
        allocator.free(pending);
    }

    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "invalidate") != null);
    try testing.expect(std.mem.indexOf(u8, pending[0], "myset") != null);

    const followup = try client_registry.getInvalidationMessages("myset", writer_client, allocator);
    defer allocator.free(followup);
    try testing.expectEqual(@as(usize, 0), followup.len);
}

test "iter410 - SADD with no tracking clients does not error and produces no pending messages" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const writer_client = try client_registry.registerClient("127.0.0.1:2002", 2002, "127.0.0.1:6379");

    var args = makeArgs(&.{ "SADD", "untracked_set", "member1" });
    const result = try sets_cmds.cmdSadd(allocator, storage, &args, &pubsub, 0, &client_registry, writer_client);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    try testing.expect(client_registry.takePendingInvalidations(writer_client) == null);
}
