const std = @import("std");
const testing = std.testing;

const storage = @import("../src/storage/memory.zig");
const notifications_mod = @import("../src/storage/notifications.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");

const Memory = storage.Memory;
const PubSub = pubsub_mod.PubSub;
const NotificationFlag = notifications_mod.NotificationFlag;
const parseNotificationFlags = notifications_mod.parseNotificationFlags;
const shouldNotify = notifications_mod.shouldNotify;
const publishNotification = notifications_mod.publishNotification;

// ============================================================================
// Unit Tests — Notification Flag Parsing and Checking
// ============================================================================

test "parseNotificationFlags - KEA enables all events" {
    const flags = parseNotificationFlags("KEA");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .keyevent));
    try testing.expect(shouldNotify(flags, .generic));
    try testing.expect(shouldNotify(flags, .string));
    try testing.expect(shouldNotify(flags, .list));
    try testing.expect(shouldNotify(flags, .set));
    try testing.expect(shouldNotify(flags, .hash));
    try testing.expect(shouldNotify(flags, .sorted_set));
    try testing.expect(shouldNotify(flags, .stream));
    try testing.expect(shouldNotify(flags, .expired));
    try testing.expect(shouldNotify(flags, .evicted));
}

test "parseNotificationFlags - Kgx enables keyspace, generic, and expired only" {
    const flags = parseNotificationFlags("Kgx");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .generic));
    try testing.expect(shouldNotify(flags, .expired));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .string));
    try testing.expect(!shouldNotify(flags, .list));
}

test "parseNotificationFlags - E only enables keyevent" {
    const flags = parseNotificationFlags("E");
    try testing.expect(!shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .keyevent));
}

test "parseNotificationFlags - empty string disables all" {
    const flags = parseNotificationFlags("");
    try testing.expect(flags == 0);
    try testing.expect(!shouldNotify(flags, .keyspace));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .generic));
}

test "parseNotificationFlags - K$ enables keyspace and string only" {
    const flags = parseNotificationFlags("K$");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .string));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .list));
}

test "parseNotificationFlags - Kl enables keyspace and list only" {
    const flags = parseNotificationFlags("Kl");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .list));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .set));
}

test "parseNotificationFlags - Kh enables keyspace and hash only" {
    const flags = parseNotificationFlags("Kh");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .hash));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .sorted_set));
}

test "parseNotificationFlags - Ks enables keyspace and set only" {
    const flags = parseNotificationFlags("Ks");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .set));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .hash));
}

test "parseNotificationFlags - Kz enables keyspace and sorted_set only" {
    const flags = parseNotificationFlags("Kz");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .sorted_set));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .stream));
}

test "parseNotificationFlags - Kt enables keyspace and stream only" {
    const flags = parseNotificationFlags("Kt");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .stream));
    try testing.expect(!shouldNotify(flags, .keyevent));
}

test "parseNotificationFlags - Kxe enables keyspace, expired, and evicted" {
    const flags = parseNotificationFlags("Kxe");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .expired));
    try testing.expect(shouldNotify(flags, .evicted));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .generic));
}

test "parseNotificationFlags - case insensitive ignored" {
    // Lowercase letters should be ignored, only uppercase recognized
    const flags = parseNotificationFlags("kga");
    try testing.expect(!shouldNotify(flags, .keyspace));
    try testing.expect(!shouldNotify(flags, .generic));
}

test "shouldNotify - respects individual flags" {
    const flags = @intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.string);
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .string));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .list));
}

test "shouldNotify - zero flags disables all" {
    const flags = 0;
    try testing.expect(!shouldNotify(flags, .keyspace));
    try testing.expect(!shouldNotify(flags, .keyevent));
    try testing.expect(!shouldNotify(flags, .generic));
}

// ============================================================================
// Unit Tests — Notification Publishing
// ============================================================================

test "publishNotification - keyspace only publishes to keyspace channel" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = @intFromEnum(NotificationFlag.keyspace);

    // Subscribe to the keyspace channel
    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:mykey");

    // Publish notification
    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    // Should have received the message
    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "set") != null);
}

test "publishNotification - keyevent only publishes to keyevent channel" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = @intFromEnum(NotificationFlag.keyevent);

    // Subscribe to the keyevent channel
    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");

    // Publish notification
    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    // Should have received the message with key name
    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "mykey") != null);
}

test "publishNotification - both keyspace and keyevent publishes to both channels" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = @intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.keyevent);

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:mykey");
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 2), pending.len);
}

test "publishNotification - respects db index in channel names" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = @intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.keyevent);

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@2__:testkey");
    _ = try ps.subscribe(sub_id, "__keyevent@2__:del");

    try publishNotification(allocator, &ps, 2, "testkey", "del", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 2), pending.len);
}

test "publishNotification - no subscribers when disabled" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = 0; // Disabled

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:mykey");
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    // No messages should be delivered because flags are disabled
    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 0), pending.len);
}

test "publishNotification - multiple keys with same event" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = @intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.keyevent);

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:key1");
    _ = try ps.subscribe(sub_id, "__keyspace@0__:key2");
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");

    try publishNotification(allocator, &ps, 0, "key1", "set", flags);

    // key1 keyspace + set keyevent = 2 messages
    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 2), pending.len);
}

// ============================================================================
// Integration Tests — RESP2 Mode (Default)
// ============================================================================

test "RESP2 - keyspace event for DEL operation" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:testkey");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "testkey", "del", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);

    // RESP2 format: *3\r\n$7\r\nmessage\r\n...
    try testing.expect(std.mem.startsWith(u8, pending[0], "*3\r\n"));
    try testing.expect(std.mem.indexOf(u8, pending[0], "message") != null);
    try testing.expect(std.mem.indexOf(u8, pending[0], "__keyspace@0__:testkey") != null);
    try testing.expect(std.mem.indexOf(u8, pending[0], "del") != null);
}

test "RESP2 - keyevent event for SET operation" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("E$");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);

    // RESP2 format: *3\r\n$7\r\nmessage\r\n...
    try testing.expect(std.mem.startsWith(u8, pending[0], "*3\r\n"));
    try testing.expect(std.mem.indexOf(u8, pending[0], "__keyevent@0__:set") != null);
    try testing.expect(std.mem.indexOf(u8, pending[0], "mykey") != null);
}

test "RESP2 - generic event EXPIRE" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:expiringkey");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "expiringkey", "expire", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "expire") != null);
}

test "RESP2 - string event INCR" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("K$");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:counter");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "counter", "incr", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "incr") != null);
}

test "RESP2 - list event LPUSH" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kl");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:mylist");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "mylist", "lpush", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "lpush") != null);
}

test "RESP2 - hash event HSET" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kh");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:myhash");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "myhash", "hset", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "hset") != null);
}

test "RESP2 - set event SADD" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Ks");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:myset");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "myset", "sadd", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "sadd") != null);
}

test "RESP2 - sorted set event ZADD" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kz");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:myzset");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "myzset", "zadd", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "zadd") != null);
}

test "RESP2 - expired event" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kx");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:oldkey");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "oldkey", "expired", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "expired") != null);
}

// ============================================================================
// Integration Tests — RESP3 Mode
// ============================================================================

test "RESP3 - keyspace event format with push type" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:testkey");
    try ps.setSubscriberVersion(sub_id, 3);

    try publishNotification(allocator, &ps, 0, "testkey", "del", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);

    // RESP3 format: >3\r\n+message\r\n...
    try testing.expect(std.mem.startsWith(u8, pending[0], ">3\r\n"));
    try testing.expect(std.mem.indexOf(u8, pending[0], "+message\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, pending[0], "del") != null);
}

test "RESP3 - keyevent event format with push type" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("E$");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");
    try ps.setSubscriberVersion(sub_id, 3);

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);

    // RESP3 format: >3\r\n+message\r\n...
    try testing.expect(std.mem.startsWith(u8, pending[0], ">3\r\n"));
    try testing.expect(std.mem.indexOf(u8, pending[0], "+message\r\n") != null);
}

test "RESP3 - generic event with multiple event types" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id1: u64 = 1;
    const sub_id2: u64 = 2;
    const sub_id3: u64 = 3;

    _ = try ps.subscribe(sub_id1, "__keyspace@0__:key1");
    try ps.setSubscriberVersion(sub_id1, 3);

    _ = try ps.subscribe(sub_id2, "__keyspace@0__:key1");
    try ps.setSubscriberVersion(sub_id2, 2);

    _ = try ps.subscribe(sub_id3, "__keyspace@0__:key2");
    try ps.setSubscriberVersion(sub_id3, 3);

    try publishNotification(allocator, &ps, 0, "key1", "del", flags);

    // Subscriber 1 should get RESP3
    const pending1 = ps.pendingMessages(sub_id1);
    try testing.expectEqual(@as(usize, 1), pending1.len);
    try testing.expect(std.mem.startsWith(u8, pending1[0], ">3\r\n"));

    // Subscriber 2 should get RESP2
    const pending2 = ps.pendingMessages(sub_id2);
    try testing.expectEqual(@as(usize, 1), pending2.len);
    try testing.expect(std.mem.startsWith(u8, pending2[0], "*3\r\n"));

    // Subscriber 3 should get nothing
    const pending3 = ps.pendingMessages(sub_id3);
    try testing.expectEqual(@as(usize, 0), pending3.len);
}

// ============================================================================
// Integration Tests — Configuration and Event Control
// ============================================================================

test "config - KEA enables all event types" {
    const flags = parseNotificationFlags("KEA");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .keyevent));
    try testing.expect(shouldNotify(flags, .generic));
    try testing.expect(shouldNotify(flags, .string));
}

test "config - Kg enables keyspace and generic only" {
    const flags = parseNotificationFlags("Kg");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .generic));
    try testing.expect(!shouldNotify(flags, .keyevent));
}

test "config - Kx enables keyspace and expired only" {
    const flags = parseNotificationFlags("Kx");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .expired));
    try testing.expect(!shouldNotify(flags, .keyevent));
}

test "config - Ke enables keyspace and evicted only" {
    const flags = parseNotificationFlags("Ke");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .evicted));
    try testing.expect(!shouldNotify(flags, .keyevent));
}

test "config - KEx enables keyspace, expired, and evicted" {
    const flags = parseNotificationFlags("KEx");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .expired));
    try testing.expect(shouldNotify(flags, .evicted));
}

test "config - empty string disables notifications" {
    const flags = parseNotificationFlags("");
    try testing.expectEqual(@as(u16, 0), flags);
}

// ============================================================================
// Integration Tests — Pattern Matching (PSUBSCRIBE)
// ============================================================================

test "pattern - PSUBSCRIBE __keyspace@0__:* matches all keys" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.psubscribe(sub_id, "__keyspace@0__:*");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "key1", "set", flags);
    try publishNotification(allocator, &ps, 0, "key2", "del", flags);
    try publishNotification(allocator, &ps, 0, "key3", "expire", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 3), pending.len);
}

test "pattern - PSUBSCRIBE __keyevent@0__:* matches all events" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Eg");

    const sub_id: u64 = 1;
    _ = try ps.psubscribe(sub_id, "__keyevent@0__:*");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "key1", "set", flags);
    try publishNotification(allocator, &ps, 0, "key2", "del", flags);
    try publishNotification(allocator, &ps, 0, "key3", "expire", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 3), pending.len);
}

test "pattern - PSUBSCRIBE __keyspace@0__:user:* matches user keys only" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.psubscribe(sub_id, "__keyspace@0__:user:*");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "user:1", "set", flags);
    try publishNotification(allocator, &ps, 0, "user:2", "del", flags);
    try publishNotification(allocator, &ps, 0, "session:abc", "expire", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 2), pending.len);
}

test "pattern - mixed exact and pattern subscriptions" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:important");
    _ = try ps.psubscribe(sub_id, "__keyspace@0__:*");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "important", "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
}

// ============================================================================
// Integration Tests — Multiple Databases
// ============================================================================

test "multi-db - notifications respect db index" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:mykey");
    _ = try ps.subscribe(sub_id, "__keyspace@1__:mykey");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);

    ps.drainMessages(sub_id);

    try publishNotification(allocator, &ps, 1, "mykey", "del", flags);
    const pending2 = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending2.len);
}

test "multi-db - pattern matches all databases" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.psubscribe(sub_id, "__keyspace@*__:mykey");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);
    try publishNotification(allocator, &ps, 1, "mykey", "del", flags);
    try publishNotification(allocator, &ps, 2, "mykey", "expire", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 3), pending.len);
}

// ============================================================================
// Integration Tests — Edge Cases
// ============================================================================

test "edge - empty key name is still notified" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "", "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
}

test "edge - very long key names are handled" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    var long_key: [256]u8 = undefined;
    for (long_key, 0..) |*c, i| {
        c.* = @intCast(@mod(i, 26) + @as(u8, 'a'));
    }

    const sub_id: u64 = 1;
    const chan = try std.fmt.allocPrint(allocator, "__keyspace@0__:{s}", .{long_key[0..]});
    defer allocator.free(chan);

    _ = try ps.subscribe(sub_id, chan);
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, &long_key, "set", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
}

test "edge - special characters in event names" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:key:with:colons");
    try ps.setSubscriberVersion(sub_id, 2);

    try publishNotification(allocator, &ps, 0, "key:with:colons", "custom-event", flags);

    const pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expect(std.mem.indexOf(u8, pending[0], "custom-event") != null);
}

test "edge - many subscribers on same notification" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("Kg");

    const num_subs = 100;
    var i: u64 = 0;
    while (i < num_subs) : (i += 1) {
        _ = try ps.subscribe(i, "__keyspace@0__:mykey");
        try ps.setSubscriberVersion(i, if (i % 2 == 0) 2 else 3);
    }

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    i = 0;
    while (i < num_subs) : (i += 1) {
        const pending = ps.pendingMessages(i);
        try testing.expectEqual(@as(usize, 1), pending.len);
    }
}

test "edge - single subscriber to multiple events on same key" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("KEg$l");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:mykey");
    _ = try ps.subscribe(sub_id, "__keyevent@0__:set");
    _ = try ps.subscribe(sub_id, "__keyevent@0__:lpush");
    try ps.setSubscriberVersion(sub_id, 3);

    try publishNotification(allocator, &ps, 0, "mykey", "expire", flags);
    var pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 1), pending.len);

    ps.drainMessages(sub_id);

    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);
    pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 2), pending.len);

    ps.drainMessages(sub_id);

    try publishNotification(allocator, &ps, 0, "mykey", "lpush", flags);
    pending = ps.pendingMessages(sub_id);
    try testing.expectEqual(@as(usize, 2), pending.len);
}

// ============================================================================
// Integration Tests — Flag Combinations
// ============================================================================

test "flags - K alone only enables keyspace" {
    const flags = parseNotificationFlags("K");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(!shouldNotify(flags, .keyevent));
}

test "flags - E alone only enables keyevent" {
    const flags = parseNotificationFlags("E");
    try testing.expect(!shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .keyevent));
}

test "flags - Kgz$ enables keyspace with generic, sorted_set, and string" {
    const flags = parseNotificationFlags("Kgz$");
    try testing.expect(shouldNotify(flags, .keyspace));
    try testing.expect(shouldNotify(flags, .generic));
    try testing.expect(shouldNotify(flags, .sorted_set));
    try testing.expect(shouldNotify(flags, .string));
    try testing.expect(!shouldNotify(flags, .keyevent));
}

// ============================================================================
// Integration Tests — Event Type Coverage
// ============================================================================

test "event-coverage - all event types are recognized" {
    const allocator = testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const flags = parseNotificationFlags("KEA");

    const sub_id: u64 = 1;
    _ = try ps.subscribe(sub_id, "__keyspace@0__:key");
    try ps.setSubscriberVersion(sub_id, 2);

    const events = [_][]const u8{
        "set", "incr", "lpush", "sadd", "hset", "zadd",
        "del", "expire", "rename", "expired", "evicted",
    };

    for (events) |event| {
        ps.drainMessages(sub_id);
        try publishNotification(allocator, &ps, 0, "key", event, flags);
        const pending = ps.pendingMessages(sub_id);
        try testing.expectEqual(@as(usize, 1), pending.len);
        try testing.expect(std.mem.indexOf(u8, pending[0], event) != null);
    }
}
