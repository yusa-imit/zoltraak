//! Keyspace notification flags and dispatch for `notify-keyspace-events`,
//! publishing `__keyspace@<db>__`/`__keyevent@<db>__` messages through pub/sub.

const std = @import("std");
const pubsub_mod = @import("pubsub.zig");
const PubSub = pubsub_mod.PubSub;

/// Keyspace notification event types
/// Redis CONFIG SET notify-keyspace-events flags
pub const NotificationFlag = enum(u16) {
    /// K - Keyspace events published as __keyspace@<db>__:<key>
    keyspace = 1 << 0,
    /// E - Keyevent events published as __keyevent@<db>__:<event>
    keyevent = 1 << 1,
    /// g - Generic commands (DEL, EXPIRE, RENAME, etc.)
    generic = 1 << 2,
    /// $ - String commands
    string = 1 << 3,
    /// l - List commands
    list = 1 << 4,
    /// s - Set commands
    set = 1 << 5,
    /// h - Hash commands
    hash = 1 << 6,
    /// z - Sorted set commands
    sorted_set = 1 << 7,
    /// t - Stream commands
    stream = 1 << 8,
    /// d - Module key type events
    module = 1 << 9,
    /// x - Expired events
    expired = 1 << 10,
    /// e - Evicted events
    evicted = 1 << 11,
    /// m - Missed events (key-miss)
    missed = 1 << 12,
    /// n - New key events
    new = 1 << 13,
    /// A - Alias for all flags (g$lshztdxemn)
    all = 0x3FFC, // All flags except keyspace/keyevent
};

/// Parse notify-keyspace-events config string to bit flags
/// Examples: "KEA" (all events), "Kgx" (generic+expired), "" (disabled)
pub fn parseNotificationFlags(config_str: []const u8) u16 {
    var flags: u16 = 0;

    for (config_str) |c| {
        switch (c) {
            'K' => flags |= @intFromEnum(NotificationFlag.keyspace),
            'E' => flags |= @intFromEnum(NotificationFlag.keyevent),
            'g' => flags |= @intFromEnum(NotificationFlag.generic),
            '$' => flags |= @intFromEnum(NotificationFlag.string),
            'l' => flags |= @intFromEnum(NotificationFlag.list),
            's' => flags |= @intFromEnum(NotificationFlag.set),
            'h' => flags |= @intFromEnum(NotificationFlag.hash),
            'z' => flags |= @intFromEnum(NotificationFlag.sorted_set),
            't' => flags |= @intFromEnum(NotificationFlag.stream),
            'd' => flags |= @intFromEnum(NotificationFlag.module),
            'x' => flags |= @intFromEnum(NotificationFlag.expired),
            'e' => flags |= @intFromEnum(NotificationFlag.evicted),
            'm' => flags |= @intFromEnum(NotificationFlag.missed),
            'n' => flags |= @intFromEnum(NotificationFlag.new),
            'A' => flags |= @intFromEnum(NotificationFlag.all),
            else => {},
        }
    }

    return flags;
}

/// Check if a specific notification type should fire
pub fn shouldNotify(flags: u16, event_flag: NotificationFlag) bool {
    return (flags & @intFromEnum(event_flag)) != 0;
}

/// Publish keyspace notification to pub/sub channels
/// Format: __keyspace@<db>__:<key> -> <event>
///         __keyevent@<db>__:<event> -> <key>
pub fn publishNotification(
    allocator: std.mem.Allocator,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event: []const u8,
    flags: u16,
) !void {
    // Check if keyspace events enabled
    if (shouldNotify(flags, .keyspace)) {
        const channel = try std.fmt.allocPrint(
            allocator,
            "__keyspace@{d}__:{s}",
            .{ db_index, key },
        );
        defer allocator.free(channel);

        _ = try pubsub_state.publish(channel, event);
    }

    // Check if keyevent events enabled
    if (shouldNotify(flags, .keyevent)) {
        const channel = try std.fmt.allocPrint(
            allocator,
            "__keyevent@{d}__:{s}",
            .{ db_index, event },
        );
        defer allocator.free(channel);

        _ = try pubsub_state.publish(channel, key);
    }
}

// Tests
test "parseNotificationFlags - all flags" {
    const flags = parseNotificationFlags("KEA");
    try std.testing.expect(shouldNotify(flags, .keyspace));
    try std.testing.expect(shouldNotify(flags, .keyevent));
    try std.testing.expect(shouldNotify(flags, .generic));
    try std.testing.expect(shouldNotify(flags, .string));
    try std.testing.expect(shouldNotify(flags, .expired));
}

test "parseNotificationFlags - selective flags" {
    const flags = parseNotificationFlags("Kgx");
    try std.testing.expect(shouldNotify(flags, .keyspace));
    try std.testing.expect(shouldNotify(flags, .generic));
    try std.testing.expect(shouldNotify(flags, .expired));
    try std.testing.expect(!shouldNotify(flags, .keyevent));
    try std.testing.expect(!shouldNotify(flags, .string));
}

test "parseNotificationFlags - empty (disabled)" {
    const flags = parseNotificationFlags("");
    try std.testing.expect(flags == 0);
    try std.testing.expect(!shouldNotify(flags, .keyspace));
    try std.testing.expect(!shouldNotify(flags, .keyevent));
}

test "shouldNotify - individual flags" {
    const flags = @intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.generic);
    try std.testing.expect(shouldNotify(flags, .keyspace));
    try std.testing.expect(shouldNotify(flags, .generic));
    try std.testing.expect(!shouldNotify(flags, .keyevent));
}

/// Assert `subscriber_id` holds exactly one pending RESP2 frame for `message` on `channel`.
fn expect_frame_delivered(
    gpa: std.mem.Allocator,
    ps: *PubSub,
    subscriber_id: u64,
    channel: []const u8,
    message: []const u8,
) !void {
    const frame = try pubsub_mod.buildMessageFrame(gpa, channel, message, 2);
    defer gpa.free(frame);

    const pending = ps.pendingMessages(subscriber_id);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expectEqualStrings(frame, pending[0]);
}

test "publishNotification - keyspace only" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "__keyspace@0__:mykey");
    _ = try ps.subscribe(2, "__keyevent@0__:set");

    const flags = @intFromEnum(NotificationFlag.keyspace);
    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    try expect_frame_delivered(allocator, &ps, 1, "__keyspace@0__:mykey", "set");
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(2).len);
}

test "publishNotification - keyevent only" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "__keyspace@0__:mykey");
    _ = try ps.subscribe(2, "__keyevent@0__:set");

    const flags = @intFromEnum(NotificationFlag.keyevent);
    try publishNotification(allocator, &ps, 0, "mykey", "set", flags);

    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(1).len);
    try expect_frame_delivered(allocator, &ps, 2, "__keyevent@0__:set", "mykey");
}

test "publishNotification - both keyspace and keyevent" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "__keyspace@3__:mykey");
    _ = try ps.subscribe(2, "__keyevent@3__:del");

    const flags = @intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.keyevent);
    try publishNotification(allocator, &ps, 3, "mykey", "del", flags);

    try expect_frame_delivered(allocator, &ps, 1, "__keyspace@3__:mykey", "del");
    try expect_frame_delivered(allocator, &ps, 2, "__keyevent@3__:del", "mykey");
}

test "publishNotification - no flags publishes nothing" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "__keyspace@0__:mykey");
    _ = try ps.subscribe(2, "__keyevent@0__:set");

    try publishNotification(allocator, &ps, 0, "mykey", "set", 0);

    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(1).len);
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(2).len);
}
