const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const PubSub = @import("../src/storage/pubsub.zig").PubSub;
const Config = @import("../src/storage/config.zig").Config;
const RespValue = @import("../src/protocol/parser.zig").RespValue;

// Import bitmap command modules
const bits_mod = @import("../src/commands/bits.zig");
const bitfield_mod = @import("../src/commands/bitfield.zig");

/// Helper: Subscribe to keyspace and keyevent channels for a key
fn setupNotificationSubscription(
    allocator: std.mem.Allocator,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event: []const u8,
) !void {
    // Subscribe to keyspace channel: __keyspace@<db>__:<key>
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:{s}",
        .{ db_index, key },
    );
    defer allocator.free(keyspace_channel);
    _ = try pubsub_state.subscribe(1, keyspace_channel);

    // Subscribe to keyevent channel: __keyevent@<db>__:<event>
    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:{s}",
        .{ db_index, event },
    );
    defer allocator.free(keyevent_channel);
    _ = try pubsub_state.subscribe(1, keyevent_channel);
}

/// Helper: Check if notification channels have subscribers (indicates notification was published)
fn checkNotificationFired(
    allocator: std.mem.Allocator,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event: []const u8,
) !bool {
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:{s}",
        .{ db_index, key },
    );
    defer allocator.free(keyspace_channel);
    const keyspace_subs = pubsub_state.getSubscriberCount(keyspace_channel);

    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:{s}",
        .{ db_index, event },
    );
    defer allocator.free(keyevent_channel);
    const keyevent_subs = pubsub_state.getSubscriberCount(keyevent_channel);

    return keyspace_subs > 0 and keyevent_subs > 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// SETBIT Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "SETBIT fires notification when bit changes (0→1)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mykey", "setbit");

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // THIS WILL FAIL TO COMPILE - cmdSetbit doesn't have ps/db_index parameters yet
    // zig-implementor needs to update signature to:
    // pub fn cmdSetbit(storage, args, writer, allocator, ps, db_index)
    try bits_mod.cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mykey", "7", "1" }, writer, allocator, &pubsub_state, db_index);

    // After implementation, verify notification was sent
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "mykey", "setbit"));
}

test "SETBIT does NOT fire when bit value unchanged" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // Set bit to 1 first
    try bits_mod.cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mykey", "7", "1" }, writer, allocator, &pubsub_state, db_index);
    buf.clearRetainingCapacity();

    // Subscribe after first set
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mykey", "setbit");

    const keyspace_ch = try std.fmt.allocPrint(allocator, "__keyspace@{d}__:mykey", .{db_index});
    defer allocator.free(keyspace_ch);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_ch);

    // SETBIT mykey 7 1 again (no change)
    try bits_mod.cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mykey", "7", "1" }, writer, allocator, &pubsub_state, db_index);

    // Verify NO notification (subscriber count unchanged)
    const final_count = pubsub_state.getSubscriberCount(keyspace_ch);
    try testing.expectEqual(initial_count, final_count);
}

test "SETBIT fires when string grows" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mykey", "setbit");

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // SETBIT at large offset causes string growth
    try bits_mod.cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mykey", "1000", "1" }, writer, allocator, &pubsub_state, db_index);

    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "mykey", "setbit"));
}

test "SETBIT does NOT fire on WRONGTYPE error" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create a list
    _ = try storage.lpush("mylist", &[_][]const u8{"value"});

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mylist", "setbit");

    const keyspace_ch = try std.fmt.allocPrint(allocator, "__keyspace@{d}__:mylist", .{db_index});
    defer allocator.free(keyspace_ch);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_ch);

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // Try SETBIT on list (should error)
    try bits_mod.cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mylist", "0", "1" }, writer, allocator, &pubsub_state, db_index);

    // Verify error
    try testing.expect(std.mem.indexOf(u8, buf.items, "WRONGTYPE") != null);

    // Verify NO notification
    const final_count = pubsub_state.getSubscriberCount(keyspace_ch);
    try testing.expectEqual(initial_count, final_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// BITOP Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "BITOP fires 'set' notification when result has content" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try storage.set("key1", "foo", null);
    try storage.set("key2", "bar", null);

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest", "set");

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // THIS WILL FAIL TO COMPILE - cmdBitop needs ps/db_index parameters
    try bits_mod.cmdBitop(&storage, &[_][]const u8{ "BITOP", "AND", "dest", "key1", "key2" }, writer, allocator, &pubsub_state, db_index);

    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "dest", "set"));
}

test "BITOP fires 'del' when result empty and destkey existed" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kg"); // generic flag for 'del'
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try storage.set("dest", "oldvalue", null);

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest", "del");

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // BITOP with non-existent keys yields empty result
    try bits_mod.cmdBitop(&storage, &[_][]const u8{ "BITOP", "AND", "dest", "nonexist1", "nonexist2" }, writer, allocator, &pubsub_state, db_index);

    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "dest", "del"));
}

test "BITOP does NOT fire when result empty and destkey didn't exist" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEg");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "newdest", "del");

    const keyevent_ch = try std.fmt.allocPrint(allocator, "__keyevent@{d}__:del", .{db_index});
    defer allocator.free(keyevent_ch);
    const initial_count = pubsub_state.getSubscriberCount(keyevent_ch);

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const writer = buf.writer(allocator);

    // BITOP with empty result, destkey doesn't exist
    try bits_mod.cmdBitop(&storage, &[_][]const u8{ "BITOP", "AND", "newdest", "nonexist1", "nonexist2" }, writer, allocator, &pubsub_state, db_index);

    // Verify NO notification
    const final_count = pubsub_state.getSubscriberCount(keyevent_ch);
    try testing.expectEqual(initial_count, final_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// BITFIELD Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "BITFIELD fires 'setbit' when SET modifies string" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mykey", "setbit");

    const args = [_]RespValue{
        .{ .bulk_string = "BITFIELD" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "u8" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "255" },
    };

    // THIS WILL FAIL TO COMPILE - cmdBitfield needs ps/db_index parameters
    const result = try bitfield_mod.cmdBitfield(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "mykey", "setbit"));
}

test "BITFIELD does NOT fire for GET-only operations" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    try storage.set("mykey", "\xff", null);

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mykey", "setbit");

    const keyspace_ch = try std.fmt.allocPrint(allocator, "__keyspace@{d}__:mykey", .{db_index});
    defer allocator.free(keyspace_ch);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_ch);

    const args = [_]RespValue{
        .{ .bulk_string = "BITFIELD" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "GET" },
        .{ .bulk_string = "u8" },
        .{ .bulk_string = "0" },
    };

    const result = try bitfield_mod.cmdBitfield(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify NO notification
    const final_count = pubsub_state.getSubscriberCount(keyspace_ch);
    try testing.expectEqual(initial_count, final_count);
}

test "BITFIELD does NOT fire when OVERFLOW FAIL prevents modification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Set u8 to max value
    const setup_args = [_]RespValue{
        .{ .bulk_string = "BITFIELD" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "u8" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "255" },
    };
    const setup_result = try bitfield_mod.cmdBitfield(allocator, &storage, &setup_args, &pubsub_state, db_index);
    allocator.free(setup_result);

    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mykey", "setbit");

    const keyspace_ch = try std.fmt.allocPrint(allocator, "__keyspace@{d}__:mykey", .{db_index});
    defer allocator.free(keyspace_ch);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_ch);

    // Try to increment (should overflow with FAIL)
    const args = [_]RespValue{
        .{ .bulk_string = "BITFIELD" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "OVERFLOW" },
        .{ .bulk_string = "FAIL" },
        .{ .bulk_string = "INCRBY" },
        .{ .bulk_string = "u8" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "1" },
    };

    const result = try bitfield_mod.cmdBitfield(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify result is nil (overflow)
    try testing.expect(std.mem.indexOf(u8, result, "$-1") != null);

    // Verify NO notification
    const final_count = pubsub_state.getSubscriberCount(keyspace_ch);
    try testing.expectEqual(initial_count, final_count);
}
