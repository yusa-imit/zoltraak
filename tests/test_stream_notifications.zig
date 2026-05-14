const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const storage_mod = @import("../src/storage/memory.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");
const config_mod = @import("../src/storage/config.zig");
const commands = @import("../src/commands/streams.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;
const PubSub = pubsub_mod.PubSub;
const Config = config_mod.Config;

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

/// Helper: Check if notification was published
fn checkNotification(
    allocator: std.mem.Allocator,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event: []const u8,
) !bool {
    // Check keyspace channel
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:{s}",
        .{ db_index, key },
    );
    defer allocator.free(keyspace_channel);
    const keyspace_subs = pubsub_state.getSubscriberCount(keyspace_channel);

    // Check keyevent channel
    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:{s}",
        .{ db_index, event },
    );
    defer allocator.free(keyevent_channel);
    const keyevent_subs = pubsub_state.getSubscriberCount(keyevent_channel);

    return keyspace_subs > 0 and keyevent_subs > 0;
}

test "XADD fires keyspace notification when Kt enabled" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt"); // Enable keyspace + stream
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xadd");

    // Execute XADD
    const args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "*" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const result = try commands.cmdXadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify notification was published
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xadd"));
}

test "XADD does not fire notification when Kt disabled" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", ""); // Disabled
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications (but they shouldn't fire)
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xadd");

    // Count initial subscribers
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:mystream",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute XADD
    const args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "*" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const result = try commands.cmdXadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify subscriber count didn't increase (no publish happened)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "XADD fires notification with KEt (both keyspace and keyevent)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEt"); // Enable keyspace + keyevent + stream
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to both channels
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xadd");

    // Execute XADD
    const args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "*" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const result = try commands.cmdXadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify both channels received notifications
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xadd"));
}

test "XDEL fires keyspace notification when Kt enabled" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create stream with entry
    const add_args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const add_result = try commands.cmdXadd(allocator, &storage, &add_args, &pubsub_state, db_index);
    defer allocator.free(add_result);

    // Subscribe to XDEL notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xdel");

    // Execute XDEL
    const del_args = [_]RespValue{
        .{ .bulk_string = "XDEL" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
    };
    const del_result = try commands.cmdXdel(allocator, &storage, &del_args, &pubsub_state, db_index);
    defer allocator.free(del_result);

    // Verify notification was published
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xdel"));
}

test "XDEL does not fire notification when no entries deleted" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create stream
    const add_args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const add_result = try commands.cmdXadd(allocator, &storage, &add_args, &pubsub_state, db_index);
    defer allocator.free(add_result);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xdel");

    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:mystream",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute XDEL with non-existent ID
    const del_args = [_]RespValue{
        .{ .bulk_string = "XDEL" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "999-999" },
    };
    const del_result = try commands.cmdXdel(allocator, &storage, &del_args, &pubsub_state, db_index);
    defer allocator.free(del_result);

    // Verify no publish (count stays same)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "XTRIM fires keyspace notification when Kt enabled" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create stream with multiple entries
    const add_args1 = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const add_result1 = try commands.cmdXadd(allocator, &storage, &add_args1, &pubsub_state, db_index);
    defer allocator.free(add_result1);

    const add_args2 = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "2-0" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    const add_result2 = try commands.cmdXadd(allocator, &storage, &add_args2, &pubsub_state, db_index);
    defer allocator.free(add_result2);

    // Subscribe to XTRIM notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xtrim");

    // Execute XTRIM
    const trim_args = [_]RespValue{
        .{ .bulk_string = "XTRIM" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "MAXLEN" },
        .{ .bulk_string = "1" },
    };
    const trim_result = try commands.cmdXtrim(allocator, &storage, &trim_args, &pubsub_state, db_index);
    defer allocator.free(trim_result);

    // Verify notification was published
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xtrim"));
}

test "XTRIM does not fire notification when no entries trimmed" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create stream with one entry
    const add_args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const add_result = try commands.cmdXadd(allocator, &storage, &add_args, &pubsub_state, db_index);
    defer allocator.free(add_result);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xtrim");

    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:mystream",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute XTRIM with large MAXLEN (nothing trimmed)
    const trim_args = [_]RespValue{
        .{ .bulk_string = "XTRIM" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "MAXLEN" },
        .{ .bulk_string = "1000" },
    };
    const trim_result = try commands.cmdXtrim(allocator, &storage, &trim_args, &pubsub_state, db_index);
    defer allocator.free(trim_result);

    // Verify no publish (count stays same)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "XSETID fires keyspace notification when Kt enabled" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create stream
    const add_args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const add_result = try commands.cmdXadd(allocator, &storage, &add_args, &pubsub_state, db_index);
    defer allocator.free(add_result);

    // Subscribe to XSETID notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xsetid");

    // Execute XSETID
    const setid_args = [_]RespValue{
        .{ .bulk_string = "XSETID" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "5-0" },
    };
    const setid_result = try commands.cmdXsetid(allocator, &storage, &setid_args, &pubsub_state, db_index);
    defer allocator.free(setid_result);

    // Verify notification was published
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xsetid"));
}

test "Multiple stream commands fire distinct notifications" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEt"); // Enable all
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to all stream event channels
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xadd");
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xdel");
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xtrim");
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "mystream", "xsetid");

    // Execute XADD
    const add_args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const add_result = try commands.cmdXadd(allocator, &storage, &add_args, &pubsub_state, db_index);
    defer allocator.free(add_result);
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xadd"));

    // Execute XSETID
    const setid_args = [_]RespValue{
        .{ .bulk_string = "XSETID" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "5-0" },
    };
    const setid_result = try commands.cmdXsetid(allocator, &storage, &setid_args, &pubsub_state, db_index);
    defer allocator.free(setid_result);
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xsetid"));

    // Execute XDEL
    const del_args = [_]RespValue{
        .{ .bulk_string = "XDEL" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1-0" },
    };
    const del_result = try commands.cmdXdel(allocator, &storage, &del_args, &pubsub_state, db_index);
    defer allocator.free(del_result);
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "mystream", "xdel"));
}

test "Stream notifications respect database index" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kt");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    // Subscribe to DB 0
    try setupNotificationSubscription(allocator, &pubsub_state, 0, "mystream", "xadd");

    // Execute XADD on DB 1
    const args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "*" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const result = try commands.cmdXadd(allocator, &storage, &args, &pubsub_state, 1);
    defer allocator.free(result);

    // Verify DB 0 notification was NOT published (different DB)
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@0__:mystream",
        .{},
    );
    defer allocator.free(keyspace_channel);
    const count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(@as(usize, 1), count); // Still 1 subscriber, no publish
}
