const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const storage_mod = @import("../src/storage/memory.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");
const config_mod = @import("../src/storage/config.zig");
const commands = @import("../src/commands/hyperloglog.zig");

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

/// Helper: Check if notification was published by comparing subscriber counts
fn checkNotificationFired(
    allocator: std.mem.Allocator,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event: []const u8,
) !bool {
    // Check keyspace channel has subscribers
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:{s}",
        .{ db_index, key },
    );
    defer allocator.free(keyspace_channel);
    const keyspace_subs = pubsub_state.getSubscriberCount(keyspace_channel);

    // Check keyevent channel has subscribers
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
// PFADD Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "PFADD fires notification when registers updated (first add)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "hll1", "pfadd");

    // Execute PFADD - first add should update registers
    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll1" },
        .{ .bulk_string = "elem1" },
        .{ .bulk_string = "elem2" },
    };
    const result = try commands.cmdPfadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "hll1", "pfadd"));
}

test "PFADD does NOT fire notification when no update (re-add same elements)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Add initial elements
    var args1 = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll2" },
        .{ .bulk_string = "elem1" },
        .{ .bulk_string = "elem2" },
    };
    const result1 = try commands.cmdPfadd(allocator, &storage, &args1, &pubsub_state, db_index);
    allocator.free(result1);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "hll2", "pfadd");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:hll2",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_keyspace_count = pubsub_state.getSubscriberCount(keyspace_channel);

    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:pfadd",
        .{db_index},
    );
    defer allocator.free(keyevent_channel);
    const initial_keyevent_count = pubsub_state.getSubscriberCount(keyevent_channel);

    // Re-add same elements (should NOT update registers)
    var args2 = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll2" },
        .{ .bulk_string = "elem1" },
        .{ .bulk_string = "elem2" },
    };
    const result2 = try commands.cmdPfadd(allocator, &storage, &args2, &pubsub_state, db_index);
    allocator.free(result2);

    // Verify subscriber counts stayed the same (no new notification published)
    const final_keyspace_count = pubsub_state.getSubscriberCount(keyspace_channel);
    const final_keyevent_count = pubsub_state.getSubscriberCount(keyevent_channel);

    try testing.expectEqual(initial_keyspace_count, final_keyspace_count);
    try testing.expectEqual(initial_keyevent_count, final_keyevent_count);
}

test "PFADD fires notification on auto-create (new key)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications BEFORE first PFADD
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "hll_new", "pfadd");

    // Execute PFADD on non-existent key
    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll_new" },
        .{ .bulk_string = "a" },
    };
    const result = try commands.cmdPfadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify notification was published for new key creation
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "hll_new", "pfadd"));
}

test "PFADD fires both keyspace and keyevent with K$ flags" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Explicitly both K and $
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to both channels
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "hll3", "pfadd");

    // Execute PFADD
    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll3" },
        .{ .bulk_string = "x" },
    };
    const result = try commands.cmdPfadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify BOTH channels have subscribers (notification fired on both)
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:hll3",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    try testing.expect(pubsub_state.getSubscriberCount(keyspace_channel) > 0);

    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:pfadd",
        .{db_index},
    );
    defer allocator.free(keyevent_channel);
    try testing.expect(pubsub_state.getSubscriberCount(keyevent_channel) > 0);
}

test "PFADD respects disabled notifications (config=\"\")" {
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

    // Subscribe to notifications (but they should NOT fire)
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "hll4", "pfadd");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:hll4",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute PFADD
    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll4" },
        .{ .bulk_string = "test" },
    };
    const result = try commands.cmdPfadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify subscriber count didn't increase (no publish happened)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "PFADD WRONGTYPE error does not fire notification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable notifications
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create a string value at key "wrongtype_key"
    var string_args = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "wrongtype_key" },
        .{ .bulk_string = "stringvalue" },
    };
    const strings_cmd = @import("../src/commands/strings.zig");
    const set_result = try strings_cmd.cmdSet(allocator, &storage, &string_args, &pubsub_state, db_index);
    allocator.free(set_result);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "wrongtype_key", "pfadd");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:wrongtype_key",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Try PFADD on string key (should fail with WRONGTYPE)
    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "wrongtype_key" },
        .{ .bulk_string = "elem" },
    };
    const result = try commands.cmdPfadd(allocator, &storage, &args, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify it was an error response
    try testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);

    // Verify subscriber count didn't increase (no notification fired)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// PFMERGE Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "PFMERGE fires notification on successful merge" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create source HyperLogLogs
    var pfadd1 = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "src1" },
        .{ .bulk_string = "a" },
        .{ .bulk_string = "b" },
    };
    const result1 = try commands.cmdPfadd(allocator, &storage, &pfadd1, &pubsub_state, db_index);
    allocator.free(result1);

    var pfadd2 = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "src2" },
        .{ .bulk_string = "c" },
        .{ .bulk_string = "d" },
    };
    const result2 = try commands.cmdPfadd(allocator, &storage, &pfadd2, &pubsub_state, db_index);
    allocator.free(result2);

    // Subscribe to destination key notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest", "pfadd");

    // Execute PFMERGE
    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "src1" },
        .{ .bulk_string = "src2" },
    };
    const merge_result = try commands.cmdPfmerge(allocator, &storage, &merge_args, &pubsub_state, db_index);
    allocator.free(merge_result);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "dest", "pfadd"));
}

test "PFMERGE fires notification even with empty sources" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to destination key notifications BEFORE merge
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest_empty", "pfadd");

    // Execute PFMERGE with non-existent source keys (all empty)
    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "dest_empty" },
        .{ .bulk_string = "empty1" },
        .{ .bulk_string = "empty2" },
    };
    const merge_result = try commands.cmdPfmerge(allocator, &storage, &merge_args, &pubsub_state, db_index);
    allocator.free(merge_result);

    // Verify notification was published even with empty sources
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "dest_empty", "pfadd"));
}

test "PFMERGE fires notification when destkey already exists (overwrite)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create initial destination HyperLogLog
    var pfadd_dest = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "dest_exist" },
        .{ .bulk_string = "old1" },
    };
    const dest_result = try commands.cmdPfadd(allocator, &storage, &pfadd_dest, &pubsub_state, db_index);
    allocator.free(dest_result);

    // Create source
    var pfadd_src = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "src_exist" },
        .{ .bulk_string = "new1" },
    };
    const src_result = try commands.cmdPfadd(allocator, &storage, &pfadd_src, &pubsub_state, db_index);
    allocator.free(src_result);

    // Subscribe to destination key notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest_exist", "pfadd");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:dest_exist",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute PFMERGE (overwrites destination)
    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "dest_exist" },
        .{ .bulk_string = "src_exist" },
    };
    const merge_result = try commands.cmdPfmerge(allocator, &storage, &merge_args, &pubsub_state, db_index);
    allocator.free(merge_result);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "dest_exist", "pfadd"));
}

test "PFMERGE respects disabled notifications (config=\"\")" {
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

    // Create source
    var pfadd_src = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "src_noflag" },
        .{ .bulk_string = "x" },
    };
    const src_result = try commands.cmdPfadd(allocator, &storage, &pfadd_src, &pubsub_state, db_index);
    allocator.free(src_result);

    // Subscribe to notifications (but they should NOT fire)
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest_noflag", "pfadd");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:dest_noflag",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute PFMERGE
    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "dest_noflag" },
        .{ .bulk_string = "src_noflag" },
    };
    const merge_result = try commands.cmdPfmerge(allocator, &storage, &merge_args, &pubsub_state, db_index);
    allocator.free(merge_result);

    // Verify subscriber count didn't increase (no publish happened)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "PFMERGE WRONGTYPE error does not fire notification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable notifications
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create a string value at source key (wrong type)
    var string_args = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "src_wrongtype" },
        .{ .bulk_string = "notahll" },
    };
    const strings_cmd = @import("../src/commands/strings.zig");
    const set_result = try strings_cmd.cmdSet(allocator, &storage, &string_args, &pubsub_state, db_index);
    allocator.free(set_result);

    // Subscribe to destination key notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest_wrongtype", "pfadd");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:dest_wrongtype",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Try PFMERGE with wrong source type (should fail with WRONGTYPE)
    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "dest_wrongtype" },
        .{ .bulk_string = "src_wrongtype" },
    };
    const merge_result = try commands.cmdPfmerge(allocator, &storage, &merge_args, &pubsub_state, db_index);
    defer allocator.free(merge_result);

    // Verify it was an error response
    try testing.expect(std.mem.indexOf(u8, merge_result, "WRONGTYPE") != null);

    // Verify subscriber count didn't increase (no notification fired)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// Database Separation Test
// ─────────────────────────────────────────────────────────────────────────────

test "Notifications respect database index separation (DB 0 vs DB 1)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "K$"); // Enable keyspace + string
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    // Subscribe to DB 0 notifications only
    try setupNotificationSubscription(allocator, &pubsub_state, 0, "hll_db", "pfadd");

    // Get DB 0 keyspace channel subscriber count
    const db0_keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@0__:hll_db",
        .{},
    );
    defer allocator.free(db0_keyspace_channel);
    const db0_initial_count = pubsub_state.getSubscriberCount(db0_keyspace_channel);

    // Execute PFADD in DB 1 (different database)
    const db_index: u32 = 1;
    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll_db" },
        .{ .bulk_string = "elem" },
    };
    const result = try commands.cmdPfadd(allocator, &storage, &args, &pubsub_state, db_index);
    allocator.free(result);

    // Verify DB 0 subscriber count stayed the same (no cross-DB pollution)
    const db0_final_count = pubsub_state.getSubscriberCount(db0_keyspace_channel);
    try testing.expectEqual(db0_initial_count, db0_final_count);

    // Verify DB 1 channel has subscribers (notification fired in correct DB)
    const db1_keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@1__:hll_db",
        .{},
    );
    defer allocator.free(db1_keyspace_channel);
    try testing.expect(pubsub_state.getSubscriberCount(db1_keyspace_channel) > 0);
}
