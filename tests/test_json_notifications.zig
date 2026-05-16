const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const storage_mod = @import("../src/storage/memory.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");
const config_mod = @import("../src/storage/config.zig");
const commands = @import("../src/commands/json.zig");

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
    const keyspace_subs = pubsub_state.channelSubscriberCount(keyspace_channel);

    // Check keyevent channel has subscribers
    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:{s}",
        .{ db_index, event },
    );
    defer allocator.free(keyevent_channel);
    const keyevent_subs = pubsub_state.channelSubscriberCount(keyevent_channel);

    return keyspace_subs > 0 and keyevent_subs > 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.SET Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "JSON.SET fires notification on successful set" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd"); // Enable keyspace + module (JSON)
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc1", "json.set");

    // Execute JSON.SET
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc1" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"name\":\"Alice\"}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "doc1", "json.set"));
}

test "JSON.SET respects NX condition (doesn't fire when key exists)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create initial JSON
    var args1 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_nx" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"a\":1}" },
    };
    const result1 = try commands.cmdJsonSet(&storage, &args1, allocator, &pubsub_state, db_index);
    allocator.free(result1);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_nx", "json.set");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:doc_nx",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_keyspace_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Try JSON.SET with NX (should not notify since key exists)
    var args2 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_nx" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"b\":2}" },
        .{ .bulk_string = "NX" },
    };
    const result2 = try commands.cmdJsonSet(&storage, &args2, allocator, &pubsub_state, db_index);
    allocator.free(result2);

    // Verify subscriber count stayed the same (no new notification)
    const final_keyspace_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_keyspace_count, final_keyspace_count);
}

test "JSON.SET respects XX condition (fires when key exists)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create initial JSON
    var args1 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_xx" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"a\":1}" },
    };
    const result1 = try commands.cmdJsonSet(&storage, &args1, allocator, &pubsub_state, db_index);
    allocator.free(result1);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_xx", "json.set");

    // Execute JSON.SET with XX (should notify since key exists)
    var args2 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_xx" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"b\":2}" },
        .{ .bulk_string = "XX" },
    };
    const result2 = try commands.cmdJsonSet(&storage, &args2, allocator, &pubsub_state, db_index);
    defer allocator.free(result2);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "doc_xx", "json.set"));
}

test "JSON.SET fires notification on auto-create (new key)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications BEFORE first set
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_new", "json.set");

    // Execute JSON.SET on non-existent key
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_new" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"x\":10}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "doc_new", "json.set"));
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.DEL Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "JSON.DEL fires notification when deleting entire key" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create initial JSON
    var args1 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_del" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"data\":\"value\"}" },
    };
    const result1 = try commands.cmdJsonSet(&storage, &args1, allocator, &pubsub_state, db_index);
    allocator.free(result1);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_del", "json.del");

    // Execute JSON.DEL
    var args2 = [_]RespValue{
        .{ .bulk_string = "JSON.DEL" },
        .{ .bulk_string = "doc_del" },
    };
    const result2 = try commands.cmdJsonDel(&storage, &args2, allocator, &pubsub_state, db_index);
    defer allocator.free(result2);

    // Verify notification was published
    try testing.expect(try checkNotificationFired(allocator, &pubsub_state, db_index, "doc_del", "json.del"));
}

test "JSON.DEL does NOT fire notification when key not found" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications for non-existent key
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "nonexistent_doc", "json.del");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:nonexistent_doc",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Try JSON.DEL on non-existent key
    var args = [_]RespValue{
        .{ .bulk_string = "JSON.DEL" },
        .{ .bulk_string = "nonexistent_doc" },
    };
    const result = try commands.cmdJsonDel(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify subscriber count stayed the same (no publish happened)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// Configuration and Flag Tests
// ─────────────────────────────────────────────────────────────────────────────

test "JSON notifications respect disabled config (config=\"\")" {
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
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_nonotif", "json.set");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:doc_nonotif",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute JSON.SET
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_nonotif" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"test\":true}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify subscriber count didn't increase (no publish happened)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "JSON notifications fire with both K and d flags" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd"); // Both K and d flags
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to both channels
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_both", "json.set");

    // Execute JSON.SET
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_both" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"flag\":\"test\"}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify BOTH channels have subscribers
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:doc_both",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    try testing.expect(pubsub_state.getSubscriberCount(keyspace_channel) > 0);

    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:json.set",
        .{db_index},
    );
    defer allocator.free(keyevent_channel);
    try testing.expect(pubsub_state.getSubscriberCount(keyevent_channel) > 0);
}

test "JSON notifications respect database index separation (DB 0 vs DB 1)" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    // Subscribe to DB 0 notifications only
    try setupNotificationSubscription(allocator, &pubsub_state, 0, "doc_db", "json.set");

    // Get DB 0 keyspace channel subscriber count
    const db0_keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@0__:doc_db",
        .{},
    );
    defer allocator.free(db0_keyspace_channel);
    const db0_initial_count = pubsub_state.getSubscriberCount(db0_keyspace_channel);

    // Execute JSON.SET in DB 1 (different database)
    const db_index: u32 = 1;
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_db" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"db\":1}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    allocator.free(result);

    // Verify DB 0 subscriber count stayed the same (no cross-DB pollution)
    const db0_final_count = pubsub_state.getSubscriberCount(db0_keyspace_channel);
    try testing.expectEqual(db0_initial_count, db0_final_count);

    // Verify DB 1 channel has subscribers (notification fired in correct DB)
    const db1_keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@1__:doc_db",
        .{},
    );
    defer allocator.free(db1_keyspace_channel);
    try testing.expect(pubsub_state.getSubscriberCount(db1_keyspace_channel) > 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// WRONGTYPE Error Tests
// ─────────────────────────────────────────────────────────────────────────────

test "JSON.SET WRONGTYPE error does not fire notification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
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
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "wrongtype_key", "json.set");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:wrongtype_key",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Try JSON.SET on string key (should fail with WRONGTYPE)
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "wrongtype_key" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify it was an error response
    try testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);

    // Verify subscriber count didn't increase (no notification fired)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "JSON.DEL WRONGTYPE error does not fire notification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Create a string value at key
    var string_args = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "wrongtype_del_key" },
        .{ .bulk_string = "notjson" },
    };
    const strings_cmd = @import("../src/commands/strings.zig");
    const set_result = try strings_cmd.cmdSet(allocator, &storage, &string_args, &pubsub_state, db_index);
    allocator.free(set_result);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "wrongtype_del_key", "json.del");

    // Get initial subscriber counts
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:wrongtype_del_key",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Try JSON.DEL on string key (should fail with WRONGTYPE)
    var args = [_]RespValue{
        .{ .bulk_string = "JSON.DEL" },
        .{ .bulk_string = "wrongtype_del_key" },
    };
    const result = try commands.cmdJsonDel(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify it was an error response
    try testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);

    // Verify subscriber count didn't increase (no notification fired)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional Command Notification Tests
// ─────────────────────────────────────────────────────────────────────────────

test "JSON write commands use module flag 'd' correctly" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    // Set only 'd' flag (module only, no keyspace)
    try config.set("notify-keyspace-events", "d");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to keyevent channel only
    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:json.set",
        .{db_index},
    );
    defer allocator.free(keyevent_channel);
    _ = try pubsub_state.subscribe(1, keyevent_channel);

    // Execute JSON.SET
    const args = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_d_flag" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"test\":1}" },
    };
    const result = try commands.cmdJsonSet(&storage, &args, allocator, &pubsub_state, db_index);
    defer allocator.free(result);

    // Verify keyevent channel has subscribers (module flag enables it)
    try testing.expect(pubsub_state.getSubscriberCount(keyevent_channel) > 0);
}

test "JSON.SET multiple consecutive calls fire multiple notifications" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "doc_multi", "json.set");

    // Get initial subscriber count
    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:doc_multi",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute first JSON.SET
    var args1 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_multi" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"v\":1}" },
    };
    const result1 = try commands.cmdJsonSet(&storage, &args1, allocator, &pubsub_state, db_index);
    allocator.free(result1);

    // Execute second JSON.SET (should be XX mode now)
    var args2 = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_multi" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"v\":2}" },
        .{ .bulk_string = "XX" },
    };
    const result2 = try commands.cmdJsonSet(&storage, &args2, allocator, &pubsub_state, db_index);
    allocator.free(result2);

    // Verify notification was fired for second set
    try testing.expect(pubsub_state.getSubscriberCount(keyspace_channel) > initial_count);
}

test "JSON.SET and JSON.DEL together fire separate notifications" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Kd");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Subscribe to both event types
    const keyevent_set_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:json.set",
        .{db_index},
    );
    defer allocator.free(keyevent_set_channel);
    _ = try pubsub_state.subscribe(1, keyevent_set_channel);

    const keyevent_del_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:json.del",
        .{db_index},
    );
    defer allocator.free(keyevent_del_channel);
    _ = try pubsub_state.subscribe(1, keyevent_del_channel);

    // Execute JSON.SET
    var args_set = [_]RespValue{
        .{ .bulk_string = "JSON.SET" },
        .{ .bulk_string = "doc_combined" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "{\"a\":1}" },
    };
    const result_set = try commands.cmdJsonSet(&storage, &args_set, allocator, &pubsub_state, db_index);
    allocator.free(result_set);

    // Execute JSON.DEL
    var args_del = [_]RespValue{
        .{ .bulk_string = "JSON.DEL" },
        .{ .bulk_string = "doc_combined" },
    };
    const result_del = try commands.cmdJsonDel(&storage, &args_del, allocator, &pubsub_state, db_index);
    allocator.free(result_del);

    // Verify both events have subscribers
    try testing.expect(pubsub_state.getSubscriberCount(keyevent_set_channel) > 0);
    try testing.expect(pubsub_state.getSubscriberCount(keyevent_del_channel) > 0);
}
