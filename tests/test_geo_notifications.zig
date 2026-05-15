const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");
const config_mod = @import("../src/storage/config.zig");
const geo_cmds = @import("../src/commands/geo.zig");

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

// ============================================================================
// GEOSEARCHSTORE Tests (GEOADD notifications will be tested in separate file once signature is updated)
// ============================================================================

test "GEOSEARCHSTORE fires notification on successful store" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEz"); // Enable keyspace + keyevent + sorted set
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Pre-populate source key directly with zadd (simulating geohash scores)
    // Palermo: lon=13.361389, lat=38.115556 encodes to a geohash
    // Catania: lon=15.087269, lat=37.502669 encodes to a geohash
    // These are simplified scores for testing (in reality they'd be actual geohash values)
    const scores = [_]f64{ 3481534591061, 3479099956230 };
    const members = [_][]const u8{ "Palermo", "Catania" };
    _ = try storage.zadd("source", &scores, &members, 0, null);

    // Subscribe to notifications for GEOSEARCHSTORE
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest", "geosearchstore");

    // Execute GEOSEARCHSTORE: GEOSEARCHSTORE dest source FROMLONLAT 15 37 BYRADIUS 200 km
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "15" },
        .{ .bulk_string = "37" },
        .{ .bulk_string = "BYRADIUS" },
        .{ .bulk_string = "200" },
        .{ .bulk_string = "km" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, db_index);
    defer allocator.free(search_result);

    // Verify notification was published
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "dest", "geosearchstore"));
}

test "GEOSEARCHSTORE no notification when 0 results" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEz");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Pre-populate source key with single location
    const scores = [_]f64{3481534591061}; // Palermo
    const members = [_][]const u8{"Palermo"};
    _ = try storage.zadd("source", &scores, &members, 0, null);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest", "geosearchstore");

    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:dest",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute GEOSEARCHSTORE with coordinates far from any location
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "BYRADIUS" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "m" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, db_index);
    defer allocator.free(search_result);

    // Verify no notification (count stays same)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "GEOSEARCHSTORE with COUNT limit fires notification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Ez"); // Enable keyevent + sorted set
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Pre-populate source key with cities
    const scores = [_]f64{ 3481534591061, 3479099956230 };
    const members = [_][]const u8{ "Palermo", "Catania" };
    _ = try storage.zadd("cities", &scores, &members, 0, null);

    // Subscribe to keyevent for geosearchstore
    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:geosearchstore",
        .{db_index},
    );
    defer allocator.free(keyevent_channel);
    _ = try pubsub_state.subscribe(1, keyevent_channel);

    const initial_count = pubsub_state.getSubscriberCount(keyevent_channel);

    // Execute GEOSEARCHSTORE with COUNT limit
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "result" },
        .{ .bulk_string = "cities" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "15" },
        .{ .bulk_string = "37" },
        .{ .bulk_string = "BYRADIUS" },
        .{ .bulk_string = "300" },
        .{ .bulk_string = "km" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "1" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, db_index);
    defer allocator.free(search_result);

    // Verify notification fired
    const final_count = pubsub_state.getSubscriberCount(keyevent_channel);
    try testing.expect(final_count > initial_count);
}

test "GEOSEARCHSTORE respects disabled notifications" {
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

    // Pre-populate source
    const scores = [_]f64{3481534591061};
    const members = [_][]const u8{"point1"};
    _ = try storage.zadd("src", &scores, &members, 0, null);

    // Subscribe to notifications (but they shouldn't fire)
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dst", "geosearchstore");

    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@{d}__:dst",
        .{db_index},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Execute GEOSEARCHSTORE
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "dst" },
        .{ .bulk_string = "src" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "1.0" },
        .{ .bulk_string = "1.0" },
        .{ .bulk_string = "BYRADIUS" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "km" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, db_index);
    defer allocator.free(search_result);

    // Verify no notification (count stays same)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count);
}

test "GEOSEARCHSTORE overwrites existing key" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEz");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Pre-populate destination with a string value
    _ = try storage.set("dest", "old_value", null);

    // Pre-populate source with location
    const scores = [_]f64{3481534591061};
    const members = [_][]const u8{"p1"};
    _ = try storage.zadd("source", &scores, &members, 0, null);

    // Subscribe to notifications
    try setupNotificationSubscription(allocator, &pubsub_state, db_index, "dest", "geosearchstore");

    // Execute GEOSEARCHSTORE (should overwrite string with sorted set)
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "1.0" },
        .{ .bulk_string = "1.0" },
        .{ .bulk_string = "BYRADIUS" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "km" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, db_index);
    defer allocator.free(search_result);

    // Verify notification was published (overwrite case)
    try testing.expect(try checkNotification(allocator, &pubsub_state, db_index, "dest", "geosearchstore"));
}

test "GEOSEARCHSTORE BYBOX variant fires notification" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "Ez");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    const db_index: u32 = 0;

    // Pre-populate source with locations
    const scores = [_]f64{ 3481534591061, 3479099956230 };
    const members = [_][]const u8{ "p1", "p2" };
    _ = try storage.zadd("boxes", &scores, &members, 0, null);

    // Subscribe to keyevent for geosearchstore
    const keyevent_channel = try std.fmt.allocPrint(
        allocator,
        "__keyevent@{d}__:geosearchstore",
        .{db_index},
    );
    defer allocator.free(keyevent_channel);
    _ = try pubsub_state.subscribe(1, keyevent_channel);

    const initial_count = pubsub_state.getSubscriberCount(keyevent_channel);

    // Execute GEOSEARCHSTORE with BYBOX
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "result" },
        .{ .bulk_string = "boxes" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "1.5" },
        .{ .bulk_string = "1.5" },
        .{ .bulk_string = "BYBOX" },
        .{ .bulk_string = "200" },
        .{ .bulk_string = "200" },
        .{ .bulk_string = "km" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, db_index);
    defer allocator.free(search_result);

    // Verify notification fired
    const final_count = pubsub_state.getSubscriberCount(keyevent_channel);
    try testing.expect(final_count > initial_count);
}

test "Geo notifications respect database index separation" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    var config = Config.init(allocator);
    defer config.deinit();
    try config.set("notify-keyspace-events", "KEz");
    storage.config = &config;

    var pubsub_state = PubSub.init(allocator);
    defer pubsub_state.deinit();

    // Pre-populate DB 0 with location (using zadd directly, db_index is passed to GEOSEARCHSTORE)
    const scores_db0 = [_]f64{3481534591061};
    const members_db0 = [_][]const u8{"p1"};
    _ = try storage.zadd("db0_key", &scores_db0, &members_db0, 0, null);

    // Subscribe to DB 0 notifications only
    try setupNotificationSubscription(allocator, &pubsub_state, 0, "db0_key", "geosearchstore");

    const keyspace_channel = try std.fmt.allocPrint(
        allocator,
        "__keyspace@0__:db0_key",
        .{},
    );
    defer allocator.free(keyspace_channel);
    const initial_count = pubsub_state.getSubscriberCount(keyspace_channel);

    // Pre-populate DB 1 with location
    const scores_db1 = [_]f64{3479099956230};
    const members_db1 = [_][]const u8{"p3"};
    _ = try storage.zadd("db1_key", &scores_db1, &members_db1, 0, null);

    // Execute GEOSEARCHSTORE on DB 1 (different database index)
    const search_args = [_]RespValue{
        .{ .bulk_string = "GEOSEARCHSTORE" },
        .{ .bulk_string = "db1_dest" },
        .{ .bulk_string = "db1_key" },
        .{ .bulk_string = "FROMLONLAT" },
        .{ .bulk_string = "15" },
        .{ .bulk_string = "37" },
        .{ .bulk_string = "BYRADIUS" },
        .{ .bulk_string = "500" },
        .{ .bulk_string = "km" },
    };
    const search_result = try geo_cmds.cmdGeosearchstore(allocator, &storage, &search_args, &pubsub_state, 1);
    defer allocator.free(search_result);

    // Verify DB 0 notification was NOT published (different DB)
    const final_count = pubsub_state.getSubscriberCount(keyspace_channel);
    try testing.expectEqual(initial_count, final_count); // Count stays at 1 subscriber, no publish
}
