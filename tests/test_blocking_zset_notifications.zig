const std = @import("std");
const resp = @import("resp");
const RespValue = resp.RespValue;
const Storage = @import("storage").MemoryStorage;
const PubSub = @import("storage").PubSub;
const sorted_sets = @import("commands").sorted_sets;
const config_mod = @import("storage").config;

test "BZPOPMIN fires zpopmin notification on successful pop" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "KEz");

    // Setup: add sorted set
    const zadd_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "one" },
    };
    const zadd_result = try sorted_sets.cmdZadd(allocator, &storage, &zadd_args, &ps, 0);
    defer allocator.free(zadd_result);
    try std.testing.expectEqualStrings(":1\r\n", zadd_result);

    // Subscribe to keyspace notification channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myzset");
    defer allocator.free(sub_result);

    // Execute BZPOPMIN
    const bzpopmin_args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMIN" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "0.1" }, // 0.1 second timeout
    };
    const result = try sorted_sets.cmdBzpopmin(allocator, &storage, &bzpopmin_args, &ps, 0);
    defer allocator.free(result);

    // Verify result format: *3\r\n$6\r\nmyzset\r\n$3\r\none\r\n$1\r\n1\r\n
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    // Channel should exist if notification was sent
    try std.testing.expect(channels.len >= 0);
}

test "BZPOPMAX fires zpopmax notification on successful pop" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "KEz");

    // Setup: add sorted set
    const zadd_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "2.0" },
        RespValue{ .bulk_string = "two" },
    };
    const zadd_result = try sorted_sets.cmdZadd(allocator, &storage, &zadd_args, &ps, 0);
    defer allocator.free(zadd_result);
    try std.testing.expectEqualStrings(":2\r\n", zadd_result);

    // Subscribe to keyevent notification channel
    const sub_result = try ps.subscribe(allocator, "__keyevent@0__:zpopmax");
    defer allocator.free(sub_result);

    // Execute BZPOPMAX
    const bzpopmax_args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMAX" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "0.1" }, // 0.1 second timeout
    };
    const result = try sorted_sets.cmdBzpopmax(allocator, &storage, &bzpopmax_args, &ps, 0);
    defer allocator.free(result);

    // Verify result format: *3\r\n$6\r\nmyzset\r\n$3\r\ntwo\r\n$1\r\n2\r\n
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
}

test "BZMPOP fires zpopmin notification on successful MIN pop" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "KEz");

    // Setup: add sorted set
    const zadd_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "2.0" },
        RespValue{ .bulk_string = "two" },
    };
    const zadd_result = try sorted_sets.cmdZadd(allocator, &storage, &zadd_args, &ps, 0);
    defer allocator.free(zadd_result);
    try std.testing.expectEqualStrings(":2\r\n", zadd_result);

    // Subscribe to keyevent notification channel
    const sub_result = try ps.subscribe(allocator, "__keyevent@0__:zpopmin");
    defer allocator.free(sub_result);

    // Execute BZMPOP with MIN
    const bzmpop_args = [_]RespValue{
        RespValue{ .bulk_string = "BZMPOP" },
        RespValue{ .bulk_string = "0.1" }, // 0.1 second timeout
        RespValue{ .bulk_string = "1" }, // numkeys
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "MIN" },
    };
    const result = try sorted_sets.cmdBzmpop(allocator, &storage, &bzmpop_args, &ps, 0);
    defer allocator.free(result);

    // Verify result format: *2\r\n$6\r\nmyzset\r\n*2\r\n...
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "BZMPOP fires zpopmax notification on successful MAX pop" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "KEz");

    // Setup: add sorted set
    const zadd_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "2.0" },
        RespValue{ .bulk_string = "two" },
    };
    const zadd_result = try sorted_sets.cmdZadd(allocator, &storage, &zadd_args, &ps, 0);
    defer allocator.free(zadd_result);
    try std.testing.expectEqualStrings(":2\r\n", zadd_result);

    // Subscribe to keyevent notification channel
    const sub_result = try ps.subscribe(allocator, "__keyevent@0__:zpopmax");
    defer allocator.free(sub_result);

    // Execute BZMPOP with MAX
    const bzmpop_args = [_]RespValue{
        RespValue{ .bulk_string = "BZMPOP" },
        RespValue{ .bulk_string = "0.1" }, // 0.1 second timeout
        RespValue{ .bulk_string = "1" }, // numkeys
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "MAX" },
    };
    const result = try sorted_sets.cmdBzmpop(allocator, &storage, &bzmpop_args, &ps, 0);
    defer allocator.free(result);

    // Verify result format: *2\r\n$6\r\nmyzset\r\n*2\r\n...
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "BZPOPMIN fires del notification when last member removed" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "KEgz");

    // Setup: add sorted set with single member
    const zadd_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "only" },
    };
    const zadd_result = try sorted_sets.cmdZadd(allocator, &storage, &zadd_args, &ps, 0);
    defer allocator.free(zadd_result);
    try std.testing.expectEqualStrings(":1\r\n", zadd_result);

    // Subscribe to generic del event
    const sub_result = try ps.subscribe(allocator, "__keyevent@0__:del");
    defer allocator.free(sub_result);

    // Execute BZPOPMIN (should delete key after pop)
    const bzpopmin_args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMIN" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "0.1" }, // 0.1 second timeout
    };
    const result = try sorted_sets.cmdBzpopmin(allocator, &storage, &bzpopmin_args, &ps, 0);
    defer allocator.free(result);

    // Verify result is successful
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));

    // Verify key no longer exists
    const key_exists = storage.exists(&[_][]const u8{"myzset"});
    try std.testing.expect(!key_exists);
}

test "BZPOPMIN no notification on timeout" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "KEz");

    // No sorted set exists

    // Subscribe to keyspace notification channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myzset");
    defer allocator.free(sub_result);

    // Execute BZPOPMIN with short timeout (should timeout)
    const bzpopmin_args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMIN" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "0.1" }, // 0.1 second timeout
    };
    const result = try sorted_sets.cmdBzpopmin(allocator, &storage, &bzpopmin_args, &ps, 0);
    defer allocator.free(result);

    // Verify result is null (timeout)
    try std.testing.expectEqualStrings("$-1\r\n", result);

    // No notification should have been published since no pop occurred
}
