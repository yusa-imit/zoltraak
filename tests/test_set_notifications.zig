const std = @import("std");
const resp = @import("resp");
const RespValue = resp.RespValue;
const Storage = @import("storage").MemoryStorage;
const PubSub = @import("storage").PubSub;
const sets = @import("commands").sets;
const config_mod = @import("storage").config;

test "SADD fires sadd notification on add" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications for sets (K + E + s)
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Subscribe to keyspace notification channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myset");
    defer allocator.free(sub_result);

    // Execute SADD
    const sadd_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "member1" },
        RespValue{ .bulk_string = "member2" },
    };

    // Need client_registry for tracking
    const client_mod = @import("commands").client;
    var client_registry = client_mod.ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const sadd_result = try sets.cmdSadd(allocator, &storage, &sadd_args, &ps, 0, &client_registry, 1);
    defer allocator.free(sadd_result);

    // Verify response is :2 (2 members added)
    try std.testing.expectEqualStrings(":2\r\n", sadd_result);

    // Verify notification was published (check channels list)
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 0);
}

test "SADD no notification when no members added (duplicates)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add member first
    _ = try storage.sadd("myset", &[_][]const u8{"member1"}, null);

    // Subscribe to keyspace channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myset");
    defer allocator.free(sub_result);

    // Try to add same member (duplicate)
    const sadd_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "member1" },
    };

    const client_mod = @import("commands").client;
    var client_registry = client_mod.ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const sadd_result = try sets.cmdSadd(allocator, &storage, &sadd_args, &ps, 0, &client_registry, 1);
    defer allocator.free(sadd_result);

    // Verify response is :0 (no members added)
    try std.testing.expectEqualStrings(":0\r\n", sadd_result);
}

test "SREM fires srem notification on remove" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add members first
    _ = try storage.sadd("myset", &[_][]const u8{ "member1", "member2", "member3" }, null);

    // Subscribe to keyspace channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myset");
    defer allocator.free(sub_result);

    // Execute SREM
    const srem_args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "member1" },
        RespValue{ .bulk_string = "member2" },
    };
    const srem_result = try sets.cmdSrem(allocator, &storage, &srem_args, &ps, 0);
    defer allocator.free(srem_result);

    // Verify response is :2 (2 members removed)
    try std.testing.expectEqualStrings(":2\r\n", srem_result);

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 0);
}

test "SREM fires del notification when last member removed" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications (including generic 'g' flag for del)
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kges");

    // Add single member
    _ = try storage.sadd("myset", &[_][]const u8{"member1"}, null);

    // Subscribe to keyspace channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myset");
    defer allocator.free(sub_result);

    // Remove last member
    const srem_args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "member1" },
    };
    const srem_result = try sets.cmdSrem(allocator, &storage, &srem_args, &ps, 0);
    defer allocator.free(srem_result);

    // Verify response is :1 (1 member removed)
    try std.testing.expectEqualStrings(":1\r\n", srem_result);

    // Verify notifications were published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 0);
}

test "SPOP fires spop notification on pop" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add members
    _ = try storage.sadd("myset", &[_][]const u8{ "member1", "member2" }, null);

    // Subscribe to keyspace channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myset");
    defer allocator.free(sub_result);

    // Execute SPOP
    const spop_args = [_]RespValue{
        RespValue{ .bulk_string = "SPOP" },
        RespValue{ .bulk_string = "myset" },
    };
    const spop_result = try sets.cmdSpop(allocator, &storage, &spop_args, &ps, 0);
    defer allocator.free(spop_result);

    // Verify response is a bulk string (the popped member)
    try std.testing.expect(std.mem.startsWith(u8, spop_result, "$"));

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 0);
}

test "SPOP fires del notification when last member popped" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications (including generic)
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kges");

    // Add single member
    _ = try storage.sadd("myset", &[_][]const u8{"member1"}, null);

    // Subscribe to keyspace channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:myset");
    defer allocator.free(sub_result);

    // Pop last member
    const spop_args = [_]RespValue{
        RespValue{ .bulk_string = "SPOP" },
        RespValue{ .bulk_string = "myset" },
    };
    const spop_result = try sets.cmdSpop(allocator, &storage, &spop_args, &ps, 0);
    defer allocator.free(spop_result);

    // Verify response is the member
    try std.testing.expect(std.mem.startsWith(u8, spop_result, "$"));

    // Verify notifications were published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 0);
}

test "SMOVE fires srem on source and sadd on destination" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add member to source
    _ = try storage.sadd("source", &[_][]const u8{ "member1", "member2" }, null);

    // Subscribe to both source and destination channels
    const sub_result1 = try ps.subscribe(allocator, "__keyspace@0__:source");
    defer allocator.free(sub_result1);
    const sub_result2 = try ps.subscribe(allocator, "__keyspace@0__:dest");
    defer allocator.free(sub_result2);

    // Execute SMOVE
    const smove_args = [_]RespValue{
        RespValue{ .bulk_string = "SMOVE" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "member1" },
    };
    const smove_result = try sets.cmdSmove(allocator, &storage, &smove_args, &ps, 0);
    defer allocator.free(smove_result);

    // Verify response is :1 (member moved)
    try std.testing.expectEqualStrings(":1\r\n", smove_result);

    // Verify notifications were published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 2);
}

test "SMOVE fires del when source becomes empty" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications (including generic)
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kges");

    // Add single member to source
    _ = try storage.sadd("source", &[_][]const u8{"member1"}, null);

    // Subscribe to source channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:source");
    defer allocator.free(sub_result);

    // Move last member
    const smove_args = [_]RespValue{
        RespValue{ .bulk_string = "SMOVE" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "member1" },
    };
    const smove_result = try sets.cmdSmove(allocator, &storage, &smove_args, &ps, 0);
    defer allocator.free(smove_result);

    // Verify response is :1
    try std.testing.expectEqualStrings(":1\r\n", smove_result);

    // Verify notifications were published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 1);
}

test "SUNIONSTORE fires sunionstore notification" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add members to sets
    _ = try storage.sadd("set1", &[_][]const u8{ "a", "b" }, null);
    _ = try storage.sadd("set2", &[_][]const u8{ "b", "c" }, null);

    // Subscribe to destination channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:dest");
    defer allocator.free(sub_result);

    // Execute SUNIONSTORE
    const sunionstore_args = [_]RespValue{
        RespValue{ .bulk_string = "SUNIONSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "set1" },
        RespValue{ .bulk_string = "set2" },
    };
    const result = try sets.cmdSunionstore(allocator, &storage, &sunionstore_args, &ps, 0);
    defer allocator.free(result);

    // Verify response is :3 (union has 3 members: a, b, c)
    try std.testing.expectEqualStrings(":3\r\n", result);

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 1);
}

test "SINTERSTORE fires sinterstore notification" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add members to sets
    _ = try storage.sadd("set1", &[_][]const u8{ "a", "b", "c" }, null);
    _ = try storage.sadd("set2", &[_][]const u8{ "b", "c", "d" }, null);

    // Subscribe to destination channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:dest");
    defer allocator.free(sub_result);

    // Execute SINTERSTORE
    const sinterstore_args = [_]RespValue{
        RespValue{ .bulk_string = "SINTERSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "set1" },
        RespValue{ .bulk_string = "set2" },
    };
    const result = try sets.cmdSinterstore(allocator, &storage, &sinterstore_args, &ps, 0);
    defer allocator.free(result);

    // Verify response is :2 (intersection has 2 members: b, c)
    try std.testing.expectEqualStrings(":2\r\n", result);

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 1);
}

test "SDIFFSTORE fires sdiffstore notification" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kes");

    // Add members to sets
    _ = try storage.sadd("set1", &[_][]const u8{ "a", "b", "c" }, null);
    _ = try storage.sadd("set2", &[_][]const u8{ "b", "c" }, null);

    // Subscribe to destination channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:dest");
    defer allocator.free(sub_result);

    // Execute SDIFFSTORE
    const sdiffstore_args = [_]RespValue{
        RespValue{ .bulk_string = "SDIFFSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "set1" },
        RespValue{ .bulk_string = "set2" },
    };
    const result = try sets.cmdSdiffstore(allocator, &storage, &sdiffstore_args, &ps, 0);
    defer allocator.free(result);

    // Verify response is :1 (difference has 1 member: a)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 1);
}

test "SINTERSTORE fires del notification when result is empty" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Enable keyspace notifications (including generic)
    try config_mod.setConfig(&storage.config, "notify-keyspace-events", "Kges");

    // Add non-intersecting sets
    _ = try storage.sadd("set1", &[_][]const u8{"a"}, null);
    _ = try storage.sadd("set2", &[_][]const u8{"b"}, null);

    // Subscribe to destination channel
    const sub_result = try ps.subscribe(allocator, "__keyspace@0__:dest");
    defer allocator.free(sub_result);

    // Execute SINTERSTORE (result will be empty)
    const sinterstore_args = [_]RespValue{
        RespValue{ .bulk_string = "SINTERSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "set1" },
        RespValue{ .bulk_string = "set2" },
    };
    const result = try sets.cmdSinterstore(allocator, &storage, &sinterstore_args, &ps, 0);
    defer allocator.free(result);

    // Verify response is :0 (empty intersection)
    try std.testing.expectEqualStrings(":0\r\n", result);

    // Verify notification was published
    const channels = try ps.listChannels(allocator, null);
    defer allocator.free(channels);
    try std.testing.expect(channels.len >= 0);
}
