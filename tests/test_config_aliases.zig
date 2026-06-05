const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const keys_cmds = zoltraak.commands.keys_cmds;

// ── CONFIG alias sync tests ───────────────────────────────────────────────────
// Regression tests for Iteration 333: ziplist/listpack and slave/replica
// parameter aliases must stay in sync when either is set via CONFIG SET.

test "CONFIG alias: setting hash-max-ziplist-entries syncs to hash-max-listpack-entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set via the deprecated ziplist alias
    try storage.config.set("hash-max-ziplist-entries", @as([]const u8, "16"));

    // Both the alias and canonical must now return "16"
    const alias = try storage.config.getAsString("hash-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("16", alias.?);

    const canonical = try storage.config.getAsString("hash-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("16", canonical.?);
}

test "CONFIG alias: setting hash-max-listpack-entries syncs to hash-max-ziplist-entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("hash-max-listpack-entries", @as([]const u8, "32"));

    const canonical = try storage.config.getAsString("hash-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", canonical.?);

    const alias = try storage.config.getAsString("hash-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", alias.?);
}

test "CONFIG alias: setting zset-max-ziplist-entries syncs to zset-max-listpack-entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("zset-max-ziplist-entries", @as([]const u8, "8"));

    const canonical = try storage.config.getAsString("zset-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("8", canonical.?);
}

test "CONFIG alias: setting slave-serve-stale-data syncs to replica-serve-stale-data" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("slave-serve-stale-data", @as([]const u8, "no"));

    const canonical = try storage.config.getAsString("replica-serve-stale-data");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("no", canonical.?);

    // Setting canonical should sync back to alias
    try storage.config.set("replica-serve-stale-data", @as([]const u8, "yes"));
    const alias = try storage.config.getAsString("slave-serve-stale-data");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", alias.?);
}

test "CONFIG alias: min-slaves-to-write syncs to min-replicas-to-write" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("min-slaves-to-write", @as([]const u8, "3"));

    const canonical = try storage.config.getAsString("min-replicas-to-write");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("3", canonical.?);
}

// ── OBJECT ENCODING reflects ziplist alias ────────────────────────────────────
// Verify that OBJECT ENCODING reads the synced canonical config value,
// so changes via the ziplist alias actually affect encoding detection.

test "OBJECT ENCODING: hash-max-ziplist-entries alias controls encoding threshold" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set threshold to 2 via deprecated alias — must sync to hash-max-listpack-entries
    try storage.config.set("hash-max-ziplist-entries", @as([]const u8, "2"));

    // Add 3 entries to exceed the alias threshold of 2
    _ = try storage.hset("myhash", &[_][]const u8{ "f1", "f2", "f3" }, &[_][]const u8{ "v1", "v2", "v3" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "myhash" },
    };
    const result = try keys_cmds.cmdObject(allocator, storage, &args);
    defer allocator.free(result);
    // With 3 entries and threshold=2, encoding should be hashtable (not listpack)
    try std.testing.expectEqualStrings("$9\r\nhashtable\r\n", result);
}

test "OBJECT ENCODING: zset-max-ziplist-entries alias controls encoding threshold" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set threshold to 2 via deprecated alias — must sync to zset-max-listpack-entries
    try storage.config.set("zset-max-ziplist-entries", @as([]const u8, "2"));

    // Add 3 members to exceed the alias threshold of 2
    _ = try storage.zadd("myzset", &[_]f64{ 1.0, 2.0, 3.0 }, &[_][]const u8{ "a", "b", "c" }, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "myzset" },
    };
    const result = try keys_cmds.cmdObject(allocator, storage, &args);
    defer allocator.free(result);
    // With 3 members and threshold=2, encoding should be skiplist (not listpack)
    try std.testing.expectEqualStrings("$8\r\nskiplist\r\n", result);
}

// ── set-max-ziplist alias tests ───────────────────────────────────────────────
// Verify that set-max-ziplist-entries and set-max-ziplist-value aliases
// are synced with their canonical set-max-listpack-* counterparts.

test "CONFIG alias: set-max-ziplist-entries syncs to set-max-listpack-entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("set-max-ziplist-entries", @as([]const u8, "16"));

    const canonical = try storage.config.getAsString("set-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("16", canonical.?);

    const alias = try storage.config.getAsString("set-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("16", alias.?);
}

test "CONFIG alias: set-max-listpack-entries syncs to set-max-ziplist-entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("set-max-listpack-entries", @as([]const u8, "64"));

    const canonical = try storage.config.getAsString("set-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("64", canonical.?);

    const alias = try storage.config.getAsString("set-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("64", alias.?);
}

test "CONFIG alias: set-max-ziplist-value syncs to set-max-listpack-value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("set-max-ziplist-value", @as([]const u8, "32"));

    const canonical = try storage.config.getAsString("set-max-listpack-value");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", canonical.?);
}

// ── new config param defaults tests ──────────────────────────────────────────

test "CONFIG: list-compress-depth has correct default" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const val = try storage.config.getAsString("list-compress-depth");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("0", val.?);
}

test "CONFIG: stream-node-max-bytes and stream-node-max-entries have correct defaults" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const bytes = try storage.config.getAsString("stream-node-max-bytes");
    defer if (bytes) |v| allocator.free(v);
    try std.testing.expectEqualStrings("4096", bytes.?);

    const entries = try storage.config.getAsString("stream-node-max-entries");
    defer if (entries) |v| allocator.free(v);
    try std.testing.expectEqualStrings("100", entries.?);
}

test "CONFIG: acl-pubsub-default has correct default" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const val = try storage.config.getAsString("acl-pubsub-default");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("resetchannels", val.?);
}
