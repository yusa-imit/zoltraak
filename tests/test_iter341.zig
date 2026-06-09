const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const keys_cmds = zoltraak.keys_commands;
const streams_cmds = zoltraak.streams_commands;

// Iteration 341: SORT/XPENDING non-existent key response fixes
//
// Redis returns *0\r\n (empty array) for SORT on a non-existent key.
// Previously: *-1\r\n (null array) was returned instead.
//
// Redis XPENDING (summary form) on a non-existent key returns NOGROUP error.
// Redis XPENDING (extended form) on a non-existent key returns empty array.
// Previously: *-1\r\n (null array) was returned for both.

test "SORT - non-existent key returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "nonexistentkey" },
    };
    const result = try keys_cmds.cmdSort(allocator, storage, &args);
    defer allocator.free(result);

    // Should return empty array *0\r\n not null array *-1\r\n
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "SORT - non-existent key with ALPHA returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "nonexistentkey" },
        .{ .bulk_string = "ALPHA" },
    };
    const result = try keys_cmds.cmdSort(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "SORT - non-existent key with LIMIT returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "nonexistentkey" },
        .{ .bulk_string = "LIMIT" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "10" },
    };
    const result = try keys_cmds.cmdSort(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "SORT - non-existent key STORE deletes dest and returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set up a dest key to be deleted
    _ = try storage.lpush("destkey", &[_][]const u8{"existing"}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "nonexistentkey" },
        .{ .bulk_string = "STORE" },
        .{ .bulk_string = "destkey" },
    };
    const result = try keys_cmds.cmdSort(allocator, storage, &args);
    defer allocator.free(result);

    // Should return integer 0
    try std.testing.expectEqualStrings(":0\r\n", result);
    // dest key should be deleted
    try std.testing.expect(!storage.exists("destkey"));
}

test "SORT - existing list returns sorted elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.rpush("mylist", &[_][]const u8{ "3", "1", "2" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "mylist" },
    };
    const result = try keys_cmds.cmdSort(allocator, storage, &args);
    defer allocator.free(result);

    // Should return sorted array: 1, 2, 3
    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\n1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\n2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\n3\r\n") != null);
}

test "XPENDING - extended form on non-existent key returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "XPENDING" },
        .{ .bulk_string = "nonexistentstream" },
        .{ .bulk_string = "mygroup" },
        .{ .bulk_string = "-" },
        .{ .bulk_string = "+" },
        .{ .bulk_string = "10" },
    };
    const result = try streams_cmds.cmdXpending(allocator, storage, &args);
    defer allocator.free(result);

    // Extended form: non-existent key → empty array *0\r\n
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "XPENDING - summary form on non-existent key returns NOGROUP error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "XPENDING" },
        .{ .bulk_string = "nonexistentstream" },
        .{ .bulk_string = "mygroup" },
    };
    const result = try streams_cmds.cmdXpending(allocator, storage, &args);
    defer allocator.free(result);

    // Summary form: non-existent key → NOGROUP error
    try std.testing.expect(std.mem.startsWith(u8, result, "-NOGROUP"));
    try std.testing.expect(std.mem.indexOf(u8, result, "mygroup") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nonexistentstream") != null);
}
