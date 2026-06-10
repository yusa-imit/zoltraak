const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const ss_cmds = zoltraak.sorted_sets;
const list_cmds = zoltraak.lists_commands;

// Iteration 343: LMPOP/ZMPOP/BLMPOP/BZMPOP null array response fix
//
// Redis returns null array (*-1\r\n) when LMPOP/ZMPOP cannot pop from any key
// (all keys empty or non-existent). Previously Zoltraak returned a null bulk
// string ($-1\r\n) which is the wrong RESP type for these commands.

test "LMPOP all-empty keys returns null array *-1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "LMPOP" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "nokey1" },
        .{ .bulk_string = "nokey2" },
        .{ .bulk_string = "LEFT" },
    };
    const result = try list_cmds.cmdLmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    // Must be null array, not null bulk string
    try std.testing.expectEqualStrings("*-1\r\n", result);
}

test "LMPOP non-existent single key returns null array *-1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "LMPOP" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "RIGHT" },
    };
    const result = try list_cmds.cmdLmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*-1\r\n", result);
}

test "LMPOP existing key returns *2 array, not null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    _ = try storage.lpush("mylist", &[_][]const u8{"hello"}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "LMPOP" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "mylist" },
        .{ .bulk_string = "LEFT" },
    };
    const result = try list_cmds.cmdLmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    // Should start with *2 (key + array of elements)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "ZMPOP all-empty keys returns null array *-1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZMPOP" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "nokey1" },
        .{ .bulk_string = "nokey2" },
        .{ .bulk_string = "MIN" },
    };
    const result = try ss_cmds.cmdZmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    // Must be null array, not null bulk string
    try std.testing.expectEqualStrings("*-1\r\n", result);
}

test "ZMPOP non-existent single key returns null array *-1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZMPOP" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "nosuchzset" },
        .{ .bulk_string = "MAX" },
    };
    const result = try ss_cmds.cmdZmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*-1\r\n", result);
}

test "ZMPOP existing key returns *2 array, not null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    _ = try storage.zadd("myzset", &[_]f64{1.0}, &[_][]const u8{"a"}, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "ZMPOP" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "MIN" },
    };
    const result = try ss_cmds.cmdZmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    // Should start with *2 (key + array of member-score pairs)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "BLMPOP timeout 0.001 on empty key returns null array *-1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Very short timeout so it expires quickly
    const args = [_]RespValue{
        .{ .bulk_string = "BLMPOP" },
        .{ .bulk_string = "0.001" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "LEFT" },
    };
    const result = try list_cmds.cmdBlmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*-1\r\n", result);
}

test "BZMPOP timeout 0.001 on empty key returns null array *-1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Very short timeout so it expires quickly
    const args = [_]RespValue{
        .{ .bulk_string = "BZMPOP" },
        .{ .bulk_string = "0.001" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "nosuchzset" },
        .{ .bulk_string = "MIN" },
    };
    const result = try ss_cmds.cmdBzmpop(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*-1\r\n", result);
}
