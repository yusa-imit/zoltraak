const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const keys_cmds = zoltraak.commands.keys_cmds;
const RespProtocol = zoltraak.client.RespProtocol;

// Integration tests for SCAN family cursor format compliance (Iteration 337)
// Redis returns cursor as bulk string ($N\r\n...\r\n), not integer (:N\r\n).

test "SCAN cursor is bulk string (not integer)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "v1", null);
    try storage.set("key2", "v2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "SCAN" },
        .{ .bulk_string = "0" },
    };
    const response = try keys_cmds.cmdScan(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Must start with *2\r\n$  (bulk string cursor, not integer :N)
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$"));
    try std.testing.expect(!std.mem.startsWith(u8, response, "*2\r\n:"));
}

test "SCAN cursor 0 is bulk string $1\\r\\n0\\r\\n" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("only_key", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "SCAN" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "100" },
    };
    const response = try keys_cmds.cmdScan(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // With a single key and count=100, cursor should be 0 as bulk string
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$1\r\n0\r\n"));
}

test "HSCAN cursor is bulk string (not integer)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.hset("myhash", &[_][]const u8{ "f1", "f2" }, &[_][]const u8{ "v1", "v2" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "0" },
    };
    const response = try keys_cmds.cmdHscan(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Must start with *2\r\n$  (bulk string cursor)
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$"));
    try std.testing.expect(!std.mem.startsWith(u8, response, "*2\r\n:"));
    // Cursor 0 as bulk string
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$1\r\n0\r\n"));
}

test "SSCAN cursor is bulk string (not integer)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.sadd("myset", &[_][]const u8{ "alpha", "beta" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SSCAN" },
        .{ .bulk_string = "myset" },
        .{ .bulk_string = "0" },
    };
    const response = try keys_cmds.cmdSscan(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Must start with *2\r\n$  (bulk string cursor)
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$"));
    try std.testing.expect(!std.mem.startsWith(u8, response, "*2\r\n:"));
}

test "ZSCAN cursor is bulk string (not integer)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.zadd("myzset", &[_]f64{ 1.0, 2.0 }, &[_][]const u8{ "mem1", "mem2" }, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "ZSCAN" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "0" },
    };
    const response = try keys_cmds.cmdZscan(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Must start with *2\r\n$  (bulk string cursor)
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$"));
    try std.testing.expect(!std.mem.startsWith(u8, response, "*2\r\n:"));
}

test "SCAN full iteration returns all keys with bulk string cursor" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("aaa", "1", null);
    try storage.set("bbb", "2", null);
    try storage.set("ccc", "3", null);

    const args = [_]RespValue{
        .{ .bulk_string = "SCAN" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "100" },
    };
    const response = try keys_cmds.cmdScan(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Cursor 0 as bulk string means done
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n$1\r\n0\r\n"));
    // All 3 keys present
    try std.testing.expect(std.mem.indexOf(u8, response, "aaa") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "bbb") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "ccc") != null);
}
