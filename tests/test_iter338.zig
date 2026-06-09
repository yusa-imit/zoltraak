const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const hashes_cmds = zoltraak.commands.hashes_cmds;
const keys_cmds = zoltraak.commands.keys_cmds;
const RespProtocol = zoltraak.client.RespProtocol;

// Iteration 338: HRANDFIELD empty-array fix + TTL round-to-nearest
//
// Fix 1: HRANDFIELD returns *0\r\n (empty array) when count is provided
//         and key does not exist. Previously returned $-1\r\n (nil).
//         Matches ZRANDMEMBER and SRANDMEMBER behavior.
//
// Fix 2: TTL converts ms→seconds using round-to-nearest ((ttl+500)/1000),
//         not ceiling ((ttl+999)/1000). Matches Redis source.

// --- HRANDFIELD fix tests ---

test "HRANDFIELD nonexistent key with positive count returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "3" },
    };
    const response = try hashes_cmds.cmdHrandfield(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(response);

    // Must be empty array, not nil bulk string
    try std.testing.expectEqualStrings("*0\r\n", response);
}

test "HRANDFIELD nonexistent key with negative count returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "-5" },
    };
    const response = try hashes_cmds.cmdHrandfield(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*0\r\n", response);
}

test "HRANDFIELD nonexistent key with count 0 returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "0" },
    };
    const response = try hashes_cmds.cmdHrandfield(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*0\r\n", response);
}

test "HRANDFIELD nonexistent key with WITHVALUES returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "WITHVALUES" },
    };
    const response = try hashes_cmds.cmdHrandfield(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*0\r\n", response);
}

test "HRANDFIELD nonexistent key without count returns nil" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "nosuchkey" },
    };
    const response = try hashes_cmds.cmdHrandfield(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(response);

    // Without count: nil bulk string
    try std.testing.expectEqualStrings("$-1\r\n", response);
}

test "HRANDFIELD existing key with count works correctly" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.hset("myhash", &[_][]const u8{"f1"}, &[_][]const u8{"v1"}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "1" },
    };
    const response = try hashes_cmds.cmdHrandfield(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(response);

    // Should be an array with one field, not empty and not nil
    try std.testing.expect(std.mem.startsWith(u8, response, "*1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "f1") != null);
}

// --- TTL rounding tests ---

test "TTL rounds 1499ms to 1 second (round-to-nearest)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("rkey2", "val", null);
    const now_ms = std.time.milliTimestamp();
    _ = storage.setExpiry("rkey2", now_ms + 1499, 0);

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "rkey2" },
    };
    const response = try keys_cmds.cmdTtl(allocator, storage, &args);
    defer allocator.free(response);

    // 1499ms rounds to 1 with round-to-nearest (ceiling would give 2)
    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "TTL rounds 1500ms to 2 seconds (round-to-nearest)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("rkey3", "val", null);
    const now_ms = std.time.milliTimestamp();
    _ = storage.setExpiry("rkey3", now_ms + 1500, 0);

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "rkey3" },
    };
    const response = try keys_cmds.cmdTtl(allocator, storage, &args);
    defer allocator.free(response);

    // 1500ms rounds to 2 (rounds up at exactly .5)
    try std.testing.expectEqualStrings(":2\r\n", response);
}
