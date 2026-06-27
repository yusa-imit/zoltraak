const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const PubSub = zoltraak.pubsub.PubSub;
const RespValue = zoltraak.protocol.RespValue;
const RespProtocol = zoltraak.client.RespProtocol;
const streams = zoltraak.streams_commands;

// Regression test: XRANGE used to return 0xaa garbage bytes due to use-after-free.
// The old code stored pre-formatted RESP buffers as RespValue.bulk_string references
// and freed them with `defer` inside the loop body, causing use-after-free when the
// outer w.writeArray() read the now-freed slices after the loop ended.
test "XRANGE - returns actual field values not garbage bytes" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Add entries with known field values
    const xadd1 = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1000-0" },
        .{ .bulk_string = "sensor" },
        .{ .bulk_string = "temperature" },
        .{ .bulk_string = "value" },
        .{ .bulk_string = "42.5" },
    };
    const r1 = try streams.cmdXadd(allocator, storage, &xadd1, &pubsub, 0);
    defer allocator.free(r1);

    const xadd2 = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "2000-0" },
        .{ .bulk_string = "sensor" },
        .{ .bulk_string = "humidity" },
        .{ .bulk_string = "value" },
        .{ .bulk_string = "78" },
    };
    const r2 = try streams.cmdXadd(allocator, storage, &xadd2, &pubsub, 0);
    defer allocator.free(r2);

    // XRANGE should return properly formatted entries
    const args = [_]RespValue{
        .{ .bulk_string = "XRANGE" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "-" },
        .{ .bulk_string = "+" },
    };
    const result = try streams.cmdXrange(allocator, storage, &args, .RESP2);
    defer allocator.free(result);

    // Must start with *2\r\n (2 entries)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must contain entry IDs
    try std.testing.expect(std.mem.indexOf(u8, result, "1000-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "2000-0") != null);
    // Must contain actual field names and values
    try std.testing.expect(std.mem.indexOf(u8, result, "sensor") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "temperature") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "42.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "humidity") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "78") != null);
    // Must NOT contain garbage 0xaa bytes (regression guard)
    try std.testing.expect(std.mem.indexOf(u8, result, "\xaa") == null);
    // Must be valid RESP2: each entry is *2\r\n (id + fields array)
    // Count *2\r\n occurrences: outer array + 2 entry arrays = 3 total
    var count: usize = 0;
    var idx: usize = 0;
    while (std.mem.indexOf(u8, result[idx..], "*2\r\n")) |pos| {
        count += 1;
        idx += pos + 4;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "XRANGE - empty result for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "XRANGE" },
        .{ .bulk_string = "nokey" },
        .{ .bulk_string = "-" },
        .{ .bulk_string = "+" },
    };
    const result = try streams.cmdXrange(allocator, storage, &args, .RESP2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "XRANGE - COUNT limits entries, no garbage" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Add 4 entries
    inline for (.{ "1000-0", "2000-0", "3000-0", "4000-0" }) |id| {
        const a = [_]RespValue{
            .{ .bulk_string = "XADD" }, .{ .bulk_string = "s" }, .{ .bulk_string = id },
            .{ .bulk_string = "k" }, .{ .bulk_string = "v" },
        };
        const r = try streams.cmdXadd(allocator, storage, &a, &pubsub, 0);
        defer allocator.free(r);
    }

    const args = [_]RespValue{
        .{ .bulk_string = "XRANGE" },
        .{ .bulk_string = "s" },
        .{ .bulk_string = "-" },
        .{ .bulk_string = "+" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "2" },
    };
    const result = try streams.cmdXrange(allocator, storage, &args, .RESP2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "1000-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "2000-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "3000-0") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\xaa") == null);
}

test "XREVRANGE - returns entries in reverse with correct content" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const xadd1 = [_]RespValue{
        .{ .bulk_string = "XADD" }, .{ .bulk_string = "rev" }, .{ .bulk_string = "1000-0" },
        .{ .bulk_string = "name" }, .{ .bulk_string = "alice" },
    };
    const r1 = try streams.cmdXadd(allocator, storage, &xadd1, &pubsub, 0);
    defer allocator.free(r1);

    const xadd2 = [_]RespValue{
        .{ .bulk_string = "XADD" }, .{ .bulk_string = "rev" }, .{ .bulk_string = "2000-0" },
        .{ .bulk_string = "name" }, .{ .bulk_string = "bob" },
    };
    const r2 = try streams.cmdXadd(allocator, storage, &xadd2, &pubsub, 0);
    defer allocator.free(r2);

    const args = [_]RespValue{
        .{ .bulk_string = "XREVRANGE" },
        .{ .bulk_string = "rev" },
        .{ .bulk_string = "+" },
        .{ .bulk_string = "-" },
    };
    const result = try streams.cmdXrevrange(allocator, storage, &args, .RESP2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "2000-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1000-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "name") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "bob") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\xaa") == null);
    // 2000-0 (newer) should come BEFORE 1000-0 (older) in reverse order
    const pos2000 = std.mem.indexOf(u8, result, "2000-0").?;
    const pos1000 = std.mem.indexOf(u8, result, "1000-0").?;
    try std.testing.expect(pos2000 < pos1000);
}
