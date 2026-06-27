// Iteration 346: XREAD/XREADGROUP BLOCK null array response fix
// Redis returns *-1\r\n (null array) on timeout, not $-1\r\n (null bulk string).
// Same pattern as LMPOP/ZMPOP fix in iteration 343.
const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const streams_adv = zoltraak.streams_advanced_commands;
const RespProtocol = zoltraak.client.RespProtocol;

test "iter346 - XREAD BLOCK timeout returns null array not null bulk string" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create a stream entry directly via storage layer
    _ = try storage.xadd("mystream", "*", &[_][]const u8{ "field", "value" }, null, .{});

    // XREAD BLOCK 100 STREAMS mystream $ — no new data after $, should timeout and return *-1\r\n
    const xread_args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "BLOCK" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "$" },
    };
    const result = try streams_adv.cmdXread(allocator, storage, &xread_args, .RESP2);
    defer allocator.free(result);

    // Must be null array (*-1\r\n), NOT null bulk string ($-1\r\n)
    try testing.expectEqualStrings("*-1\r\n", result);
    try testing.expect(!std.mem.eql(u8, result, "$-1\r\n"));
}

test "iter346 - XREAD BLOCK timeout on non-existent stream returns null array" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // XREAD BLOCK 100 STREAMS nonexistent $ — stream doesn't exist, timeout
    const xread_args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "BLOCK" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "nonexistent" },
        .{ .bulk_string = "$" },
    };
    const result = try streams_adv.cmdXread(allocator, storage, &xread_args, .RESP2);
    defer allocator.free(result);

    try testing.expectEqualStrings("*-1\r\n", result);
}

test "iter346 - XREADGROUP BLOCK timeout returns null array not null bulk string" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream entry and consumer group, position group at last entry ($)
    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field", "value" }, null, .{});
    try storage.xgroupCreate("mystream", "mygroup", "$", null);

    // XREADGROUP BLOCK 100 GROUP mygroup consumer > — no new messages after $, should timeout
    const xreadgroup_args = [_]RespValue{
        .{ .bulk_string = "XREADGROUP" },
        .{ .bulk_string = "GROUP" },
        .{ .bulk_string = "mygroup" },
        .{ .bulk_string = "consumer1" },
        .{ .bulk_string = "BLOCK" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = ">" },
    };
    const result = try streams_adv.cmdXreadgroup(allocator, storage, &xreadgroup_args, .RESP2);
    defer allocator.free(result);

    // Must be null array (*-1\r\n), NOT null bulk string ($-1\r\n)
    try testing.expectEqualStrings("*-1\r\n", result);
    try testing.expect(!std.mem.eql(u8, result, "$-1\r\n"));
}

test "iter346 - XREAD without BLOCK returns null array when no data matches" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field", "value" }, null, .{});

    // XREAD STREAMS mystream 9999-0 — asks for entries after 9999-0, none exist
    const xread_args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "9999-0" },
    };
    const result = try streams_adv.cmdXread(allocator, storage, &xread_args, .RESP2);
    defer allocator.free(result);

    // No BLOCK, no data: null array
    try testing.expectEqualStrings("*-1\r\n", result);
}

test "iter346 - XREAD with data returns valid array not null" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field", "value" }, null, .{});

    // XREAD STREAMS mystream 0-0 — asks for entries after 0-0, entry at 1000-0 exists
    const xread_args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "0-0" },
    };
    const result = try streams_adv.cmdXread(allocator, storage, &xread_args, .RESP2);
    defer allocator.free(result);

    // Should NOT be null — there's data
    try testing.expect(!std.mem.eql(u8, result, "*-1\r\n"));
    try testing.expect(!std.mem.eql(u8, result, "$-1\r\n"));
    // Should start with *1\r\n (array of 1 stream)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
}
