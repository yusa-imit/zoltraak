// Integration tests for XREAD/XREADGROUP BLOCK functionality
const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const streams_adv = @import("../src/commands/streams_advanced.zig");
const streams = @import("../src/commands/streams.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

test "XREAD BLOCK with timeout" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add initial entry
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const add_result = try streams.cmdXadd(allocator, &storage, &xadd_args, null);
    defer allocator.free(add_result);

    // XREAD BLOCK 100 with $ (no new data, should timeout)
    const start_time = std.time.milliTimestamp();
    const xread_args = [_]RespValue{
        RespValue{ .bulk_string = "XREAD" },
        RespValue{ .bulk_string = "BLOCK" },
        RespValue{ .bulk_string = "100" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "$" },
    };
    const result = try streams_adv.cmdXread(allocator, &storage, &xread_args);
    defer allocator.free(result);
    const elapsed = std.time.milliTimestamp() - start_time;

    // Should return null after ~100ms
    try testing.expectEqualStrings("$-1\r\n", result);
    try testing.expect(elapsed >= 100);
    try testing.expect(elapsed < 200); // Allow some slack
}

test "XREAD BLOCK returns immediately if data available" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add entry
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const add_result = try streams.cmdXadd(allocator, &storage, &xadd_args, null);
    defer allocator.free(add_result);

    // XREAD BLOCK 1000 with 0-0 (data available, should return immediately)
    const start_time = std.time.milliTimestamp();
    const xread_args = [_]RespValue{
        RespValue{ .bulk_string = "XREAD" },
        RespValue{ .bulk_string = "BLOCK" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "0-0" },
    };
    const result = try streams_adv.cmdXread(allocator, &storage, &xread_args);
    defer allocator.free(result);
    const elapsed = std.time.milliTimestamp() - start_time;

    // Should return immediately (< 50ms)
    try testing.expect(elapsed < 50);
    // Should contain data (not null)
    try testing.expect(!std.mem.eql(u8, result, "$-1\r\n"));
}

test "XREADGROUP BLOCK with timeout" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream and group
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const add_result = try streams.cmdXadd(allocator, &storage, &xadd_args, null);
    defer allocator.free(add_result);

    try storage.xgroupCreate("mystream", "mygroup", "$");

    // XREADGROUP BLOCK 100 with > (no new data, should timeout)
    const start_time = std.time.milliTimestamp();
    const xreadgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "BLOCK" },
        RespValue{ .bulk_string = "100" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = ">" },
    };
    const result = try streams_adv.cmdXreadgroup(allocator, &storage, &xreadgroup_args);
    defer allocator.free(result);
    const elapsed = std.time.milliTimestamp() - start_time;

    // Should return null after ~100ms
    try testing.expectEqualStrings("$-1\r\n", result);
    try testing.expect(elapsed >= 100);
    try testing.expect(elapsed < 200);
}

test "XREADGROUP BLOCK with ID=0 returns immediately" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream and group
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const add_result = try streams.cmdXadd(allocator, &storage, &xadd_args, null);
    defer allocator.free(add_result);

    try storage.xgroupCreate("mystream", "mygroup", "0-0");

    // Read once to create PEL entry
    const xreadgroup_args1 = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = ">" },
    };
    const result1 = try streams_adv.cmdXreadgroup(allocator, &storage, &xreadgroup_args1);
    defer allocator.free(result1);

    // XREADGROUP BLOCK 1000 with ID=0 (should return immediately with pending)
    const start_time = std.time.milliTimestamp();
    const xreadgroup_args2 = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "BLOCK" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "0" },
    };
    const result2 = try streams_adv.cmdXreadgroup(allocator, &storage, &xreadgroup_args2);
    defer allocator.free(result2);
    const elapsed = std.time.milliTimestamp() - start_time;

    // Should return immediately (< 50ms) even with BLOCK
    try testing.expect(elapsed < 50);
    // Should contain pending data (not null)
    try testing.expect(!std.mem.eql(u8, result2, "$-1\r\n"));
}
