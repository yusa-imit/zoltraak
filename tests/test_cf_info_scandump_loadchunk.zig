const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const cuckoo_cmds = @import("../src/commands/cuckoo.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

test "CF.INFO returns filter metadata" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create a cuckoo filter with CF.RESERVE
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "1000" },
    };
    const reserve_result = try cuckoo_cmds.cmdCfReserve(allocator, storage, &reserve_args);
    try std.testing.expect(std.mem.eql(u8, reserve_result.simple_string, "OK"));

    // Add some items
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "item1" },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, storage, &add_args);

    // Get filter info
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
    };
    const info_result = try cuckoo_cmds.cmdCfInfo(allocator, storage, &info_args);

    // Should return array of key-value pairs
    switch (info_result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 14), arr.len);
            // First pair: Size
            try std.testing.expect(std.mem.eql(u8, arr[0].bulk_string, "Size"));
            // Value should be an integer
            try std.testing.expect(arr[1] == .integer);

            // Clean up array
            allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INFO with specific field returns single value" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create a cuckoo filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "BUCKETSIZE" },
        RespValue{ .bulk_string = "4" },
    };
    const reserve_result = try cuckoo_cmds.cmdCfReserve(allocator, storage, &reserve_args);
    try std.testing.expect(std.mem.eql(u8, reserve_result.simple_string, "OK"));

    // Get bucketsize field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "bucketsize" },
    };
    const info_result = try cuckoo_cmds.cmdCfInfo(allocator, storage, &info_args);

    // Should return integer value
    switch (info_result) {
        .integer => |val| try std.testing.expectEqual(@as(i64, 4), val),
        else => try std.testing.expect(false),
    }
}

test "CF.INFO on nonexistent key returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "nosuchkey" },
    };
    const info_result = try cuckoo_cmds.cmdCfInfo(allocator, storage, &info_args);

    switch (info_result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR not found")),
        else => try std.testing.expect(false),
    }
}

test "CF.SCANDUMP returns serialized filter data" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create and populate filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "100" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, storage, &reserve_args);

    const add_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "test" },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, storage, &add_args);

    // Dump filter
    const dump_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0" },
    };
    const dump_result = try cuckoo_cmds.cmdCfScandump(allocator, storage, &dump_args);

    // Should return [iterator, data] array
    switch (dump_result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 2), arr.len);
            // First element: iterator (0 for single chunk)
            try std.testing.expectEqual(@as(i64, 0), arr[0].integer);
            // Second element: data (non-null)
            try std.testing.expect(arr[1] == .bulk_string);

            // Clean up
            if (arr[1] == .bulk_string) {
                allocator.free(arr[1].bulk_string);
            }
            allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.LOADCHUNK restores filter from dump" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create original filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "original" },
        RespValue{ .bulk_string = "100" },
        RespValue{ .bulk_string = "BUCKETSIZE" },
        RespValue{ .bulk_string = "4" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, storage, &reserve_args);

    // Add items
    const add_args1 = [_]RespValue{
        RespValue{ .bulk_string = "original" },
        RespValue{ .bulk_string = "item1" },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, storage, &add_args1);

    const add_args2 = [_]RespValue{
        RespValue{ .bulk_string = "original" },
        RespValue{ .bulk_string = "item2" },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, storage, &add_args2);

    // Dump original filter
    const dump_args = [_]RespValue{
        RespValue{ .bulk_string = "original" },
        RespValue{ .bulk_string = "0" },
    };
    const dump_result = try cuckoo_cmds.cmdCfScandump(allocator, storage, &dump_args);

    var data_copy: []const u8 = undefined;
    switch (dump_result) {
        .array => |arr| {
            data_copy = try allocator.dupe(u8, arr[1].bulk_string);
            allocator.free(arr[1].bulk_string);
            allocator.free(arr);
        },
        else => unreachable,
    }
    defer allocator.free(data_copy);

    // Load into new filter
    const load_args = [_]RespValue{
        RespValue{ .bulk_string = "restored" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = data_copy },
    };
    const load_result = try cuckoo_cmds.cmdCfLoadchunk(allocator, storage, &load_args);

    try std.testing.expect(std.mem.eql(u8, load_result.simple_string, "OK"));

    // Verify restored filter has the items
    const exists_args1 = [_]RespValue{
        RespValue{ .bulk_string = "restored" },
        RespValue{ .bulk_string = "item1" },
    };
    const exists_result1 = try cuckoo_cmds.cmdCfExists(allocator, storage, &exists_args1);
    try std.testing.expectEqual(@as(i64, 1), exists_result1.integer);

    const exists_args2 = [_]RespValue{
        RespValue{ .bulk_string = "restored" },
        RespValue{ .bulk_string = "item2" },
    };
    const exists_result2 = try cuckoo_cmds.cmdCfExists(allocator, storage, &exists_args2);
    try std.testing.expectEqual(@as(i64, 1), exists_result2.integer);
}

test "CF.INFO validates arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Test with 0 arguments
    const args0 = [_]RespValue{};
    const result0 = try cuckoo_cmds.cmdCfInfo(allocator, storage, &args0);
    switch (result0) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }

    // Test with 3 arguments
    const args3 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "field" },
        RespValue{ .bulk_string = "extra" },
    };
    const result3 = try cuckoo_cmds.cmdCfInfo(allocator, storage, &args3);
    switch (result3) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }
}

test "CF.SCANDUMP validates arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Test with 1 argument
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
    };
    const result1 = try cuckoo_cmds.cmdCfScandump(allocator, storage, &args1);
    switch (result1) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }

    // Test with 3 arguments
    const args3 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "extra" },
    };
    const result3 = try cuckoo_cmds.cmdCfScandump(allocator, storage, &args3);
    switch (result3) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }
}

test "CF.LOADCHUNK validates arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Test with 2 arguments
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "0" },
    };
    const result2 = try cuckoo_cmds.cmdCfLoadchunk(allocator, storage, &args2);
    switch (result2) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }

    // Test with 4 arguments
    const args4 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "data" },
        RespValue{ .bulk_string = "extra" },
    };
    const result4 = try cuckoo_cmds.cmdCfLoadchunk(allocator, storage, &args4);
    switch (result4) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }
}

test "CF.INFO handles unknown field" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "100" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, storage, &reserve_args);

    // Query unknown field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "nosuchfield" },
    };
    const info_result = try cuckoo_cmds.cmdCfInfo(allocator, storage, &info_args);

    switch (info_result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR unknown field")),
        else => try std.testing.expect(false),
    }
}

test "CF.SCANDUMP on nonexistent key returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const dump_args = [_]RespValue{
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "0" },
    };
    const dump_result = try cuckoo_cmds.cmdCfScandump(allocator, storage, &dump_args);

    switch (dump_result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR key does not exist")),
        else => try std.testing.expect(false),
    }
}
