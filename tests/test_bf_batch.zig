const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const bloom_cmd = @import("../src/commands/bloom.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

fn createBulkString(str: []const u8) RespValue {
    return RespValue{ .bulk_string = str };
}

test "BF.MADD basic usage - add multiple items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create filter first
    const reserve_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("0.01"),
        createBulkString("100"),
    };
    const reserve_result = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);
    try std.testing.expectEqual(RespValue{ .simple_string = "OK" }, reserve_result);

    // BF.MADD myfilter item1 item2 item3
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &args);

    // Should return array of integers [1, 1, 1] (all new)
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false), // Expected array
    }
}

test "BF.MADD duplicate items - returns 0 for duplicates" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create and add initial items
    const reserve_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("0.01"),
        createBulkString("100"),
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    const add_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
    };
    _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args);

    // BF.MADD myfilter item1 item2 (item1 already exists)
    const madd_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &madd_args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(2, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]); // duplicate
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // new
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.MADD auto-creates filter with defaults" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.MADD on nonexistent key should auto-create
    const args = [_]RespValue{
        createBulkString("autofilter"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(2, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.MADD arity error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{createBulkString("myfilter")};

    const result = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR wrong number of arguments for 'bf.madd' command" }, result);
}

test "BF.MADD WRONGTYPE error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a string key
    const key = try std.testing.allocator.dupe(u8, "stringkey");
    const value = storage_mod.Value{ .string = "hello" };
    try storage.data.put(key, value);

    const args = [_]RespValue{
        createBulkString("stringkey"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" }, result);
}

test "BF.MEXISTS basic usage - check multiple items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create filter and add items
    const reserve_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("0.01"),
        createBulkString("100"),
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    const add_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
        createBulkString("item3"),
    };
    _ = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &add_args);

    // BF.MEXISTS myfilter item1 item2 item3
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &args);

    // Should return [1, 0, 1] (item1 exists, item2 doesn't, item3 exists)
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.MEXISTS nonexistent key - returns all zeros" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("nonexistent"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(2, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.MEXISTS arity error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{createBulkString("myfilter")};

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR wrong number of arguments for 'bf.mexists' command" }, result);
}

test "BF.MEXISTS WRONGTYPE error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a string key
    const key = try std.testing.allocator.dupe(u8, "stringkey");
    const value = storage_mod.Value{ .string = "hello" };
    try storage.data.put(key, value);

    const args = [_]RespValue{
        createBulkString("stringkey"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" }, result);
}

test "BF.MADD and BF.MEXISTS integration" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add items using MADD
    const madd_args = [_]RespValue{
        createBulkString("intfilter"),
        createBulkString("apple"),
        createBulkString("banana"),
        createBulkString("cherry"),
    };
    const madd_result = try bloom_cmd.cmdBfMadd(std.testing.allocator, &storage, &madd_args);
    switch (madd_result) {
        .array => |arr| std.testing.allocator.free(arr),
        else => {},
    }

    // Check using MEXISTS
    const mexists_args = [_]RespValue{
        createBulkString("intfilter"),
        createBulkString("apple"),
        createBulkString("date"),
        createBulkString("banana"),
        createBulkString("elderberry"),
        createBulkString("cherry"),
    };

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &mexists_args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(5, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // apple
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]); // date
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]); // banana
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[3]); // elderberry
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[4]); // cherry
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}
