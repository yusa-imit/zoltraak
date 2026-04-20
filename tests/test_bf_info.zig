const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const bloom_cmds = @import("../src/commands/bloom.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

test "BF.INFO - basic filter info" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    const reserve_result = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);
    try std.testing.expect(reserve_result == .simple_string);

    // Add some items
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "item1" },
    };
    _ = try bloom_cmds.cmdBfAdd(allocator, &storage, &add_args);

    // Get full info
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 10), result.array.len); // 5 fields * 2 (key-value pairs)

    // Verify field names
    try std.testing.expect(result.array[0] == .bulk_string);
    try std.testing.expectEqualStrings("Capacity", result.array[0].bulk_string);
    try std.testing.expect(result.array[1] == .integer);
    try std.testing.expectEqual(@as(i64, 1000), result.array[1].integer);

    try std.testing.expectEqualStrings("Size", result.array[2].bulk_string);
    try std.testing.expect(result.array[3].integer > 0); // Size should be positive

    try std.testing.expectEqualStrings("Number of filters", result.array[4].bulk_string);
    try std.testing.expectEqual(@as(i64, 1), result.array[5].integer);

    try std.testing.expectEqualStrings("Number of items inserted", result.array[6].bulk_string);
    try std.testing.expectEqual(@as(i64, 1), result.array[7].integer);

    try std.testing.expectEqualStrings("Expansion rate", result.array[8].bulk_string);
    try std.testing.expectEqual(@as(i64, 2), result.array[9].integer); // Default expansion

    allocator.free(result.array);
}

test "BF.INFO - specific field (CAPACITY)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Get specific field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "CAPACITY" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 1000), result.integer);
}

test "BF.INFO - specific field (SIZE)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Get SIZE field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "SIZE" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .integer);
    try std.testing.expect(result.integer > 0);
}

test "BF.INFO - specific field (FILTERS)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Get FILTERS field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "FILTERS" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 1), result.integer);
}

test "BF.INFO - specific field (ITEMS)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter and add items
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Add multiple items
    const add_args1 = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "item1" },
    };
    _ = try bloom_cmds.cmdBfAdd(allocator, &storage, &add_args1);

    const add_args2 = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "item2" },
    };
    _ = try bloom_cmds.cmdBfAdd(allocator, &storage, &add_args2);

    // Get ITEMS field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "ITEMS" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 2), result.integer);
}

test "BF.INFO - specific field (EXPANSION)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter with custom expansion
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "EXPANSION" },
        RespValue{ .bulk_string = "4" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Get EXPANSION field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "EXPANSION" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 4), result.integer);
}

test "BF.INFO - NONSCALING filter returns null for EXPANSION" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a NONSCALING filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "NONSCALING" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Get EXPANSION field - should be null
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "EXPANSION" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .null_bulk_string);
}

test "BF.INFO - nonexistent key" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Try to get info for nonexistent key
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .error_string);
    try std.testing.expectEqualStrings("ERR not found", result.error_string);
}

test "BF.INFO - wrong type" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Set a string value
    const key = "mykey";
    const owned_key = try allocator.dupe(u8, key);
    try storage.data.put(owned_key, storage_mod.Value{ .string = "value" });

    // Try to get info
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "mykey" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .error_string);
}

test "BF.INFO - unknown field" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Try to get unknown field
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "UNKNOWN" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .error_string);
    try std.testing.expectEqualStrings("ERR unknown field", result.error_string);
}

test "BF.INFO - case insensitive field names" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // Create a filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "0.01" },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try bloom_cmds.cmdBfReserve(allocator, &storage, &reserve_args);

    // Try lowercase field name
    const info_args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "capacity" },
    };
    const result = try bloom_cmds.cmdBfInfo(allocator, &storage, &info_args);

    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 1000), result.integer);
}

test "BF.INFO - arity errors" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, .{ .max_clients = 10 });
    defer storage.deinit();

    // No arguments
    const no_args = [_]RespValue{};
    const result1 = try bloom_cmds.cmdBfInfo(allocator, &storage, &no_args);
    try std.testing.expect(result1 == .error_string);

    // Too many arguments
    const too_many = [_]RespValue{
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "CAPACITY" },
        RespValue{ .bulk_string = "extra" },
    };
    const result2 = try bloom_cmds.cmdBfInfo(allocator, &storage, &too_many);
    try std.testing.expect(result2 == .error_string);
}
