const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const bloom_cmd = @import("../src/commands/bloom.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

fn createBulkString(str: []const u8) RespValue {
    return RespValue{ .bulk_string = str };
}

// ============================================================================
// Section 1: Basic Auto-Creation Tests (6 tests)
// ============================================================================

test "BF.INSERT auto-creates filter with default parameters" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter ITEMS item1 item2 item3
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    // Should return [1, 1, 1] (all new items)
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with custom CAPACITY parameter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter CAPACITY 500 ITEMS item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("500"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with custom ERROR rate" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter ERROR 0.001 ITEMS item1 item2
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ERROR"),
        createBulkString("0.001"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

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

test "BF.INSERT with custom EXPANSION parameter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter EXPANSION 4 ITEMS item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("EXPANSION"),
        createBulkString("4"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with NONSCALING flag" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter NONSCALING ITEMS item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NONSCALING"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with all custom parameters" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter CAPACITY 200 ERROR 0.005 EXPANSION 3 NONSCALING ITEMS item1 item2
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("200"),
        createBulkString("ERROR"),
        createBulkString("0.005"),
        createBulkString("EXPANSION"),
        createBulkString("3"),
        createBulkString("NONSCALING"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

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

// ============================================================================
// Section 2: NOCREATE Behavior Tests (5 tests)
// ============================================================================

test "BF.INSERT NOCREATE on nonexistent filter returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT nonexistent NOCREATE ITEMS item1
    const args = [_]RespValue{
        createBulkString("nonexistent"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR no such key" }, result);
}

test "BF.INSERT NOCREATE on existing filter works" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create filter first
    const reserve_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("0.01"),
        createBulkString("100"),
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // BF.INSERT myfilter NOCREATE ITEMS item1 item2
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

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

test "BF.INSERT NOCREATE with CAPACITY returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter NOCREATE CAPACITY 100 ITEMS item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NOCREATE"),
        createBulkString("CAPACITY"),
        createBulkString("100"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR NOCREATE cannot be used with CAPACITY or ERROR" }, result);
}

test "BF.INSERT NOCREATE with ERROR returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter NOCREATE ERROR 0.01 ITEMS item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NOCREATE"),
        createBulkString("ERROR"),
        createBulkString("0.01"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR NOCREATE cannot be used with CAPACITY or ERROR" }, result);
}

test "BF.INSERT NOCREATE with both CAPACITY and ERROR returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT myfilter NOCREATE CAPACITY 100 ERROR 0.01 ITEMS item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NOCREATE"),
        createBulkString("CAPACITY"),
        createBulkString("100"),
        createBulkString("ERROR"),
        createBulkString("0.01"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR NOCREATE cannot be used with CAPACITY or ERROR" }, result);
}

// ============================================================================
// Section 3: Existing Filter Tests (3 tests)
// ============================================================================

test "BF.INSERT ignores creation params when filter exists" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create filter with capacity 100
    const reserve_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("0.01"),
        createBulkString("100"),
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // Try to insert with different capacity (should be ignored)
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("500"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with duplicate detection on existing filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create and add item
    const add_args1 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &add_args1);

    // Try to insert same item again
    const add_args2 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &add_args2);

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

test "BF.INSERT NONSCALING flag ignored on existing filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create scaling filter (default)
    const reserve_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("0.01"),
        createBulkString("100"),
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // Try to add with NONSCALING (should be ignored)
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NONSCALING"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

// ============================================================================
// Section 4: Return Value Tests (4 tests)
// ============================================================================

test "BF.INSERT returns array of 1s for all new items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("apple"),
        createBulkString("banana"),
        createBulkString("cherry"),
        createBulkString("date"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(4, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[3]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT returns mixed 1s and 0s for new and duplicate items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // First insert
    const args1 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("apple"),
        createBulkString("cherry"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args1);

    // Second insert with mix of new and duplicates
    const args2 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("apple"),    // duplicate
        createBulkString("banana"),   // new
        createBulkString("cherry"),   // duplicate
        createBulkString("date"),     // new
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args2);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(4, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]); // duplicate
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // new
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]); // duplicate
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[3]); // new
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with duplicate items in same call" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Insert same item twice in one call
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // first item1
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]); // second item1 (duplicate)
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]); // item2
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT single item returns array with one element" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("single"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

// ============================================================================
// Section 5: Scaling and NONSCALING Tests (4 tests)
// ============================================================================

test "BF.INSERT auto-scaling handles capacity overflow" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create small capacity filter
    const args_base = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("5"),
        createBulkString("ITEMS"),
    };

    // Add 10 items (exceeds capacity)
    var args_list = std.ArrayList(RespValue).init(std.testing.allocator);
    defer args_list.deinit();

    for (args_base) |arg| {
        try args_list.append(arg);
    }

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var buf: [20]u8 = undefined;
        const item = try std.fmt.bufPrint(&buf, "item{d}", .{i});
        const owned = try std.testing.allocator.dupe(u8, item);
        defer std.testing.allocator.free(owned);
        try args_list.append(createBulkString(owned));
    }

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, args_list.items);

    // Should succeed with auto-scaling (all 1s)
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(10, arr.len);
            for (arr) |item| {
                try std.testing.expectEqual(RespValue{ .integer = 1 }, item);
            }
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT NONSCALING returns error on capacity overflow" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create small NONSCALING filter
    const args_base = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("3"),
        createBulkString("NONSCALING"),
        createBulkString("ITEMS"),
    };

    // Try to add 5 items (exceeds capacity of 3)
    var args_list = std.ArrayList(RespValue).init(std.testing.allocator);
    defer args_list.deinit();

    for (args_base) |arg| {
        try args_list.append(arg);
    }

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var buf: [20]u8 = undefined;
        const item = try std.fmt.bufPrint(&buf, "item{d}", .{i});
        const owned = try std.testing.allocator.dupe(u8, item);
        defer std.testing.allocator.free(owned);
        try args_list.append(createBulkString(owned));
    }

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, args_list.items);

    // Should have partial success + errors
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(5, arr.len);
            // First 3 items should succeed
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            // Last 2 should be errors
            try std.testing.expectEqual(RespValue{ .error_string = "ERR filter is full" }, arr[3]);
            try std.testing.expectEqual(RespValue{ .error_string = "ERR filter is full" }, arr[4]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT NONSCALING partial success commits early items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create NONSCALING filter with capacity 2
    const args1 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("2"),
        createBulkString("NONSCALING"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args1);

    // Verify first 2 items were added using BF.MEXISTS
    const check_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &check_args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // item1 exists
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // item2 exists
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]); // item3 does NOT exist
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with EXPANSION affects scaling behavior" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create filter with low capacity and high expansion
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("3"),
        createBulkString("EXPANSION"),
        createBulkString("10"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
        createBulkString("item4"),
        createBulkString("item5"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    // Should scale and succeed for all items
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(5, arr.len);
            for (arr) |item| {
                try std.testing.expectEqual(RespValue{ .integer = 1 }, item);
            }
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

// ============================================================================
// Section 6: Error Cases (9 tests)
// ============================================================================

test "BF.INSERT missing key returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{};

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR wrong number of arguments for 'bf.insert' command" }, result);
}

test "BF.INSERT missing ITEMS keyword returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR ITEMS keyword required" }, result);
}

test "BF.INSERT empty ITEMS list returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR wrong number of arguments for 'bf.insert' command" }, result);
}

test "BF.INSERT invalid CAPACITY returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Capacity 0
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("0"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR capacity must be greater than 0" }, result);
}

test "BF.INSERT invalid ERROR rate too low" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ERROR"),
        createBulkString("0.0"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR error rate must be between 0 and 1" }, result);
}

test "BF.INSERT invalid ERROR rate too high" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ERROR"),
        createBulkString("1.0"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR error rate must be between 0 and 1" }, result);
}

test "BF.INSERT invalid EXPANSION returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("EXPANSION"),
        createBulkString("0"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR expansion must be greater than 0" }, result);
}

test "BF.INSERT WRONGTYPE error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a string key
    const key = try std.testing.allocator.dupe(u8, "stringkey");
    const value = storage_mod.Value{ .string = "hello" };
    try storage.data.put(key, value);

    const args = [_]RespValue{
        createBulkString("stringkey"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" }, result);
}

test "BF.INSERT invalid CAPACITY format returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("notanumber"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR value is not an integer or out of range" }, result);
}

// ============================================================================
// Section 7: Edge Cases (6 tests)
// ============================================================================

test "BF.INSERT with binary data containing null bytes" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const binary_item = "item\x00with\x00nulls";
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString(binary_item),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with empty string item" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString(""),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with special characters" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item:with:colons"),
        createBulkString("item|with|pipes"),
        createBulkString("item\nwith\nnewlines"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT large number of items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    var args_list = std.ArrayList(RespValue).init(std.testing.allocator);
    defer args_list.deinit();

    try args_list.append(createBulkString("myfilter"));
    try args_list.append(createBulkString("CAPACITY"));
    try args_list.append(createBulkString("1000"));
    try args_list.append(createBulkString("ITEMS"));

    // Add 100 items
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        var buf: [30]u8 = undefined;
        const item = try std.fmt.bufPrint(&buf, "item{d}", .{i});
        const owned = try std.testing.allocator.dupe(u8, item);
        defer std.testing.allocator.free(owned);
        try args_list.append(createBulkString(owned));
    }

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, args_list.items);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(100, arr.len);
            for (arr) |item| {
                try std.testing.expectEqual(RespValue{ .integer = 1 }, item);
            }
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT case sensitivity of ITEMS keyword" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // ITEMS is case-insensitive in Redis
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("items"),  // lowercase
        createBulkString("item1"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    // Should accept lowercase 'items'
    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with very long item value" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a long string (1000 bytes)
    var long_item = try std.testing.allocator.alloc(u8, 1000);
    defer std.testing.allocator.free(long_item);
    @memset(long_item, 'x');

    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString(long_item),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

// ============================================================================
// Section 8: Integration Tests (5 tests)
// ============================================================================

test "BF.INSERT followed by BF.EXISTS verification" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Insert items
    const insert_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("apple"),
        createBulkString("banana"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &insert_args);

    // Verify with BF.EXISTS
    const exists_args1 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("apple"),
    };
    const result1 = try bloom_cmd.cmdBfExists(std.testing.allocator, &storage, &exists_args1);
    try std.testing.expectEqual(RespValue{ .integer = 1 }, result1);

    const exists_args2 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("cherry"),
    };
    const result2 = try bloom_cmd.cmdBfExists(std.testing.allocator, &storage, &exists_args2);
    try std.testing.expectEqual(RespValue{ .integer = 0 }, result2);
}

test "BF.INSERT multiple operations on same filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // First insert
    const args1 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args1);

    // Second insert
    const args2 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item2"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args2);

    // Third insert with duplicates
    const args3 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args3);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(3, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]); // item1 duplicate
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]); // item2 duplicate
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]); // item3 new
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT with BF.RESERVE equivalent behavior" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // BF.INSERT with custom params (should behave like BF.RESERVE + BF.MADD)
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("CAPACITY"),
        createBulkString("200"),
        createBulkString("ERROR"),
        createBulkString("0.001"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(2, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }

    // Verify filter was created and items exist
    const exists_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
    };
    const exists_result = try bloom_cmd.cmdBfExists(std.testing.allocator, &storage, &exists_args);
    try std.testing.expectEqual(RespValue{ .integer = 1 }, exists_result);
}

test "BF.INSERT then BF.INSERT NOCREATE on same key" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // First insert (creates filter)
    const args1 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args1);

    // Second insert with NOCREATE (should work)
    const args2 = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item2"),
    };

    const result = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &args2);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(1, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "BF.INSERT cross-command consistency check" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Insert using BF.INSERT
    const insert_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("alpha"),
        createBulkString("beta"),
        createBulkString("gamma"),
    };
    _ = try bloom_cmd.cmdBfInsert(std.testing.allocator, &storage, &insert_args);

    // Check with BF.MEXISTS
    const mexists_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("alpha"),
        createBulkString("beta"),
        createBulkString("gamma"),
        createBulkString("delta"),
    };

    const result = try bloom_cmd.cmdBfMexists(std.testing.allocator, &storage, &mexists_args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(4, arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // alpha exists
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // beta exists
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]); // gamma exists
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[3]); // delta doesn't exist
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}
