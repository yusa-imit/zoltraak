const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const cuckoo_cmd = @import("../src/commands/cuckoo.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

fn createBulkString(str: []const u8) RespValue {
    return RespValue{ .bulk_string = str };
}

// CF.INSERT Tests (12 tests)

test "CF.INSERT - basic batch insert returns [1,1,1]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT myfilter ITEMS item1 item2 item3
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 3), arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false), // Expected array
    }
}

test "CF.INSERT - with CAPACITY on new key auto-creates with custom capacity" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT newfilter CAPACITY 500 ITEMS item1
    const args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("CAPACITY"),
        createBulkString("500"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }

    // Verify filter was created and item was added
    const check_args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("item1"),
    };
    const check_result = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &check_args);
    try std.testing.expectEqual(RespValue{ .integer = 1 }, check_result);
}

test "CF.INSERT - with NOCREATE on non-existent key returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT nonexistent NOCREATE ITEMS item1
    const args = [_]RespValue{
        createBulkString("nonexistent"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR not found" }, result);
}

test "CF.INSERT - with NOCREATE on existing key succeeds" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // First create the filter
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("initial"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Now insert with NOCREATE (should succeed since key exists)
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("NOCREATE"),
            createBulkString("ITEMS"),
            createBulkString("item2"),
        };

        const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 1), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERT - with both CAPACITY and NOCREATE returns error (mutually exclusive)" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT newfilter CAPACITY 500 NOCREATE ITEMS item1
    const args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("CAPACITY"),
        createBulkString("500"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    // NOCREATE with no existing key should error
    try std.testing.expectEqual(RespValue{ .error_string = "ERR not found" }, result);
}

test "CF.INSERT - on WRONGTYPE key returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a string key
    const key = try std.testing.allocator.dupe(u8, "stringkey");
    const value = storage_mod.Value{ .string = "hello" };
    try storage.data.put(key, value);

    // Try to INSERT on string key
    const args = [_]RespValue{
        createBulkString("stringkey"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expect(std.mem.startsWith(u8, result.error_string, "WRONGTYPE"));
}

test "CF.INSERT - arity error when missing ITEMS keyword" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT myfilter item1 (missing ITEMS keyword)
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR ITEMS keyword required" }, result);
}

test "CF.INSERT - arity error when no items after ITEMS keyword" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT myfilter ITEMS (no items)
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR ITEMS requires at least one item" }, result);
}

test "CF.INSERT - case-insensitive options (ITEMS, items, Items all valid)" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Test with lowercase 'items'
    const args_lower = [_]RespValue{
        createBulkString("filter1"),
        createBulkString("items"),
        createBulkString("item1"),
    };

    const result_lower = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args_lower);

    switch (result_lower) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }

    // Test with mixed case 'Items'
    const args_mixed = [_]RespValue{
        createBulkString("filter2"),
        createBulkString("Items"),
        createBulkString("item1"),
    };

    const result_mixed = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args_mixed);

    switch (result_mixed) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERT - verify items actually added via CF.MEXISTS" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Insert items
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("apple"),
            createBulkString("banana"),
            createBulkString("cherry"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Check items exist via MEXISTS
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("apple"),
            createBulkString("banana"),
            createBulkString("cherry"),
            createBulkString("date"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 4), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // apple
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // banana
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]); // cherry
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[3]); // date (not added)
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERT - empty key auto-creates filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERT newfilter ITEMS item1
    const args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }

    // Verify filter exists by checking we can add more items
    const verify_args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("ITEMS"),
        createBulkString("item2"),
    };

    const verify_result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &verify_args);

    switch (verify_result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERT - CAPACITY ignored on existing filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Create filter first with default capacity
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("item1"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Try to INSERT with CAPACITY on existing key (should be ignored)
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("CAPACITY"),
            createBulkString("5000"),
            createBulkString("ITEMS"),
            createBulkString("item2"),
        };

        const result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 1), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

// CF.INSERTNX Tests (10 tests)

test "CF.INSERTNX - basic batch insert on new filter returns [1,1,1]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERTNX newfilter ITEMS item1 item2 item3
    const args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
        createBulkString("item3"),
    };

    const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 3), arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERTNX - duplicate items in same batch returns [1,0,0]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERTNX myfilter ITEMS item1 item1 item1
    const args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item1"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 3), arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // first is new
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]); // second is duplicate
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]); // third is duplicate
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERTNX - items where some already exist returns [0,1,0]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // First add some items
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("item1"),
            createBulkString("item3"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Now try INSERTNX with mix of existing and new
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("item1"),
            createBulkString("item2"),
            createBulkString("item3"),
        };

        const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]); // item1 exists
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // item2 is new
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]); // item3 exists
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERTNX - with CAPACITY auto-creates filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERTNX newfilter CAPACITY 2000 ITEMS item1 item2
    const args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("CAPACITY"),
        createBulkString("2000"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 2), arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERTNX - with NOCREATE on non-existent key returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERTNX nonexistent NOCREATE ITEMS item1
    const args = [_]RespValue{
        createBulkString("nonexistent"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR not found" }, result);
}

test "CF.INSERTNX - on WRONGTYPE key returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a string key
    const key = try std.testing.allocator.dupe(u8, "stringkey");
    const value = storage_mod.Value{ .string = "hello" };
    try storage.data.put(key, value);

    // Try to INSERTNX on string key
    const args = [_]RespValue{
        createBulkString("stringkey"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

    try std.testing.expect(std.mem.startsWith(u8, result.error_string, "WRONGTYPE"));
}

test "CF.INSERTNX - mixed results new/existing [1,0,1]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Add initial item
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("existing"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // INSERTNX with mix
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("new1"),
            createBulkString("existing"),
            createBulkString("new2"),
        };

        const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // new1
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]); // existing
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]); // new2
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERTNX - CAPACITY ignored on existing filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Create filter first
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("item1"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Try INSERTNX with CAPACITY (should be ignored)
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("CAPACITY"),
            createBulkString("5000"),
            createBulkString("ITEMS"),
            createBulkString("item2"),
        };

        const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 1), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERTNX - CAPACITY and NOCREATE mutual exclusivity error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // CF.INSERTNX newfilter CAPACITY 500 NOCREATE ITEMS item1
    // Since key doesn't exist, NOCREATE will error before CAPACITY is checked
    const args = [_]RespValue{
        createBulkString("newfilter"),
        createBulkString("CAPACITY"),
        createBulkString("500"),
        createBulkString("NOCREATE"),
        createBulkString("ITEMS"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);

    try std.testing.expectEqual(RespValue{ .error_string = "ERR not found" }, result);
}

test "CF.INSERTNX - arity errors" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Missing ITEMS keyword
    {
        const args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString("item1"),
        };

        const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);
        try std.testing.expectEqual(RespValue{ .error_string = "ERR ITEMS keyword required" }, result);
    }

    // Missing items after ITEMS keyword
    {
        const args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString("ITEMS"),
        };

        const result = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);
        try std.testing.expectEqual(RespValue{ .error_string = "ERR ITEMS requires at least one item" }, result);
    }
}

// CF.MEXISTS Tests (8 tests)

test "CF.MEXISTS - basic batch exists check [1,1,0]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Add items first
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("apple"),
            createBulkString("banana"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Check existence
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("apple"),
            createBulkString("banana"),
            createBulkString("cherry"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // apple exists
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // banana exists
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]); // cherry doesn't exist
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.MEXISTS - all items exist [1,1,1]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Add items
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("a"),
            createBulkString("b"),
            createBulkString("c"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Check all exist
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("a"),
            createBulkString("b"),
            createBulkString("c"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.MEXISTS - no items exist [0,0,0]" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Create empty filter
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("dummy"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Check non-existent items
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("x"),
            createBulkString("y"),
            createBulkString("z"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]);
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]);
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.MEXISTS - non-existent key returns all zeros" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Check on non-existent key (not an error)
    const args = [_]RespValue{
        createBulkString("nonexistent"),
        createBulkString("item1"),
        createBulkString("item2"),
    };

    const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 2), arr.len);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[0]);
            try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[1]);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.MEXISTS - WRONGTYPE key returns error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create a string key
    const key = try std.testing.allocator.dupe(u8, "stringkey");
    const value = storage_mod.Value{ .string = "hello" };
    try storage.data.put(key, value);

    // Try MEXISTS on string key
    const args = [_]RespValue{
        createBulkString("stringkey"),
        createBulkString("item1"),
    };

    const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

    try std.testing.expect(std.mem.startsWith(u8, result.error_string, "WRONGTYPE"));
}

test "CF.MEXISTS - single item edge case" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Add single item
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("only"),
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Check single item
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("only"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 1), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.MEXISTS - items added via CF.INSERT are found" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Insert items via CF.INSERT
    const insert_items = [_]RespValue{
        createBulkString("inserted1"),
        createBulkString("inserted2"),
        createBulkString("inserted3"),
    };

    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            insert_items[0],
            insert_items[1],
            insert_items[2],
        };
        _ = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &args);
    }

    // Check they exist via MEXISTS
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("inserted1"),
            createBulkString("inserted2"),
            createBulkString("inserted3"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[2]);
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.MEXISTS - items added via CF.INSERTNX are found" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Add items via CF.INSERTNX
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("ITEMS"),
            createBulkString("nx1"),
            createBulkString("nx2"),
        };
        _ = try cuckoo_cmd.cmdCfInsertnx(std.testing.allocator, &storage, &args);
    }

    // Check they exist via MEXISTS
    {
        const args = [_]RespValue{
            createBulkString(key),
            createBulkString("nx1"),
            createBulkString("nx2"),
            createBulkString("nx3"),
        };

        const result = try cuckoo_cmd.cmdCfMexists(std.testing.allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[0]); // nx1
                try std.testing.expectEqual(RespValue{ .integer = 1 }, arr[1]); // nx2
                try std.testing.expectEqual(RespValue{ .integer = 0 }, arr[2]); // nx3 not added
                std.testing.allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

// CF.DEL Tests (10 tests)

test "CF.DEL - delete existing item returns 1" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add item first
    const add_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("testitem"),
    };
    _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);

    // Delete it
    const del_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("testitem"),
    };
    const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - delete non-existent item returns 0" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Create filter with one item
    const add_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item1"),
    };
    _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);

    // Try to delete non-existent item
    const del_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("nonexistent"),
    };
    const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - item no longer exists after deletion" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add item
    const add_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("testitem"),
    };
    _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);

    // Verify exists
    const exists_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("testitem"),
    };
    const exists_before = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &exists_args);
    switch (exists_before) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }

    // Delete
    const del_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("testitem"),
    };
    _ = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);

    // Verify no longer exists
    const exists_after = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &exists_args);
    switch (exists_after) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - delete from non-existent key returns 0" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("nonexistent"),
        createBulkString("item"),
    };
    const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - WRONGTYPE error for non-cuckoo key" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add a string value
    const key = "stringkey";
    const owned_key = try std.testing.allocator.dupe(u8, key);
    const str_value = try std.testing.allocator.dupe(u8, "value");
    try storage.data.put(owned_key, storage_mod.Value{ .string = str_value });

    const args = [_]RespValue{
        createBulkString(key),
        createBulkString("item"),
    };
    const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "WRONGTYPE")),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - wrong number of arguments error" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        createBulkString("key"),
    };
    const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - delete multiple different items" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add three items
    const items = [_][]const u8{ "item1", "item2", "item3" };
    for (items) |item| {
        const add_args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString(item),
        };
        _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);
    }

    // Delete item2
    const del_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item2"),
    };
    const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);
    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }

    // Verify item2 no longer exists
    const exists_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("item2"),
    };
    const exists_result = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &exists_args);
    switch (exists_result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }

    // Verify item1 and item3 still exist
    const check_items = [_][]const u8{ "item1", "item3" };
    for (check_items) |item| {
        const check_args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString(item),
        };
        const check_result = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &check_args);
        switch (check_result) {
            .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
            else => try std.testing.expect(false),
        }
    }
}

test "CF.DEL - delete all items from filter" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add three items
    const items = [_][]const u8{ "item1", "item2", "item3" };
    for (items) |item| {
        const add_args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString(item),
        };
        _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);
    }

    // Delete all items
    for (items) |item| {
        const del_args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString(item),
        };
        const result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);
        switch (result) {
            .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
            else => try std.testing.expect(false),
        }
    }

    // Verify all items no longer exist
    for (items) |item| {
        const exists_args = [_]RespValue{
            createBulkString("myfilter"),
            createBulkString(item),
        };
        const exists_result = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &exists_args);
        switch (exists_result) {
            .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
            else => try std.testing.expect(false),
        }
    }
}

test "CF.DEL - add delete add cycle" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    const key = "myfilter";
    const item = "testitem";

    // Add item
    const add_args = [_]RespValue{
        createBulkString(key),
        createBulkString(item),
    };
    _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);

    // Delete item
    const del_args = [_]RespValue{
        createBulkString(key),
        createBulkString(item),
    };
    _ = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);

    // Add same item again
    _ = try cuckoo_cmd.cmdCfAdd(std.testing.allocator, &storage, &add_args);

    // Verify exists
    const exists_args = [_]RespValue{
        createBulkString(key),
        createBulkString(item),
    };
    const exists_result = try cuckoo_cmd.cmdCfExists(std.testing.allocator, &storage, &exists_args);
    switch (exists_result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL - delete item that was added with CF.INSERT" {
    var storage = try Storage.init(std.testing.allocator);
    defer storage.deinit();

    // Add item using CF.INSERT
    const insert_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("ITEMS"),
        createBulkString("testitem"),
    };
    const insert_result = try cuckoo_cmd.cmdCfInsert(std.testing.allocator, &storage, &insert_args);
    switch (insert_result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            std.testing.allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }

    // Delete using CF.DEL
    const del_args = [_]RespValue{
        createBulkString("myfilter"),
        createBulkString("testitem"),
    };
    const del_result = try cuckoo_cmd.cmdCfDel(std.testing.allocator, &storage, &del_args);
    switch (del_result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }
}
