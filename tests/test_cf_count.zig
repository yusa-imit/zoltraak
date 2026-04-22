const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const cuckoo_cmds = @import("../src/commands/cuckoo.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

test "CF.COUNT: returns 0 for non-existent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent_key" },
        RespValue{ .bulk_string = "item" },
    };

    const result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &args);

    switch (result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 0), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: returns 0 for non-existent item in existing filter" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";

    // Create filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, &storage, &reserve_args);

    // Count non-existent item
    const count_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "nonexistent_item" },
    };

    const result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count_args);

    switch (result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 0), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: returns 1 after single add" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";
    const item = "test_item";

    // Create filter and add item
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, &storage, &reserve_args);

    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);

    // Count
    const count_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };

    const result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count_args);

    switch (result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 1), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: returns 2 after adding same item twice" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";
    const item = "duplicate_item";

    // Create filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, &storage, &reserve_args);

    // Add same item twice
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);

    // Count
    const count_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };

    const result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count_args);

    switch (result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 2), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: returns 3 after adding same item three times" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";
    const item = "triple_item";

    // Create filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, &storage, &reserve_args);

    // Add same item three times
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);

    // Count
    const count_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };

    const result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count_args);

    switch (result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 3), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: count decreases after delete" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";
    const item = "deletable_item";

    // Create filter and add twice
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, &storage, &reserve_args);

    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add_args);

    // Count before delete
    const count_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };

    const before_result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count_args);
    switch (before_result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 2), count),
        else => return error.UnexpectedResult,
    }

    // Delete one instance
    const del_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cuckoo_cmds.cmdCfDel(allocator, &storage, &del_args);

    // Count after delete
    const after_result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count_args);
    switch (after_result) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 1), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: different items have independent counts" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";

    // Create filter
    const reserve_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };
    _ = try cuckoo_cmds.cmdCfReserve(allocator, &storage, &reserve_args);

    // Add item1 once
    const add1_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item1" },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add1_args);

    // Add item2 twice
    const add2_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item2" },
    };
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add2_args);
    _ = try cuckoo_cmds.cmdCfAdd(allocator, &storage, &add2_args);

    // Count item1
    const count1_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item1" },
    };
    const result1 = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count1_args);
    switch (result1) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 1), count),
        else => return error.UnexpectedResult,
    }

    // Count item2
    const count2_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item2" },
    };
    const result2 = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count2_args);
    switch (result2) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 2), count),
        else => return error.UnexpectedResult,
    }
}

test "CF.COUNT: WRONGTYPE error for non-cuckoo key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "string_key";

    // Add a string value
    const owned_key = try allocator.dupe(u8, key);
    const str_value = try allocator.dupe(u8, "some_value");
    try storage.data.put(owned_key, storage_mod.Value{ .string = str_value });

    // Try to count on string key
    const args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item" },
    };

    const result = try cuckoo_cmds.cmdCfCount(allocator, &storage, &args);

    switch (result) {
        .error_string => |err| try std.testing.expect(std.mem.startsWith(u8, err, "WRONGTYPE")),
        else => return error.ExpectedWrongTypeError,
    }
}

test "CF.COUNT: arity error with wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Test with 1 argument
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
    };
    const result1 = try cuckoo_cmds.cmdCfCount(allocator, &storage, &args1);
    switch (result1) {
        .error_string => |err| try std.testing.expect(std.mem.startsWith(u8, err, "ERR wrong number")),
        else => return error.ExpectedArityError,
    }

    // Test with 3 arguments
    const args3 = [_]RespValue{
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "item" },
        RespValue{ .bulk_string = "extra" },
    };
    const result3 = try cuckoo_cmds.cmdCfCount(allocator, &storage, &args3);
    switch (result3) {
        .error_string => |err| try std.testing.expect(std.mem.startsWith(u8, err, "ERR wrong number")),
        else => return error.ExpectedArityError,
    }
}

test "CF.COUNT: works with CF.INSERT" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "my_cf";

    // Use CF.INSERT to add multiple items including duplicates
    const insert_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "ITEMS" },
        RespValue{ .bulk_string = "item1" },
        RespValue{ .bulk_string = "item2" },
        RespValue{ .bulk_string = "item2" },
    };
    _ = try cuckoo_cmds.cmdCfInsert(allocator, &storage, &insert_args);

    // Count item1 (should be 1)
    const count1_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item1" },
    };
    const result1 = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count1_args);
    switch (result1) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 1), count),
        else => return error.UnexpectedResult,
    }

    // Count item2 (should be 2)
    const count2_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item2" },
    };
    const result2 = try cuckoo_cmds.cmdCfCount(allocator, &storage, &count2_args);
    switch (result2) {
        .integer => |count| try std.testing.expectEqual(@as(i64, 2), count),
        else => return error.UnexpectedResult,
    }
}
