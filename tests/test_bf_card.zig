const std = @import("std");
const protocol = @import("zoltraak").protocol.parser;
const bloom_cmd = @import("zoltraak").commands.bloom;
const storage_mod = @import("zoltraak").storage.memory;

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

// ── Integration Tests ────────────────────────────────────────────────────────

test "BF.CARD: basic cardinality tracking" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    // Create a Bloom filter
    const reserve_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "0.01" },
        .{ .bulk_string = "100" },
    };
    const reserve_result = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);
    try std.testing.expectEqualStrings("OK", reserve_result.simple_string);

    // Initially cardinality should be 0
    const card_args_0 = [_]RespValue{
        .{ .bulk_string = "mybloom" },
    };
    const card_result_0 = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args_0);
    try std.testing.expectEqual(@as(i64, 0), card_result_0.integer);

    // Add 3 items
    const add_args_1 = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "item1" },
    };
    _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args_1);

    const add_args_2 = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "item2" },
    };
    _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args_2);

    const add_args_3 = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "item3" },
    };
    _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args_3);

    // Cardinality should be 3
    const card_args_3 = [_]RespValue{
        .{ .bulk_string = "mybloom" },
    };
    const card_result_3 = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args_3);
    try std.testing.expectEqual(@as(i64, 3), card_result_3.integer);
}

test "BF.CARD: nonexistent key returns 0" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "nonexistent" },
    };
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &args);
    try std.testing.expectEqual(@as(i64, 0), result.integer);
}

test "BF.CARD: wrong type error" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    // Create a string key
    const set_args = [_]RespValue{
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "value" },
    };
    const strings = @import("zoltraak").commands.strings;
    _ = try strings.cmdSet(std.testing.allocator, &storage, &set_args);

    // BF.CARD on string should fail
    const card_args = [_]RespValue{
        .{ .bulk_string = "mystring" },
    };
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args);
    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "WRONGTYPE") != null);
}

test "BF.CARD: arity error with no arguments" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    const args = [_]RespValue{};
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &args);
    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number of arguments") != null);
}

test "BF.CARD: arity error with too many arguments" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "extra" },
    };
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &args);
    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number of arguments") != null);
}

test "BF.CARD: duplicate items tracked correctly" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    // Create a Bloom filter
    const reserve_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "0.01" },
        .{ .bulk_string = "100" },
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // Add same item twice
    const add_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "item1" },
    };
    _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args);
    _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args);

    // Cardinality should be 2 (both adds are counted)
    const card_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
    };
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args);
    try std.testing.expectEqual(@as(i64, 2), result.integer);
}

test "BF.CARD: matches BF.INFO items field" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    // Create a Bloom filter
    const reserve_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "0.01" },
        .{ .bulk_string = "100" },
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // Add some items
    for (0..10) |i| {
        const item = try std.fmt.allocPrint(std.testing.allocator, "item{d}", .{i});
        defer std.testing.allocator.free(item);
        const add_args = [_]RespValue{
            .{ .bulk_string = "mybloom" },
            .{ .bulk_string = item },
        };
        _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args);
    }

    // Get cardinality
    const card_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
    };
    const card_result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args);

    // Get BF.INFO ITEMS field
    const info_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "items" },
    };
    const info_result = try bloom_cmd.cmdBfInfo(std.testing.allocator, &storage, &info_args);
    defer info_result.deinit(std.testing.allocator);

    // They should match
    try std.testing.expectEqual(@as(i64, 10), card_result.integer);
    try std.testing.expectEqual(@as(i64, 10), info_result.integer);
}

test "BF.CARD: cardinality with NONSCALING filter" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    // Create NONSCALING filter
    const reserve_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "0.01" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "NONSCALING" },
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // Add items beyond capacity
    for (0..15) |i| {
        const item = try std.fmt.allocPrint(std.testing.allocator, "item{d}", .{i});
        defer std.testing.allocator.free(item);
        const add_args = [_]RespValue{
            .{ .bulk_string = "mybloom" },
            .{ .bulk_string = item },
        };
        _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args);
    }

    // Cardinality should still be 15 (all adds counted)
    const card_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
    };
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args);
    try std.testing.expectEqual(@as(i64, 15), result.integer);
}

test "BF.CARD: cardinality with auto-scaling filter" {
    var storage = try Storage.init(std.testing.allocator, 16);
    defer storage.deinit();

    // Create auto-scaling filter with small capacity
    const reserve_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
        .{ .bulk_string = "0.01" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "EXPANSION" },
        .{ .bulk_string = "2" },
    };
    _ = try bloom_cmd.cmdBfReserve(std.testing.allocator, &storage, &reserve_args);

    // Add items beyond capacity to trigger scaling
    for (0..20) |i| {
        const item = try std.fmt.allocPrint(std.testing.allocator, "item{d}", .{i});
        defer std.testing.allocator.free(item);
        const add_args = [_]RespValue{
            .{ .bulk_string = "mybloom" },
            .{ .bulk_string = item },
        };
        _ = try bloom_cmd.cmdBfAdd(std.testing.allocator, &storage, &add_args);
    }

    // Cardinality should be 20 (all adds counted across sub-filters)
    const card_args = [_]RespValue{
        .{ .bulk_string = "mybloom" },
    };
    const result = try bloom_cmd.cmdBfCard(std.testing.allocator, &storage, &card_args);
    try std.testing.expectEqual(@as(i64, 20), result.integer);
}
