const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const protocol = @import("../src/protocol/parser.zig");
const topk_cmds = @import("../src/commands/topk.zig");

test "TOPK.INCRBY: basic increment" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create Top-K filter
    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    const result_reserve = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);
    try std.testing.expectEqual(.simple_string, @as(std.meta.Tag(@TypeOf(result_reserve)), @as(@TypeOf(result_reserve), result_reserve)));

    // Increment item
    var args_incrby = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INCRBY" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "apple" },
        protocol.RespValue{ .bulk_string = "5" },
    };
    const result = try topk_cmds.cmdTopkIncrby(allocator, &storage, &args_incrby);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
    try std.testing.expectEqual(.null_bulk_string, @as(std.meta.Tag(@TypeOf(result.array[0])), @as(@TypeOf(result.array[0]), result.array[0])));
}

test "TOPK.INCRBY: auto-create filter" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // INCRBY on nonexistent key auto-creates with defaults
    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INCRBY" },
        protocol.RespValue{ .bulk_string = "newkey" },
        protocol.RespValue{ .bulk_string = "item1" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    const result = try topk_cmds.cmdTopkIncrby(allocator, &storage, &args);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
}

test "TOPK.INCRBY: multiple item-increment pairs" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create Top-K
    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    // Increment multiple items
    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INCRBY" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "apple" },
        protocol.RespValue{ .bulk_string = "10" },
        protocol.RespValue{ .bulk_string = "banana" },
        protocol.RespValue{ .bulk_string = "5" },
        protocol.RespValue{ .bulk_string = "cherry" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    const result = try topk_cmds.cmdTopkIncrby(allocator, &storage, &args);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    try std.testing.expectEqual(@as(usize, 3), result.array.len);
}

test "TOPK.INCRBY: arity error with odd arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INCRBY" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "apple" },
        // Missing increment value
    };
    const result = try topk_cmds.cmdTopkIncrby(allocator, &storage, &args);
    try std.testing.expectEqual(.error_string, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
}

test "TOPK.INCRBY: invalid increment" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INCRBY" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "apple" },
        protocol.RespValue{ .bulk_string = "notanumber" },
    };
    const result = try topk_cmds.cmdTopkIncrby(allocator, &storage, &args);
    try std.testing.expectEqual(.error_string, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
}

test "TOPK.LIST: empty Top-K" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create empty Top-K
    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    // List items
    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.LIST" },
        protocol.RespValue{ .bulk_string = "mykey" },
    };
    const result = try topk_cmds.cmdTopkList(allocator, &storage, &args);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
}

test "TOPK.LIST: basic list without count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create and populate Top-K
    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    var args_add = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.ADD" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "apple" },
        protocol.RespValue{ .bulk_string = "banana" },
    };
    const result_add = try topk_cmds.cmdTopkAdd(allocator, &storage, &args_add);
    defer {
        if (result_add == .array) {
            for (result_add.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result_add.array);
        }
    }

    // List items
    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.LIST" },
        protocol.RespValue{ .bulk_string = "mykey" },
    };
    const result = try topk_cmds.cmdTopkList(allocator, &storage, &args);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqual(.bulk_string, @as(std.meta.Tag(@TypeOf(result.array[0])), @as(@TypeOf(result.array[0]), result.array[0])));
}

test "TOPK.LIST: with WITHCOUNT flag" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create and populate
    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    var args_add = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.ADD" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "apple" },
    };
    const result_add = try topk_cmds.cmdTopkAdd(allocator, &storage, &args_add);
    defer {
        if (result_add == .array) {
            for (result_add.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result_add.array);
        }
    }

    // List with count
    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.LIST" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "WITHCOUNT" },
    };
    const result = try topk_cmds.cmdTopkList(allocator, &storage, &args);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    // WITHCOUNT returns [item, count] pairs, so 2 elements per item
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqual(.bulk_string, @as(std.meta.Tag(@TypeOf(result.array[0])), @as(@TypeOf(result.array[0]), result.array[0])));
    try std.testing.expectEqual(.integer, @as(std.meta.Tag(@TypeOf(result.array[1])), @as(@TypeOf(result.array[1]), result.array[1])));
}

test "TOPK.LIST: nonexistent key error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.LIST" },
        protocol.RespValue{ .bulk_string = "nonexistent" },
    };
    const result = try topk_cmds.cmdTopkList(allocator, &storage, &args);
    try std.testing.expectEqual(.error_string, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
}

test "TOPK.LIST: invalid flag" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "3" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.LIST" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "INVALID" },
    };
    const result = try topk_cmds.cmdTopkList(allocator, &storage, &args);
    try std.testing.expectEqual(.error_string, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
}

test "TOPK.INFO: basic metadata" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create Top-K with specific parameters
    var args_reserve = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.RESERVE" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "5" },
        protocol.RespValue{ .bulk_string = "16" },
        protocol.RespValue{ .bulk_string = "9" },
        protocol.RespValue{ .bulk_string = "0.85" },
    };
    _ = try topk_cmds.cmdTopkReserve(allocator, &storage, &args_reserve);

    // Get info
    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INFO" },
        protocol.RespValue{ .bulk_string = "mykey" },
    };
    const result = try topk_cmds.cmdTopkInfo(allocator, &storage, &args);
    defer {
        if (result == .array) {
            for (result.array) |*r| {
                if (r.* == .bulk_string) allocator.free(r.bulk_string);
            }
            allocator.free(result.array);
        }
    }

    try std.testing.expectEqual(.array, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
    try std.testing.expectEqual(@as(usize, 4), result.array.len);

    // Verify k
    try std.testing.expectEqual(.integer, @as(std.meta.Tag(@TypeOf(result.array[0])), @as(@TypeOf(result.array[0]), result.array[0])));
    try std.testing.expectEqual(@as(i64, 5), result.array[0].integer);

    // Verify width
    try std.testing.expectEqual(.integer, @as(std.meta.Tag(@TypeOf(result.array[1])), @as(@TypeOf(result.array[1]), result.array[1])));
    try std.testing.expectEqual(@as(i64, 16), result.array[1].integer);

    // Verify depth
    try std.testing.expectEqual(.integer, @as(std.meta.Tag(@TypeOf(result.array[2])), @as(@TypeOf(result.array[2]), result.array[2])));
    try std.testing.expectEqual(@as(i64, 9), result.array[2].integer);

    // Verify decay (as string)
    try std.testing.expectEqual(.bulk_string, @as(std.meta.Tag(@TypeOf(result.array[3])), @as(@TypeOf(result.array[3]), result.array[3])));
}

test "TOPK.INFO: nonexistent key error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INFO" },
        protocol.RespValue{ .bulk_string = "nonexistent" },
    };
    const result = try topk_cmds.cmdTopkInfo(allocator, &storage, &args);
    try std.testing.expectEqual(.error_string, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
}

test "TOPK.INFO: arity error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TOPK.INFO" },
        // Missing key argument
    };
    const result = try topk_cmds.cmdTopkInfo(allocator, &storage, &args);
    try std.testing.expectEqual(.error_string, @as(std.meta.Tag(@TypeOf(result)), @as(@TypeOf(result), result)));
}
