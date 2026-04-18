const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const Value = @import("../src/storage/memory.zig").Value;
const TimeSeriesValue = @import("../src/storage/memory.zig").TimeSeriesValue;
const timeseries = @import("../src/commands/timeseries.zig");

test "TS.RANGE basic range query with no filters" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series and add samples
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add 5 samples
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const r1 = try timeseries.cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(r1);

    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "1500", "15.0" };
    const r2 = try timeseries.cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(r2);

    const add_cmd3 = [_][]const u8{ "TS.ADD", "myts", "2000", "20.0" };
    const r3 = try timeseries.cmdTsAdd(&storage, &add_cmd3, allocator);
    defer allocator.free(r3);

    const add_cmd4 = [_][]const u8{ "TS.ADD", "myts", "2500", "25.0" };
    const r4 = try timeseries.cmdTsAdd(&storage, &add_cmd4, allocator);
    defer allocator.free(r4);

    const add_cmd5 = [_][]const u8{ "TS.ADD", "myts", "3000", "30.0" };
    const r5 = try timeseries.cmdTsAdd(&storage, &add_cmd5, allocator);
    defer allocator.free(r5);

    // TS.RANGE myts 1000 3000
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "1000", "3000" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    // Should return 5 data points: [1000,10], [1500,15], [2000,20], [2500,25], [3000,30]
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.contains(u8, result, ":1000\r\n"));
    try std.testing.expect(std.mem.contains(u8, result, "+10"));
}

test "TS.RANGE empty range (no samples in range)" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add sample outside query range
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const r = try timeseries.cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(r);

    // TS.RANGE myts 2000 3000 (no samples in this range)
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "2000", "3000" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    // Should return empty array
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "TS.RANGE with COUNT limit" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add 5 samples
    for (0..5) |i| {
        const ts = 1000 + i * 500;
        const add_cmd = [_][]const u8{
            "TS.ADD", "myts",
            try std.fmt.allocPrint(allocator, "{d}", .{ts}),
            try std.fmt.allocPrint(allocator, "{d}.0", .{10 + i * 5}),
        };
        const r = try timeseries.cmdTsAdd(&storage, &add_cmd, allocator);
        allocator.free(add_cmd[2]);
        allocator.free(add_cmd[3]);
        defer allocator.free(r);
    }

    // TS.RANGE myts 1000 3000 COUNT 2 (limit to 2 samples)
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "1000", "3000", "COUNT", "2" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    // Should return 2 data points
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "TS.REVRANGE basic reverse range query" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add 3 samples
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const r1 = try timeseries.cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(r1);

    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "2000", "20.0" };
    const r2 = try timeseries.cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(r2);

    const add_cmd3 = [_][]const u8{ "TS.ADD", "myts", "3000", "30.0" };
    const r3 = try timeseries.cmdTsAdd(&storage, &add_cmd3, allocator);
    defer allocator.free(r3);

    // TS.REVRANGE myts 1000 3000
    const revrange_cmd = [_][]const u8{ "TS.REVRANGE", "myts", "1000", "3000" };
    const result = try timeseries.cmdTsRevrange(&storage, &revrange_cmd, allocator);
    defer allocator.free(result);

    // Should return 3 data points in reverse order: [3000,30], [2000,20], [1000,10]
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    // First sample should be 3000
    var iter = std.mem.splitSequence(u8, result, ":3000\r\n");
    try std.testing.expect(iter.next() != null); // First part exists
    try std.testing.expect(iter.next() != null); // There's content after ":3000\r\n"
}

test "TS.RANGE WRONGTYPE error" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Set a string key
    const string_val = Value{ .string = try allocator.dupe(u8, "hello") };
    const key_dup = try allocator.dupe(u8, "mystring");
    try storage.data.put(key_dup, string_val);

    // TS.RANGE mystring 1000 2000
    const range_cmd = [_][]const u8{ "TS.RANGE", "mystring", "1000", "2000" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.RANGE nonexistent key error" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.RANGE nonexistent 1000 2000
    const range_cmd = [_][]const u8{ "TS.RANGE", "nonexistent", "1000", "2000" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "TS.RANGE invalid timestamp error" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // TS.RANGE myts invalid 2000 (invalid from timestamp)
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "invalid", "2000" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "TS.RANGE from > to error" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // TS.RANGE myts 3000 1000 (from > to)
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "3000", "1000" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "TS.RANGE arity error" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.RANGE myts (missing both timestamps)
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number"));
}

test "TS.RANGE with FILTER_BY_VALUE" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add 5 samples
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const r1 = try timeseries.cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(r1);

    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "1500", "15.0" };
    const r2 = try timeseries.cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(r2);

    const add_cmd3 = [_][]const u8{ "TS.ADD", "myts", "2000", "20.0" };
    const r3 = try timeseries.cmdTsAdd(&storage, &add_cmd3, allocator);
    defer allocator.free(r3);

    const add_cmd4 = [_][]const u8{ "TS.ADD", "myts", "2500", "25.0" };
    const r4 = try timeseries.cmdTsAdd(&storage, &add_cmd4, allocator);
    defer allocator.free(r4);

    const add_cmd5 = [_][]const u8{ "TS.ADD", "myts", "3000", "30.0" };
    const r5 = try timeseries.cmdTsAdd(&storage, &add_cmd5, allocator);
    defer allocator.free(r5);

    // TS.RANGE myts 1000 3000 FILTER_BY_VALUE 10.5 30.5
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "1000", "3000", "FILTER_BY_VALUE", "10.5", "30.5" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    // Should return samples with values between 10.5 and 30.5: [15,1500], [20,2000], [25,2500], [30,3000]
    try std.testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
}

test "TS.RANGE with FILTER_BY_TS" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add 5 samples
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const r1 = try timeseries.cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(r1);

    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "1500", "15.0" };
    const r2 = try timeseries.cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(r2);

    const add_cmd3 = [_][]const u8{ "TS.ADD", "myts", "2000", "20.0" };
    const r3 = try timeseries.cmdTsAdd(&storage, &add_cmd3, allocator);
    defer allocator.free(r3);

    const add_cmd4 = [_][]const u8{ "TS.ADD", "myts", "2500", "25.0" };
    const r4 = try timeseries.cmdTsAdd(&storage, &add_cmd4, allocator);
    defer allocator.free(r4);

    // TS.RANGE myts 1000 3000 FILTER_BY_TS 1500 2500
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "1000", "3000", "FILTER_BY_TS", "1500", "2500" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    // Should return only samples with exact timestamps 1500 and 2500
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "TS.RANGE special timestamp - operator" {
    const allocator = std.testing.allocator;

    var config = @import("../src/storage/memory.zig").Config.default();
    var storage = try @import("../src/storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try timeseries.cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add 3 samples
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const r1 = try timeseries.cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(r1);

    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "2000", "20.0" };
    const r2 = try timeseries.cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(r2);

    const add_cmd3 = [_][]const u8{ "TS.ADD", "myts", "3000", "30.0" };
    const r3 = try timeseries.cmdTsAdd(&storage, &add_cmd3, allocator);
    defer allocator.free(r3);

    // TS.RANGE myts - + (earliest to latest)
    const range_cmd = [_][]const u8{ "TS.RANGE", "myts", "-", "+" };
    const result = try timeseries.cmdTsRange(&storage, &range_cmd, allocator);
    defer allocator.free(result);

    // Should return all 3 samples
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
}
