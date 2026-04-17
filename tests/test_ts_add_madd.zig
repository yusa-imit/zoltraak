const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const Config = @import("../src/storage/memory.zig").Config;
const parser = @import("../src/protocol/parser.zig");
const handleCommand = @import("../src/commands/strings.zig").handleCommand;
const RespValue = parser.RespValue;

// ============================================================================
// Tests for TS.ADD command
// ============================================================================

test "TS.ADD basic auto-create" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD sensor:temp 1000 42.5
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "42.5" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings(":1000\r\n", result);

    // Verify key exists and is timeseries
    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expect(value.?.* == .timeseries);
    try testing.expectEqual(@as(usize, 1), value.?.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 42.5), value.?.timeseries.samples.items[0].value);
    try testing.expectEqual(@as(i64, 1000), value.?.timeseries.samples.items[0].timestamp);
}

test "TS.ADD with wildcard timestamp" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD sensor:temp * 25.3
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "25.3" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify it returned a valid timestamp
    try testing.expect(std.mem.startsWith(u8, result, ":"));
    try testing.expect(std.mem.endsWith(u8, result, "\r\n"));

    // Verify sample was added
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(usize, 1), value.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 25.3), value.timeseries.samples.items[0].value);
}

test "TS.ADD with all creation options" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD sensor:temp 1000 42.5 RETENTION 86400000 ENCODING COMPRESSED CHUNK_SIZE 8192 DUPLICATE_POLICY SUM LABELS sensor temp location room1
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "42.5" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "86400000" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "COMPRESSED" },
        RespValue{ .bulk_string = "CHUNK_SIZE" },
        RespValue{ .bulk_string = "8192" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "SUM" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "sensor" },
        RespValue{ .bulk_string = "temp" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room1" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings(":1000\r\n", result);

    // Verify all options were set
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(i64, 86400000), value.timeseries.info.retention_ms);
    try testing.expect(value.timeseries.info.encoding == .compressed);
    try testing.expectEqual(@as(u32, 8192), value.timeseries.info.chunk_size);
    try testing.expect(value.timeseries.info.duplicate_policy == .sum);
    try testing.expectEqual(@as(usize, 2), value.timeseries.info.labels.count());
}

test "TS.ADD to existing time series ignores creation options" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series first with specific config
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "3600000" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "LAST" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Add with different options - should be ignored
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "42.5" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "86400000" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "SUM" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings(":1000\r\n", result);

    // Verify original config is preserved
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(i64, 3600000), value.timeseries.info.retention_ms);
    try testing.expect(value.timeseries.info.duplicate_policy == .last);
}

test "TS.ADD with ON_DUPLICATE FIRST" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Second add with same timestamp but ON_DUPLICATE FIRST
    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "20.0" },
        RespValue{ .bulk_string = "ON_DUPLICATE" },
        RespValue{ .bulk_string = "FIRST" },
    };
    const result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(result2);

    try testing.expectEqualStrings(":1000\r\n", result2);

    // Verify first value is kept
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(f64, 10.0), value.timeseries.samples.items[0].value);
}

test "TS.ADD with ON_DUPLICATE MIN" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "30.0" },
    };
    const result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Second add with same timestamp but ON_DUPLICATE MIN
    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "20.0" },
        RespValue{ .bulk_string = "ON_DUPLICATE" },
        RespValue{ .bulk_string = "MIN" },
    };
    const result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(result2);

    try testing.expectEqualStrings(":1000\r\n", result2);

    // Verify minimum value is kept
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(f64, 20.0), value.timeseries.samples.items[0].value);
}

test "TS.ADD with ON_DUPLICATE MAX" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Second add with same timestamp but ON_DUPLICATE MAX
    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "30.0" },
        RespValue{ .bulk_string = "ON_DUPLICATE" },
        RespValue{ .bulk_string = "MAX" },
    };
    const result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(result2);

    try testing.expectEqualStrings(":1000\r\n", result2);

    // Verify maximum value is kept
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(f64, 30.0), value.timeseries.samples.items[0].value);
}

test "TS.ADD with ON_DUPLICATE SUM" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Second add with same timestamp but ON_DUPLICATE SUM
    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5.0" },
        RespValue{ .bulk_string = "ON_DUPLICATE" },
        RespValue{ .bulk_string = "SUM" },
    };
    const result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(result2);

    try testing.expectEqualStrings(":1000\r\n", result2);

    // Verify values are summed
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(f64, 15.0), value.timeseries.samples.items[0].value);
}

test "TS.ADD with ON_DUPLICATE BLOCK error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Try to add duplicate timestamp without override
    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "20.0" },
    };
    const result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(result2);

    try testing.expect(std.mem.startsWith(u8, result2, "ERR DUPLICATE_POLICY is BLOCK"));
}

test "TS.ADD wrong number of arguments" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Too few arguments
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.ADD WRONGTYPE error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create a string key first
    const set_cmd = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "value" },
    };
    const set_result = try handleCommand(allocator, &storage, &set_cmd, 0, null, null, null);
    defer allocator.free(set_result);

    // Try to TS.ADD to string key
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "42.5" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.ADD invalid timestamp" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "not_a_timestamp" },
        RespValue{ .bulk_string = "42.5" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid timestamp"));
}

test "TS.ADD invalid value" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "not_a_number" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid value"));
}

test "TS.ADD unknown option" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "42.5" },
        RespValue{ .bulk_string = "INVALID_OPTION" },
        RespValue{ .bulk_string = "value" },
    };

    const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR unknown option"));
}

test "TS.ADD duplicate BLOCK error when series has BLOCK policy" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Try to add duplicate
    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "20.0" },
    };
    const result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(result2);

    try testing.expect(std.mem.startsWith(u8, result2, "ERR DUPLICATE_POLICY is BLOCK"));
}

test "TS.ADD multiple samples in order" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Add multiple samples with increasing timestamps
    const timestamps = [_]i64{ 1000, 2000, 3000, 4000, 5000 };
    const values = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };

    for (0..timestamps.len) |i| {
        const ts_str = try std.fmt.allocPrint(allocator, "{d}", .{timestamps[i]});
        defer allocator.free(ts_str);
        const val_str = try std.fmt.allocPrint(allocator, "{d}", .{values[i]});
        defer allocator.free(val_str);

        const add_cmd = [_]RespValue{
            RespValue{ .bulk_string = "TS.ADD" },
            RespValue{ .bulk_string = "sensor:temp" },
            RespValue{ .bulk_string = ts_str },
            RespValue{ .bulk_string = val_str },
        };

        const result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
        defer allocator.free(result);
    }

    // Verify all samples are stored
    const value = storage.get("sensor:temp").?;
    try testing.expectEqual(@as(usize, 5), value.timeseries.samples.items.len);

    // Verify samples are sorted by timestamp
    for (0..timestamps.len) |i| {
        try testing.expectEqual(timestamps[i], value.timeseries.samples.items[i].timestamp);
        try testing.expectEqual(values[i], value.timeseries.samples.items[i].value);
    }
}

// ============================================================================
// Tests for TS.MADD command
// ============================================================================

test "TS.MADD basic multiple keys" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create two time series
    const create_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "ts1" },
    };
    const create_result1 = try handleCommand(allocator, &storage, &create_cmd1, 0, null, null, null);
    defer allocator.free(create_result1);

    const create_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "ts2" },
    };
    const create_result2 = try handleCommand(allocator, &storage, &create_cmd2, 0, null, null, null);
    defer allocator.free(create_result2);

    // TS.MADD ts1 1000 10.0 ts2 2000 20.0
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "ts1" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
        RespValue{ .bulk_string = "ts2" },
        RespValue{ .bulk_string = "2000" },
        RespValue{ .bulk_string = "20.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("*2\r\n:1000\r\n:2000\r\n", result);

    // Verify both samples were added
    const val1 = storage.get("ts1").?;
    const val2 = storage.get("ts2").?;
    try testing.expectEqual(@as(usize, 1), val1.timeseries.samples.items.len);
    try testing.expectEqual(@as(usize, 1), val2.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 10.0), val1.timeseries.samples.items[0].value);
    try testing.expectEqual(@as(f64, 20.0), val2.timeseries.samples.items[0].value);
}

test "TS.MADD same key multiple times" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create one time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // TS.MADD myts 1000 10.0 myts 2000 20.0
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "2000" },
        RespValue{ .bulk_string = "20.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("*2\r\n:1000\r\n:2000\r\n", result);

    // Verify both samples were added to same series
    const value = storage.get("myts").?;
    try testing.expectEqual(@as(usize, 2), value.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 10.0), value.timeseries.samples.items[0].value);
    try testing.expectEqual(@as(f64, 20.0), value.timeseries.samples.items[1].value);
}

test "TS.MADD with wildcard timestamps" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // TS.MADD myts * 10.0 myts * 20.0
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "10.0" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "20.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify it returned array of 2 timestamps
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));

    // Verify both samples were added
    const value = storage.get("myts").?;
    try testing.expectEqual(@as(usize, 2), value.timeseries.samples.items.len);
}

test "TS.MADD nonexistent key error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.MADD nonexistent 1000 10.0
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR key does not exist"));
}

test "TS.MADD WRONGTYPE error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create a string key
    const set_cmd = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "value" },
    };
    const set_result = try handleCommand(allocator, &storage, &set_cmd, 0, null, null, null);
    defer allocator.free(set_result);

    // TS.MADD to string key
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.MADD wrong arity" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Wrong arity (1 + 2 args instead of 1 + 3N)
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.MADD invalid timestamp in triplet" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // TS.MADD myts invalid 10.0
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "invalid" },
        RespValue{ .bulk_string = "10.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid timestamp"));
}

test "TS.MADD invalid value in triplet" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // TS.MADD myts 1000 invalid_value
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "invalid_value" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid value"));
}

test "TS.MADD duplicate BLOCK error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const add_result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(add_result);

    // Try MADD with duplicate timestamp
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "20.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR DUPLICATE_POLICY is BLOCK"));
}

test "TS.MADD multiple triplets" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create three time series
    for ([_][]const u8{ "ts1", "ts2", "ts3" }) |tsname| {
        const create_cmd = [_]RespValue{
            RespValue{ .bulk_string = "TS.CREATE" },
            RespValue{ .bulk_string = tsname },
        };
        const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
        defer allocator.free(create_result);
    }

    // TS.MADD ts1 1000 10.0 ts2 2000 20.0 ts3 3000 30.0
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "ts1" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
        RespValue{ .bulk_string = "ts2" },
        RespValue{ .bulk_string = "2000" },
        RespValue{ .bulk_string = "20.0" },
        RespValue{ .bulk_string = "ts3" },
        RespValue{ .bulk_string = "3000" },
        RespValue{ .bulk_string = "30.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("*3\r\n:1000\r\n:2000\r\n:3000\r\n", result);

    // Verify all samples were added
    const val1 = storage.get("ts1").?;
    const val2 = storage.get("ts2").?;
    const val3 = storage.get("ts3").?;
    try testing.expectEqual(@as(f64, 10.0), val1.timeseries.samples.items[0].value);
    try testing.expectEqual(@as(f64, 20.0), val2.timeseries.samples.items[0].value);
    try testing.expectEqual(@as(f64, 30.0), val3.timeseries.samples.items[0].value);
}

test "TS.MADD respects duplicate policy from creation" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with SUM policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "SUM" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // First add via TS.ADD
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
    };
    const add_result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(add_result);

    // Add duplicate via MADD - should sum
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify values were summed
    const value = storage.get("myts").?;
    try testing.expectEqual(@as(f64, 15.0), value.timeseries.samples.items[0].value);
}

test "TS.MADD stores correct timestamps in order" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Add samples with different timestamps
    const madd_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MADD" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "3000" },
        RespValue{ .bulk_string = "30.0" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10.0" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "2000" },
        RespValue{ .bulk_string = "20.0" },
    };

    const result = try handleCommand(allocator, &storage, &madd_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("*3\r\n:3000\r\n:1000\r\n:2000\r\n", result);

    // Verify samples are sorted internally by timestamp
    const value = storage.get("myts").?;
    try testing.expectEqual(@as(usize, 3), value.timeseries.samples.items.len);
    try testing.expectEqual(@as(i64, 1000), value.timeseries.samples.items[0].timestamp);
    try testing.expectEqual(@as(i64, 2000), value.timeseries.samples.items[1].timestamp);
    try testing.expectEqual(@as(i64, 3000), value.timeseries.samples.items[2].timestamp);
}
