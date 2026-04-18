const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const Config = @import("../src/storage/memory.zig").Config;
const parser = @import("../src/protocol/parser.zig");
const handleCommand = @import("../src/commands/strings.zig").handleCommand;
const RespValue = parser.RespValue;

// ============================================================================
// Tests for TS.INCRBY and TS.DECRBY commands
// ============================================================================

test "TS.INCRBY storage layer - empty series" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Increment empty series at timestamp 1000 by 5.0
    try ts.incrementBy(1000, 5.0);

    try testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try testing.expectEqual(@as(i64, 1000), ts.samples.items[0].timestamp);
    try testing.expectEqual(@as(f64, 5.0), ts.samples.items[0].value);
    try testing.expectEqual(@as(u64, 1), ts.info.total_samples);
}

test "TS.INCRBY storage layer - existing series" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Add initial sample
    try ts.add(1000, 10.0, null);

    // Increment at same timestamp - should modify value
    try ts.incrementBy(1000, 5.0);

    try testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try testing.expectEqual(@as(f64, 15.0), ts.samples.items[0].value);
    try testing.expectEqual(@as(u64, 1), ts.info.total_samples);
}

test "TS.INCRBY storage layer - negative delta" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Add initial sample
    try ts.add(1000, 10.0, null);

    // Decrement with negative delta
    try ts.incrementBy(1000, -3.0);

    try testing.expectEqual(@as(f64, 7.0), ts.samples.items[0].value);
}

test "TS.INCRBY storage layer - multiple samples" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Add samples at different timestamps
    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);
    try ts.add(3000, 30.0, null);

    // Increment middle sample
    try ts.incrementBy(2000, 5.0);

    try testing.expectEqual(@as(usize, 3), ts.samples.items.len);
    try testing.expectEqual(@as(f64, 10.0), ts.samples.items[0].value);
    try testing.expectEqual(@as(f64, 25.0), ts.samples.items[1].value);
    try testing.expectEqual(@as(f64, 30.0), ts.samples.items[2].value);
}

test "TS.INCRBY storage layer - creates new sample if timestamp missing" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Add samples
    try ts.add(1000, 10.0, null);
    try ts.add(3000, 30.0, null);

    // Increment at missing timestamp between them
    try ts.incrementBy(2000, 5.0);

    try testing.expectEqual(@as(usize, 3), ts.samples.items.len);
    try testing.expectEqual(@as(i64, 1000), ts.samples.items[0].timestamp);
    try testing.expectEqual(@as(i64, 2000), ts.samples.items[1].timestamp);
    try testing.expectEqual(@as(f64, 5.0), ts.samples.items[1].value);
    try testing.expectEqual(@as(i64, 3000), ts.samples.items[2].timestamp);
}

test "TS.INCRBY storage layer - respects retention policy" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    ts.info.retention_ms = 1000; // 1 second retention

    // Add old samples
    try ts.add(1000, 10.0, null);
    try ts.add(1500, 15.0, null);

    // Increment at newer timestamp - triggers retention
    try ts.incrementBy(2500, 5.0);

    // Old samples should be removed
    try testing.expectEqual(@as(usize, 2), ts.samples.items.len);
    try testing.expectEqual(@as(i64, 1500), ts.samples.items[0].timestamp);
    try testing.expectEqual(@as(i64, 2500), ts.samples.items[1].timestamp);
    try testing.expectEqual(@as(f64, 5.0), ts.samples.items[1].value);
}

test "TS.DECRBY storage layer - empty series" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Decrement empty series
    try ts.decrementBy(1000, 3.0);

    try testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try testing.expectEqual(@as(i64, 1000), ts.samples.items[0].timestamp);
    try testing.expectEqual(@as(f64, -3.0), ts.samples.items[0].value);
}

test "TS.DECRBY storage layer - existing series" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Add initial sample
    try ts.add(1000, 10.0, null);

    // Decrement at same timestamp
    try ts.decrementBy(1000, 3.0);

    try testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try testing.expectEqual(@as(f64, 7.0), ts.samples.items[0].value);
}

test "TS.DECRBY storage layer - negative delta (becomes addition)" {
    const allocator = testing.allocator;
    var ts = try @import("../src/storage/timeseries.zig").TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Add initial sample
    try ts.add(1000, 10.0, null);

    // Decrement with negative delta (effectively adds)
    try ts.decrementBy(1000, -5.0);

    try testing.expectEqual(@as(f64, 15.0), ts.samples.items[0].value);
}

test "TS.INCRBY command - basic auto-create" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.INCRBY sensor:count 1000 5
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "sensor:count" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return bulk string with new value
    try testing.expect(std.mem.startsWith(u8, result, "$"));

    // Verify key exists and is timeseries
    const value = storage.get("sensor:count");
    try testing.expect(value != null);
    try testing.expect(value.?.* == .timeseries);
    try testing.expectEqual(@as(usize, 1), value.?.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 5.0), value.?.timeseries.samples.items[0].value);
}

test "TS.INCRBY command - increment existing sample" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create and add initial sample
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "counter" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10" },
    };
    const add_result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(add_result);

    // Increment at same timestamp
    const incrby_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try handleCommand(allocator, &storage, &incrby_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify value was incremented
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(f64, 15.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.INCRBY command - wildcard timestamp" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.INCRBY counter * 5
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return a valid timestamp (not *)
    try testing.expect(std.mem.startsWith(u8, result, "$"));
    try testing.expect(!std.mem.startsWith(u8, result, "$1\r\n*"));

    // Verify sample was added
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(usize, 1), ts_value.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 5.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.INCRBY command - with RETENTION option" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.INCRBY counter 1000 5 RETENTION 86400000
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "86400000" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify retention was set
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(i64, 86400000), ts_value.timeseries.info.retention_ms);
}

test "TS.INCRBY command - with LABELS option" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.INCRBY counter 1000 5 LABELS type counter location room1
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room1" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify labels were set
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(usize, 2), ts_value.timeseries.info.labels.count());
    try testing.expectEqualStrings("counter", ts_value.timeseries.info.labels.get("type").?);
    try testing.expectEqualStrings("room1", ts_value.timeseries.info.labels.get("location").?);
}

test "TS.INCRBY command - with ENCODING option" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.INCRBY counter 1000 5 ENCODING COMPRESSED
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "COMPRESSED" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify encoding was set
    const ts_value = storage.get("counter").?;
    try testing.expect(ts_value.timeseries.info.encoding == .compressed);
}

test "TS.INCRBY command - with IGNORE option (stub)" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.INCRBY counter 1000 5 IGNORE
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "IGNORE" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should still work (IGNORE option parses but doesn't affect behavior)
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(f64, 5.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.INCRBY command - NaN validation error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Try to increment by NaN
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "nan" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, result, "ERR") or std.mem.startsWith(u8, result, "-"));
}

test "TS.INCRBY command - WRONGTYPE error" {
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

    // Try to TS.INCRBY on string key
    const incrby_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try handleCommand(allocator, &storage, &incrby_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.INCRBY command - duplicate BLOCK policy error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "BLOCK" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Add initial sample
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10" },
    };
    const add_result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(add_result);

    // Try to increment at same timestamp (should fail with BLOCK policy)
    const incrby_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try handleCommand(allocator, &storage, &incrby_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return DUPLICATE_POLICY error
    try testing.expect(std.mem.startsWith(u8, result, "ERR DUPLICATE_POLICY is BLOCK"));
}

test "TS.INCRBY command - wrong number of arguments" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Too few arguments
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.INCRBY command - invalid timestamp" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "not_a_timestamp" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid timestamp"));
}

test "TS.INCRBY command - invalid delta value" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "not_a_number" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid value"));
}

test "TS.INCRBY command - multiple increments (counter pattern)" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // First increment
    const cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "page_views" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "1" },
    };
    const result1 = try handleCommand(allocator, &storage, &cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Second increment same timestamp
    const cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "page_views" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "1" },
    };
    const result2 = try handleCommand(allocator, &storage, &cmd2, 0, null, null, null);
    defer allocator.free(result2);

    // Third increment same timestamp
    const cmd3 = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "page_views" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "1" },
    };
    const result3 = try handleCommand(allocator, &storage, &cmd3, 0, null, null, null);
    defer allocator.free(result3);

    // Verify total is 3
    const ts_value = storage.get("page_views").?;
    try testing.expectEqual(@as(usize, 1), ts_value.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, 3.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.DECRBY command - basic auto-create" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.DECRBY counter 1000 3
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return bulk string with new value
    try testing.expect(std.mem.startsWith(u8, result, "$"));

    // Verify key exists and is timeseries
    const value = storage.get("counter");
    try testing.expect(value != null);
    try testing.expect(value.?.* == .timeseries);
    try testing.expectEqual(@as(usize, 1), value.?.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, -3.0), value.?.timeseries.samples.items[0].value);
}

test "TS.DECRBY command - decrement existing sample" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create and add initial sample
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "counter" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10" },
    };
    const add_result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(add_result);

    // Decrement at same timestamp
    const decrby_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try handleCommand(allocator, &storage, &decrby_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify value was decremented
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(f64, 7.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.DECRBY command - with creation options" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.DECRBY counter 1000 5 RETENTION 3600000 ENCODING UNCOMPRESSED
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "3600000" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "UNCOMPRESSED" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify options were set
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(i64, 3600000), ts_value.timeseries.info.retention_ms);
    try testing.expect(ts_value.timeseries.info.encoding == .uncompressed);
}

test "TS.DECRBY command - wildcard timestamp" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.DECRBY counter * 3
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return a valid timestamp (not *)
    try testing.expect(std.mem.startsWith(u8, result, "$"));

    // Verify sample was added with correct value
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(usize, 1), ts_value.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, -3.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.DECRBY command - WRONGTYPE error" {
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

    // Try to TS.DECRBY on string key
    const decrby_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try handleCommand(allocator, &storage, &decrby_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.DECRBY command - wrong number of arguments" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Too few arguments
    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.DECRBY command - invalid timestamp" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "invalid" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid timestamp"));
}

test "TS.DECRBY command - invalid delta value" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "invalid" },
    };

    const result = try handleCommand(allocator, &storage, &cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid value"));
}

test "TS.DECRBY command - multiple decrements (counter pattern)" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // First decrement
    const cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "balance" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "10" },
    };
    const result1 = try handleCommand(allocator, &storage, &cmd1, 0, null, null, null);
    defer allocator.free(result1);

    // Second decrement same timestamp
    const cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "balance" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
    };
    const result2 = try handleCommand(allocator, &storage, &cmd2, 0, null, null, null);
    defer allocator.free(result2);

    // Third decrement same timestamp
    const cmd3 = [_]RespValue{
        RespValue{ .bulk_string = "TS.DECRBY" },
        RespValue{ .bulk_string = "balance" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "2" },
    };
    const result3 = try handleCommand(allocator, &storage, &cmd3, 0, null, null, null);
    defer allocator.free(result3);

    // Verify total is -(10+5+2)
    const ts_value = storage.get("balance").?;
    try testing.expectEqual(@as(usize, 1), ts_value.timeseries.samples.items.len);
    try testing.expectEqual(@as(f64, -17.0), ts_value.timeseries.samples.items[0].value);
}

test "TS.INCRBY command - respects existing configuration" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with specific configuration
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "7200000" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "SUM" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Increment with different options (should be ignored)
    const incrby_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "3600000" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "LAST" },
    };

    const result = try handleCommand(allocator, &storage, &incrby_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Verify original configuration is preserved
    const ts_value = storage.get("counter").?;
    try testing.expectEqual(@as(i64, 7200000), ts_value.timeseries.info.retention_ms);
    try testing.expect(ts_value.timeseries.info.duplicate_policy == .sum);
}
