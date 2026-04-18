const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const Config = @import("../src/storage/memory.zig").Config;
const parser = @import("../src/protocol/parser.zig");
const handleCommand = @import("../src/commands/strings.zig").handleCommand;
const RespValue = parser.RespValue;

test "TS.CREATE basic" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.CREATE sensor:temp
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);

    // Verify key exists
    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expect(value.?.* == .timeseries);
}

test "TS.CREATE with RETENTION" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.CREATE sensor:temp RETENTION 86400000
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "86400000" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);

    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expectEqual(@as(i64, 86400000), value.?.timeseries.info.retention_ms);
}

test "TS.CREATE with ENCODING" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.CREATE sensor:temp ENCODING COMPRESSED
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "COMPRESSED" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
}

test "TS.CREATE with DUPLICATE_POLICY" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.CREATE sensor:temp DUPLICATE_POLICY SUM
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "SUM" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);

    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expect(value.?.timeseries.info.duplicate_policy == .sum);
}

test "TS.CREATE with LABELS" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.CREATE sensor:temp LABELS sensor temp location room1
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "sensor" },
        RespValue{ .bulk_string = "temp" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room1" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);

    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expectEqual(@as(usize, 2), value.?.timeseries.info.labels.count());
}

test "TS.CREATE full options" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.CREATE sensor:temp RETENTION 86400000 ENCODING COMPRESSED CHUNK_SIZE 8192 DUPLICATE_POLICY LAST LABELS sensor temp
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "86400000" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "COMPRESSED" },
        RespValue{ .bulk_string = "CHUNK_SIZE" },
        RespValue{ .bulk_string = "8192" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "LAST" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "sensor" },
        RespValue{ .bulk_string = "temp" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);

    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expectEqual(@as(i64, 86400000), value.?.timeseries.info.retention_ms);
    try testing.expectEqual(@as(u32, 8192), value.?.timeseries.info.chunk_size);
    try testing.expectEqual(@as(usize, 1), value.?.timeseries.info.labels.count());
}

test "TS.CREATE duplicate key error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create first time
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
    };

    const result1 = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result1);
    try testing.expectEqualStrings("+OK\r\n", result1);

    // Try to create again
    const result2 = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result2);
    try testing.expect(std.mem.startsWith(u8, result2, "ERR key already exists"));
}

test "TS.CREATE invalid RETENTION" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "-1" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "ERR"));
}

test "TS.CREATE invalid ENCODING" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "INVALID" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "ERR invalid ENCODING"));
}

test "TS.CREATE arity error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
    };

    const result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.INFO basic" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "3600000" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // TS.INFO sensor:temp
    const info_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INFO" },
        RespValue{ .bulk_string = "sensor:temp" },
    };

    const result = try handleCommand(allocator, &storage, &info_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Check that response starts with RESP array marker
    try testing.expect(std.mem.startsWith(u8, result, "*18\r\n"));
    // Check that it contains totalSamples
    try testing.expect(std.mem.indexOf(u8, result, "totalSamples") != null);
    // Check that it contains retentionTime
    try testing.expect(std.mem.indexOf(u8, result, "retentionTime") != null);
}

test "TS.INFO key not found" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const info_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INFO" },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try handleCommand(allocator, &storage, &info_cmd, 0, null, null, null);
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "ERR key does not exist"));
}

test "TS.INFO wrong type" {
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

    // Try TS.INFO on string key
    const info_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INFO" },
        RespValue{ .bulk_string = "mystring" },
    };

    const result = try handleCommand(allocator, &storage, &info_cmd, 0, null, null, null);
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.INFO arity error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const info_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.INFO" },
    };

    const result = try handleCommand(allocator, &storage, &info_cmd, 0, null, null, null);
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

// ============================================================================
// Tests for TS.ALTER
// ============================================================================

test "TS.ALTER basic retention" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:temp" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Alter retention
    const alter_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ALTER" },
        RespValue{ .bulk_string = "sensor:temp" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "86400000" },
    };
    const alter_result = try handleCommand(allocator, &storage, &alter_cmd, 0, null, null, null);
    defer allocator.free(alter_result);

    try testing.expectEqualStrings("+OK\r\n", alter_result);

    // Verify retention was changed
    const value = storage.get("sensor:temp");
    try testing.expect(value != null);
    try testing.expectEqual(@as(i64, 86400000), value.?.timeseries.info.retention_ms);
}

test "TS.ALTER chunk_size and duplicate_policy" {
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

    // Alter multiple fields
    const alter_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ALTER" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "CHUNK_SIZE" },
        RespValue{ .bulk_string = "8192" },
        RespValue{ .bulk_string = "DUPLICATE_POLICY" },
        RespValue{ .bulk_string = "SUM" },
    };
    const alter_result = try handleCommand(allocator, &storage, &alter_cmd, 0, null, null, null);
    defer allocator.free(alter_result);

    try testing.expectEqualStrings("+OK\r\n", alter_result);

    const value = storage.get("myts");
    try testing.expect(value != null);
    try testing.expectEqual(@as(u32, 8192), value.?.timeseries.info.chunk_size);
}

test "TS.ALTER labels replacement" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with labels
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "sensor" },
        RespValue{ .bulk_string = "temp" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Alter labels
    const alter_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ALTER" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "metric" },
        RespValue{ .bulk_string = "id" },
        RespValue{ .bulk_string = "123" },
    };
    const alter_result = try handleCommand(allocator, &storage, &alter_cmd, 0, null, null, null);
    defer allocator.free(alter_result);

    try testing.expectEqualStrings("+OK\r\n", alter_result);

    const value = storage.get("myts");
    try testing.expect(value != null);
    try testing.expectEqual(@as(usize, 2), value.?.timeseries.info.labels.count());
    try testing.expect(value.?.timeseries.info.labels.get("type") != null);
    try testing.expect(value.?.timeseries.info.labels.get("sensor") == null); // Old label should be gone
}

test "TS.ALTER nonexistent key error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const alter_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ALTER" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "1000" },
    };
    const result = try handleCommand(allocator, &storage, &alter_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR key does not exist"));
}

test "TS.ALTER WRONGTYPE error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Set a string key
    const set_cmd = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "value" },
    };
    const set_result = try handleCommand(allocator, &storage, &set_cmd, 0, null, null, null);
    defer allocator.free(set_result);

    // Try to alter
    const alter_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ALTER" },
        RespValue{ .bulk_string = "mystring" },
        RespValue{ .bulk_string = "RETENTION" },
        RespValue{ .bulk_string = "1000" },
    };
    const result = try handleCommand(allocator, &storage, &alter_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.ALTER ENCODING error" {
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

    // Try to alter encoding
    const alter_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ALTER" },
        RespValue{ .bulk_string = "myts" },
        RespValue{ .bulk_string = "ENCODING" },
        RespValue{ .bulk_string = "COMPRESSED" },
    };
    const result = try handleCommand(allocator, &storage, &alter_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR ENCODING cannot be altered"));
}

// ============================================================================
// Tests for TS.MGET
// ============================================================================

test "TS.MGET basic filter" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with labels
    const create_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:1" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "temperature" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room1" },
    };
    const create_result1 = try handleCommand(allocator, &storage, &create_cmd1, 0, null, null, null);
    defer allocator.free(create_result1);

    const create_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:2" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "temperature" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room2" },
    };
    const create_result2 = try handleCommand(allocator, &storage, &create_cmd2, 0, null, null, null);
    defer allocator.free(create_result2);

    // Add samples
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:1" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "25.5" },
    };
    const add_result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(add_result1);

    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:2" },
        RespValue{ .bulk_string = "2000" },
        RespValue{ .bulk_string = "26.0" },
    };
    const add_result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(add_result2);

    // TS.MGET FILTER type=temperature
    const mget_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MGET" },
        RespValue{ .bulk_string = "FILTER" },
        RespValue{ .bulk_string = "type=temperature" },
    };
    const result = try handleCommand(allocator, &storage, &mget_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return 2 results
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "TS.MGET with WITHLABELS" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with labels
    const create_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:1" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "temperature" },
    };
    const create_result = try handleCommand(allocator, &storage, &create_cmd, 0, null, null, null);
    defer allocator.free(create_result);

    // Add sample
    const add_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:1" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "25.5" },
    };
    const add_result = try handleCommand(allocator, &storage, &add_cmd, 0, null, null, null);
    defer allocator.free(add_result);

    // TS.MGET WITHLABELS FILTER type=temperature
    const mget_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MGET" },
        RespValue{ .bulk_string = "WITHLABELS" },
        RespValue{ .bulk_string = "FILTER" },
        RespValue{ .bulk_string = "type=temperature" },
    };
    const result = try handleCommand(allocator, &storage, &mget_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
}

test "TS.MGET with multiple filters (AND)" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:1" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "temperature" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room1" },
    };
    const create_result1 = try handleCommand(allocator, &storage, &create_cmd1, 0, null, null, null);
    defer allocator.free(create_result1);

    const create_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.CREATE" },
        RespValue{ .bulk_string = "sensor:2" },
        RespValue{ .bulk_string = "LABELS" },
        RespValue{ .bulk_string = "type" },
        RespValue{ .bulk_string = "humidity" },
        RespValue{ .bulk_string = "location" },
        RespValue{ .bulk_string = "room1" },
    };
    const create_result2 = try handleCommand(allocator, &storage, &create_cmd2, 0, null, null, null);
    defer allocator.free(create_result2);

    // Add samples
    const add_cmd1 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:1" },
        RespValue{ .bulk_string = "1000" },
        RespValue{ .bulk_string = "25.5" },
    };
    const add_result1 = try handleCommand(allocator, &storage, &add_cmd1, 0, null, null, null);
    defer allocator.free(add_result1);

    const add_cmd2 = [_]RespValue{
        RespValue{ .bulk_string = "TS.ADD" },
        RespValue{ .bulk_string = "sensor:2" },
        RespValue{ .bulk_string = "2000" },
        RespValue{ .bulk_string = "60.0" },
    };
    const add_result2 = try handleCommand(allocator, &storage, &add_cmd2, 0, null, null, null);
    defer allocator.free(add_result2);

    // TS.MGET FILTER type=temperature FILTER location=room1 (should match sensor:1 only)
    const mget_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MGET" },
        RespValue{ .bulk_string = "FILTER" },
        RespValue{ .bulk_string = "type=temperature" },
        RespValue{ .bulk_string = "FILTER" },
        RespValue{ .bulk_string = "location=room1" },
    };
    const result = try handleCommand(allocator, &storage, &mget_cmd, 0, null, null, null);
    defer allocator.free(result);

    // Should return 1 result
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
}

test "TS.MGET no positive filter error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.MGET with only negative filter (should fail)
    const mget_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MGET" },
        RespValue{ .bulk_string = "FILTER" },
        RespValue{ .bulk_string = "type!=" },
    };
    const result = try handleCommand(allocator, &storage, &mget_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR MGET requires at least one positive filter"));
}

test "TS.MGET no filter error" {
    const allocator = testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.MGET without FILTER (should fail)
    const mget_cmd = [_]RespValue{
        RespValue{ .bulk_string = "TS.MGET" },
    };
    const result = try handleCommand(allocator, &storage, &mget_cmd, 0, null, null, null);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}
