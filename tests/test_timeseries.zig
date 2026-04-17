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
