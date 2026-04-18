const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const Config = @import("../src/storage/memory.zig").Config;
const parseCommand = @import("../src/protocol/parser.zig").parseCommand;
const handleCommand = @import("../src/commands/strings.zig").handleCommand;

test "TS.DEL basic range deletion" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with samples
    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);
    try std.testing.expectEqualStrings("+OK\r\n", create_resp);

    // Add samples
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "1000" },
        .{ .bulk_string = "10.0" },
    }, 0, null, null, null);
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "2000" },
        .{ .bulk_string = "20.0" },
    }, 0, null, null, null);
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "3000" },
        .{ .bulk_string = "30.0" },
    }, 0, null, null, null);
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "4000" },
        .{ .bulk_string = "40.0" },
    }, 0, null, null, null);

    // Delete range [1500, 3500]
    const del_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.DEL" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "1500" },
        .{ .bulk_string = "3500" },
    }, 0, null, null, null);
    defer allocator.free(del_resp);
    try std.testing.expectEqualStrings(":2\r\n", del_resp);

    // Verify 2 samples remain
    const value = storage.get("sensor:temp").?;
    try std.testing.expectEqual(@as(usize, 2), value.timeseries.samples.items.len);
}

test "TS.DEL all samples" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);

    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "1000" },
        .{ .bulk_string = "10.0" },
    }, 0, null, null, null);
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "2000" },
        .{ .bulk_string = "20.0" },
    }, 0, null, null, null);

    // Delete all
    const del_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.DEL" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "5000" },
    }, 0, null, null, null);
    defer allocator.free(del_resp);
    try std.testing.expectEqualStrings(":2\r\n", del_resp);

    const value = storage.get("sensor:temp").?;
    try std.testing.expectEqual(@as(usize, 0), value.timeseries.samples.items.len);
}

test "TS.DEL nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const del_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.DEL" },
        .{ .bulk_string = "nonexistent" },
        .{ .bulk_string = "1000" },
        .{ .bulk_string = "2000" },
    }, 0, null, null, null);
    defer allocator.free(del_resp);
    try std.testing.expect(std.mem.startsWith(u8, del_resp, "ERR key does not exist"));
}

test "TS.DEL WRONGTYPE" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Set a string key
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "value" },
    }, 0, null, null, null);

    const del_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.DEL" },
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "1000" },
        .{ .bulk_string = "2000" },
    }, 0, null, null, null);
    defer allocator.free(del_resp);
    try std.testing.expect(std.mem.startsWith(u8, del_resp, "-WRONGTYPE"));
}

test "TS.DEL invalid range" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);

    // from > to
    const del_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.DEL" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "5000" },
        .{ .bulk_string = "3000" },
    }, 0, null, null, null);
    defer allocator.free(del_resp);
    try std.testing.expect(std.mem.startsWith(u8, del_resp, "ERR fromTimestamp must be"));
}

test "TS.DEL arity error" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const del_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.DEL" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "1000" },
    }, 0, null, null, null);
    defer allocator.free(del_resp);
    try std.testing.expect(std.mem.startsWith(u8, del_resp, "ERR wrong number"));
}

test "TS.GET basic" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);

    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "1000" },
        .{ .bulk_string = "10.5" },
    }, 0, null, null, null);
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "2000" },
        .{ .bulk_string = "20.5" },
    }, 0, null, null, null);

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expectEqualStrings("*2\r\n:2000\r\n+20.5\r\n", get_resp);
}

test "TS.GET with LATEST flag" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);

    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.ADD" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "1000" },
        .{ .bulk_string = "10.0" },
    }, 0, null, null, null);

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "LATEST" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expectEqualStrings("*2\r\n:1000\r\n+10\r\n", get_resp);
}

test "TS.GET empty series" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expectEqualStrings("$-1\r\n", get_resp);
}

test "TS.GET nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "nonexistent" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.startsWith(u8, get_resp, "ERR key does not exist"));
}

test "TS.GET WRONGTYPE" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    // Set a string key
    _ = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "value" },
    }, 0, null, null, null);

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "mystring" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.startsWith(u8, get_resp, "-WRONGTYPE"));
}

test "TS.GET arity error too few args" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.startsWith(u8, get_resp, "ERR wrong number"));
}

test "TS.GET arity error too many args" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "LATEST" },
        .{ .bulk_string = "extra" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.startsWith(u8, get_resp, "ERR wrong number"));
}

test "TS.GET invalid option" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config);
    defer storage.deinit();

    const create_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.CREATE" },
        .{ .bulk_string = "sensor:temp" },
    }, 0, null, null, null);
    defer allocator.free(create_resp);

    const get_resp = try handleCommand(allocator, &storage, &[_]@import("../src/protocol/parser.zig").RespValue{
        .{ .bulk_string = "TS.GET" },
        .{ .bulk_string = "sensor:temp" },
        .{ .bulk_string = "INVALID" },
    }, 0, null, null, null);
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.startsWith(u8, get_resp, "ERR unknown option"));
}
