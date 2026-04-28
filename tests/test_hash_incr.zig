const std = @import("std");
const testing = std.testing;
const Server = @import("../src/server.zig").Server;
const RespValue = @import("../src/protocol/parser.zig").RespValue;

test "HINCRBY on nonexistent key auto-creates" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "5" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .integer);
    try testing.expectEqual(@as(i64, 5), result.integer);
}

test "HINCRBY on nonexistent field auto-creates" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Create hash with one field
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "10" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Increment nonexistent field
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "3" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .integer);
    try testing.expectEqual(@as(i64, 3), result.integer);
}

test "HINCRBY with positive increment" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set initial value
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "100" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Increment
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "50" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .integer);
    try testing.expectEqual(@as(i64, 150), result.integer);
}

test "HINCRBY with negative increment" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set initial value
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "100" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Decrement
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "-30" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .integer);
    try testing.expectEqual(@as(i64, 70), result.integer);
}

test "HINCRBY error on WRONGTYPE" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Create a string key
    const set_cmd = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "value" },
    };
    const set_result = try server.storage.handleCommand(testing.allocator, &set_cmd, null);
    defer set_result.deinit(testing.allocator);

    // Try HINCRBY on string
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "1" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "WRONGTYPE") != null);
}

test "HINCRBY error on non-integer field value" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set non-integer value
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "notanumber" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Try HINCRBY
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "1" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "not an integer") != null or
        std.mem.indexOf(u8, result.error_string, "not a valid integer") != null);
}

test "HINCRBYFLOAT on nonexistent key auto-creates" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "2.5" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .bulk_string);
    try testing.expectEqualStrings("2.5", result.bulk_string);
}

test "HINCRBYFLOAT on nonexistent field auto-creates" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Create hash with one field
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "10" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Increment nonexistent field
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "1.5" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .bulk_string);
    try testing.expectEqualStrings("1.5", result.bulk_string);
}

test "HINCRBYFLOAT with positive increment" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set initial value
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "price" },
        .{ .bulk_string = "10.5" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Increment
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "price" },
        .{ .bulk_string = "2.3" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .bulk_string);
    // Parse and check numeric value (allow for floating point imprecision)
    const value = try std.fmt.parseFloat(f64, result.bulk_string);
    try testing.expect(@abs(value - 12.8) < 0.0001);
}

test "HINCRBYFLOAT with negative increment" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set initial value
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "price" },
        .{ .bulk_string = "10.5" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Decrement
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "price" },
        .{ .bulk_string = "-3.2" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .bulk_string);
    const value = try std.fmt.parseFloat(f64, result.bulk_string);
    try testing.expect(@abs(value - 7.3) < 0.0001);
}

test "HINCRBYFLOAT precision preservation" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set initial value with trailing zeros
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "price" },
        .{ .bulk_string = "10.50" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Increment
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "price" },
        .{ .bulk_string = "0.1" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .bulk_string);
    const value = try std.fmt.parseFloat(f64, result.bulk_string);
    try testing.expect(@abs(value - 10.6) < 0.0001);
}

test "HINCRBYFLOAT error on WRONGTYPE" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Create a string key
    const set_cmd = [_]RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "value" },
    };
    const set_result = try server.storage.handleCommand(testing.allocator, &set_cmd, null);
    defer set_result.deinit(testing.allocator);

    // Try HINCRBYFLOAT on string
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "1.5" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "WRONGTYPE") != null);
}

test "HINCRBYFLOAT error on non-numeric field value" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    // Set non-numeric value
    const hset_cmd = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "notanumber" },
    };
    const hset_result = try server.storage.handleCommand(testing.allocator, &hset_cmd, null);
    defer hset_result.deinit(testing.allocator);

    // Try HINCRBYFLOAT
    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "1.5" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "not a valid") != null or
        std.mem.indexOf(u8, result.error_string, "not a float") != null);
}

test "HINCRBY arity error" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "mykey" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number of arguments") != null);
}

test "HINCRBYFLOAT arity error" {
    var server = try Server.init(testing.allocator, .{ .port = 0 });
    defer server.deinit();

    const cmd = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "mykey" },
    };
    const result = try server.storage.handleCommand(testing.allocator, &cmd, null);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number of arguments") != null);
}
