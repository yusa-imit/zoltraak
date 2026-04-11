const std = @import("std");
const Server = @import("../src/server.zig").Server;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const parseResp = @import("../src/protocol/parser.zig").parseResp;

test "JSON.ARRTRIM - basic trim with positive indices" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16379) catch unreachable;
    defer server.deinit();

    // SET key with array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$21\r\n[1,2,3,4,5,6,7,8,9,10]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ 2 5 (keep indices 2-5, values 3,4,5,6)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n5\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    // Should return new length: 4
    try std.testing.expectEqual(@as(i64, 4), trim_result.integer);

    // Verify array contents
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$3\r\nkey\r\n$1\r\n$\r\n";
    var get_result = try server.handleCommand(get_cmd, allocator);
    defer get_result.deinit(allocator);

    try std.testing.expect(std.mem.indexOf(u8, get_result.bulk_string, "[3,4,5,6]") != null);
}

test "JSON.ARRTRIM - negative indices" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16380) catch unreachable;
    defer server.deinit();

    // SET key $ [1,2,3,4,5]
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$11\r\n[1,2,3,4,5]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ 1 -2 (keep indices 1 to len-2 = 3, values 2,3,4)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n1\r\n$2\r\n-2\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 3), trim_result.integer);

    // Verify array
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$3\r\nkey\r\n$1\r\n$\r\n";
    var get_result = try server.handleCommand(get_cmd, allocator);
    defer get_result.deinit(allocator);

    try std.testing.expect(std.mem.indexOf(u8, get_result.bulk_string, "[2,3,4]") != null);
}

test "JSON.ARRTRIM - reverse range creates empty array" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16381) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$11\r\n[1,2,3,4,5]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ 4 2 (start > stop -> empty)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n4\r\n$1\r\n2\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), trim_result.integer);

    // Verify empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$3\r\nkey\r\n$1\r\n$\r\n";
    var get_result = try server.handleCommand(get_cmd, allocator);
    defer get_result.deinit(allocator);

    try std.testing.expect(std.mem.indexOf(u8, get_result.bulk_string, "[]") != null);
}

test "JSON.ARRTRIM - out of bounds start creates empty array" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16382) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ 10 20 (start >= len -> empty)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$2\r\n10\r\n$2\r\n20\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), trim_result.integer);
}

test "JSON.ARRTRIM - out of bounds stop clamps to end" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16383) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$11\r\n[1,2,3,4,5]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ 2 999 (stop clamped to 4)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n2\r\n$3\r\n999\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 3), trim_result.integer);

    // Verify [3,4,5]
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$3\r\nkey\r\n$1\r\n$\r\n";
    var get_result = try server.handleCommand(get_cmd, allocator);
    defer get_result.deinit(allocator);

    try std.testing.expect(std.mem.indexOf(u8, get_result.bulk_string, "[3,4,5]") != null);
}

test "JSON.ARRTRIM - single element array" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16384) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$3\r\n[1]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ 0 0 (keep only first)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n0\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), trim_result.integer);
}

test "JSON.ARRTRIM - wildcard paths with multiple arrays" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16385) catch unreachable;
    defer server.deinit();

    // SET key $ {"a":[1,2,3,4],"b":[5,6,7,8,9]}
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$31\r\n{\"a\":[1,2,3,4],\"b\":[5,6,7,8,9]}\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $.* 1 2 (both arrays trimmed to indices 1-2)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$3\r\n$.*\r\n$1\r\n1\r\n$1\r\n2\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    // Should return array of two integers: [2, 2]
    try std.testing.expectEqual(RespValue.array, @as(@TypeOf(trim_result), trim_result));
    try std.testing.expectEqual(@as(usize, 2), trim_result.array.len);
    try std.testing.expectEqual(@as(i64, 2), trim_result.array[0].integer);
    try std.testing.expectEqual(@as(i64, 2), trim_result.array[1].integer);
}

test "JSON.ARRTRIM - non-array returns null" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16386) catch unreachable;
    defer server.deinit();

    // SET key $ {"arr":[1,2,3],"num":42}
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$25\r\n{\"arr\":[1,2,3],\"num\":42}\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $.num 0 1 (not an array)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$5\r\n$.num\r\n$1\r\n0\r\n$1\r\n1\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    // Should return null
    try std.testing.expectEqual(RespValue.null_bulk_string, @as(@TypeOf(trim_result), trim_result));
}

test "JSON.ARRTRIM - key does not exist error" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16387) catch unreachable;
    defer server.deinit();

    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$9\r\nnoexist\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expectEqualStrings("ERR key does not exist", trim_result.error_string);
}

test "JSON.ARRTRIM - wrong type error" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16388) catch unreachable;
    defer server.deinit();

    // SET key as string
    const set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expect(std.mem.startsWith(u8, trim_result.error_string, "WRONGTYPE"));
}

test "JSON.ARRTRIM - wrong number of arguments" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16389) catch unreachable;
    defer server.deinit();

    // Too few args
    const trim_cmd = "*2\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expect(std.mem.indexOf(u8, trim_result.error_string, "wrong number of arguments") != null);
}

test "JSON.ARRTRIM - invalid index format" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16390) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // Invalid start index
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$3\r\nabc\r\n$1\r\n1\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    try std.testing.expect(std.mem.indexOf(u8, trim_result.error_string, "must be") != null or std.mem.indexOf(u8, trim_result.error_string, "integer") != null);
}

test "JSON.ARRTRIM - both indices negative and clamped" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16391) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM key $ -100 -50 (both negative out of bounds, clamp to 0)
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$4\r\n-100\r\n$3\r\n-50\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    // After clamping: start=0, stop=0 -> keep first element only
    try std.testing.expectEqual(@as(i64, 1), trim_result.integer);
}

test "JSON.ARRTRIM - empty array returns 0" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16392) catch unreachable;
    defer server.deinit();

    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    var set_result = try server.handleCommand(set_cmd, allocator);
    defer set_result.deinit(allocator);

    // ARRTRIM on empty array
    const trim_cmd = "*5\r\n$13\r\nJSON.ARRTRIM\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    var trim_result = try server.handleCommand(trim_cmd, allocator);
    defer trim_result.deinit(allocator);

    // Empty array: start >= len -> 0
    try std.testing.expectEqual(@as(i64, 0), trim_result.integer);
}
