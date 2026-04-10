const std = @import("std");
const net = std.net;
const testing = std.testing;

// Helper to connect to server
fn connectToServer() !net.Stream {
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    return try net.tcpConnectToAddress(address);
}

// Helper to send command and read response
fn sendCommand(stream: net.Stream, command: []const u8) ![]u8 {
    _ = try stream.write(command);

    // Read response
    var buf: [16384]u8 = undefined;
    const n = try stream.read(&buf);

    const allocator = testing.allocator;
    return try allocator.dupe(u8, buf[0..n]);
}

// Helper to free response
fn freeResponse(response: []u8) void {
    testing.allocator.free(response);
}

// ============================================================================
// 1. BASIC FUNCTIONALITY TESTS
// ============================================================================

test "JSON.ARRLEN - basic array length at root" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\narrlen01\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get array length
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen01\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return integer 4
    try testing.expectEqualStrings(":4\r\n", response);
}

test "JSON.ARRLEN - empty array returns 0" {
    const stream = try connectToServer();
    defer stream.close();

    // Set empty array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen02\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get length
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen02\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.ARRLEN - nested array with path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object with nested array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen03\r\n$1\r\n$\r\n$25\r\n{\"items\":[10,20,30,40,50]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get nested array length
    const cmd = "*3\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen03\r\n$7\r\n$.items\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 5
    try testing.expectEqualStrings(":5\r\n", response);
}

test "JSON.ARRLEN - non-array type returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a string (not an array)
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen04\r\n$1\r\n$\r\n$7\r\n\"hello\"\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to get length (should return null for non-array)
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen04\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.ARRLEN - non-existent key returns error" {
    const stream = try connectToServer();
    defer stream.close();

    // Try to get length of non-existent key
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$10\r\nnonexistent\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRLEN - wildcard path returns array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object with multiple arrays
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen06\r\n$1\r\n$\r\n$37\r\n{\"a\":[1,2],\"b\":[3,4,5],\"c\":\"text\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get all field lengths with wildcard
    const cmd = "*3\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen06\r\n$3\r\n$.*\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array [2, 3, null] (c is not an array)
    // RESP array format: *3\r\n:2\r\n:3\r\n$-1\r\n
    try testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, ":2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, ":3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$-1\r\n") != null);
}

// ============================================================================
// 2. ERROR HANDLING TESTS
// ============================================================================

test "JSON.ARRLEN - too few arguments" {
    const stream = try connectToServer();
    defer stream.close();

    // Missing key argument
    const cmd = "*1\r\n$10\r\nJSON.ARRLEN\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRLEN - too many arguments" {
    const stream = try connectToServer();
    defer stream.close();

    // Too many arguments
    const cmd = "*4\r\n$10\r\nJSON.ARRLEN\r\n$3\r\nkey\r\n$1\r\n$\r\n$5\r\nextra\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRLEN - invalid path syntax" {
    const stream = try connectToServer();
    defer stream.close();

    // Set valid JSON
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen09\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use invalid path
    const cmd = "*3\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen09\r\n$7\r\ninvalid\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error for invalid path syntax
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRLEN - path not found returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set valid JSON
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen10\r\n$1\r\n$\r\n$17\r\n{\"items\":[1,2,3]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Query path that doesn't exist
    const cmd = "*3\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen10\r\n$11\r\n$.notfound\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null for non-existent path
    try testing.expectEqualStrings("$-1\r\n", response);
}

// ============================================================================
// 3. EDGE CASES
// ============================================================================

test "JSON.ARRLEN - nested arrays" {
    const stream = try connectToServer();
    defer stream.close();

    // Set nested arrays
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen11\r\n$1\r\n$\r\n$17\r\n[[1,2],[3,4,5]]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get outer array length (should be 2)
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen11\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":2\r\n", response);
}

test "JSON.ARRLEN - array with mixed types" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with various types
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen12\r\n$1\r\n$\r\n$24\r\n[1,\"two\",true,null,{}]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get length (all elements count regardless of type)
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen12\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":5\r\n", response);
}

test "JSON.ARRLEN - large array" {
    const stream = try connectToServer();
    defer stream.close();

    // Build a large array (100 elements)
    var buf: [2048]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const alloc = fba.allocator();

    var arr = try std.ArrayList(u8).initCapacity(alloc, 1024);
    try arr.appendSlice(alloc, "[");
    for (0..100) |i| {
        if (i > 0) try arr.appendSlice(alloc, ",");
        const num_str = try std.fmt.allocPrint(alloc, "{d}", .{i});
        try arr.appendSlice(alloc, num_str);
    }
    try arr.appendSlice(alloc, "]");

    const json_str = arr.items;
    const json_len = json_str.len;

    // Build RESP command
    var cmd_buf: [4096]u8 = undefined;
    const set_cmd = try std.fmt.bufPrint(&cmd_buf, "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrlen13\r\n$1\r\n$\r\n${d}\r\n{s}\r\n", .{ json_len, json_str });

    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get length
    const cmd = "*2\r\n$10\r\nJSON.ARRLEN\r\n$8\r\narrlen13\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":100\r\n", response);
}
