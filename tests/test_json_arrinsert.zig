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
// 1. BASIC INSERTION TESTS
// ============================================================================

test "JSON.ARRINSERT - insert at beginning (index 0)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins0\r\n$1\r\n$\r\n$9\r\n[2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert 1 at index 0
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins0\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert at middle index" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins1\r\n$1\r\n$\r\n$9\r\n[1,2,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert 3 at index 2 (before 4)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins1\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert at end (index == length)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins2\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert 4 at index 3 (append)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins2\r\n$1\r\n$\r\n$1\r\n3\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert with negative index (-1)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins3\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index -1 (before last element)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins3\r\n$1\r\n$\r\n$2\r\n-1\r\n$3\r\n2.5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert with negative index (-2)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins4\r\n$1\r\n$\r\n$13\r\n[1,2,3,4,5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index -2 (before second-to-last element)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins4\r\n$1\r\n$\r\n$2\r\n-2\r\n$3\r\n4.5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [6]
    try testing.expectEqualStrings("*1\r\n:6\r\n", response);
}

// ============================================================================
// 2. MULTIPLE VALUES INSERTION TESTS
// ============================================================================

test "JSON.ARRINSERT - insert multiple values" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins5\r\n$1\r\n$\r\n$5\r\n[1,5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert 2, 3, 4 at index 1
    const cmd = "*7\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins5\r\n$1\r\n$\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [5]
    try testing.expectEqualStrings("*1\r\n:5\r\n", response);
}

test "JSON.ARRINSERT - insert many values at start" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins6\r\n$1\r\n$\r\n$3\r\n[5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert 1, 2, 3, 4 at index 0
    const cmd = "*8\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins6\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [5]
    try testing.expectEqualStrings("*1\r\n:5\r\n", response);
}

// ============================================================================
// 3. MULTIPLE PATHS (WILDCARD) TESTS
// ============================================================================

test "JSON.ARRINSERT - wildcard path matches multiple arrays" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a structure with multiple arrays
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins7\r\n$1\r\n$\r\n$39\r\n{\"arr1\":[1,3],\"arr2\":[4,6]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index 1 in all arrays using wildcard
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins7\r\n$5\r\n$.*[*]\r\n$1\r\n1\r\n$1\r\n2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array of new lengths: [3, 3]
    try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
}

// ============================================================================
// 4. TYPE HANDLING TESTS
// ============================================================================

test "JSON.ARRINSERT - non-array path returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object (not array)
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins8\r\n$1\r\n$\r\n$12\r\n{\"x\":1,\"y\":2}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to insert into object (not array)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins8\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array containing single null
    try testing.expectEqualStrings("*1\r\n$-1\r\n", response);
}

test "JSON.ARRINSERT - insert string values" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array of strings
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nins9\r\n$1\r\n$\r\n$17\r\n[\"a\",\"c\"]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert string "b" at index 1
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins9\r\n$1\r\n$\r\n$1\r\n1\r\n$3\r\n\"b\"\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [3]
    try testing.expectEqualStrings("*1\r\n:3\r\n", response);
}

test "JSON.ARRINSERT - insert objects into array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array of objects
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins10\r\n$1\r\n$\r\n$31\r\n[{\"id\":1},{\"id\":3}]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert object at index 1
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins10\r\n$1\r\n$\r\n$1\r\n1\r\n$10\r\n{\"id\":2}\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [3]
    try testing.expectEqualStrings("*1\r\n:3\r\n", response);
}

// ============================================================================
// 5. EDGE CASES
// ============================================================================

test "JSON.ARRINSERT - insert into empty array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an empty array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins11\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert value at index 0
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins11\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [1]
    try testing.expectEqualStrings("*1\r\n:1\r\n", response);
}

test "JSON.ARRINSERT - index equals array length (append)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins12\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index 3 (same as length, should append)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins12\r\n$1\r\n$\r\n$1\r\n3\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert null value" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins13\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert null at index 1
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins13\r\n$1\r\n$\r\n$1\r\n1\r\n$4\r\nnull\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert boolean values" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins14\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert true and false at index 0
    const cmd = "*6\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins14\r\n$1\r\n$\r\n$1\r\n0\r\n$4\r\ntrue\r\n$5\r\nfalse\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [5]
    try testing.expectEqualStrings("*1\r\n:5\r\n", response);
}

// ============================================================================
// 6. OUT OF BOUNDS TESTS
// ============================================================================

test "JSON.ARRINSERT - out of bounds positive index" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins15\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index 10 (out of bounds)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins15\r\n$1\r\n$\r\n$2\r\n10\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error (out of bounds)
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

test "JSON.ARRINSERT - out of bounds negative index" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins16\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index -10 (out of bounds)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins16\r\n$1\r\n$\r\n$3\r\n-10\r\n$1\r\n0\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error (out of bounds)
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

// ============================================================================
// 7. ERROR CONDITIONS
// ============================================================================

test "JSON.ARRINSERT - wrong arity too few args" {
    const stream = try connectToServer();
    defer stream.close();

    // Command with too few arguments
    const cmd = "*3\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins0\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return ERR
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRINSERT - wrong arity no value to insert" {
    const stream = try connectToServer();
    defer stream.close();

    // Command with key and path but no values
    const cmd = "*4\r\n$13\r\nJSON.ARRINSERT\r\n$4\r\nins0\r\n$1\r\n$\r\n$1\r\n0\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return ERR
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRINSERT - nonexistent key" {
    const stream = try connectToServer();
    defer stream.close();

    // Try to insert into non-existent key
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

test "JSON.ARRINSERT - wrongtype error for non-JSON key" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a string key
    const set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\nins17\r\n$5\r\nvalue\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to insert into non-JSON key
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins17\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return WRONGTYPE error
    try testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "JSON.ARRINSERT - invalid path syntax" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a valid JSON document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins18\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use invalid path syntax
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins18\r\n$10\r\n$.[invalid]\r\n$1\r\n0\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

test "JSON.ARRINSERT - invalid index type" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a valid JSON array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins19\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use non-numeric index
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins19\r\n$1\r\n$\r\n$3\r\nabc\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

test "JSON.ARRINSERT - invalid JSON value" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a valid JSON array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins20\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to insert invalid JSON
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins20\r\n$1\r\n$\r\n$1\r\n0\r\n$8\r\ninvalid\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

// ============================================================================
// 8. COMPLEX SCENARIOS
// ============================================================================

test "JSON.ARRINSERT - nested array path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a structure with nested array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins21\r\n$1\r\n$\r\n$37\r\n{\"user\":{\"tags\":[\"a\",\"c\"]}}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert in nested array
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins21\r\n$12\r\n$.user.tags\r\n$1\r\n1\r\n$3\r\n\"b\"\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [3]
    try testing.expectEqualStrings("*1\r\n:3\r\n", response);
}

test "JSON.ARRINSERT - insert mixed value types" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with mixed types
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins22\r\n$1\r\n$\r\n$19\r\n[1,\"str\",true]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert multiple values of different types
    const cmd = "*7\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins22\r\n$1\r\n$\r\n$1\r\n1\r\n$1\r\n2\r\n$4\r\nnull\r\n$5\r\nfalse\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [6]
    try testing.expectEqualStrings("*1\r\n:6\r\n", response);
}

test "JSON.ARRINSERT - insert array into array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins23\r\n$1\r\n$\r\n$9\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert an array as an element
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins23\r\n$1\r\n$\r\n$1\r\n1\r\n$7\r\n[4,5]\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [4]
    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "JSON.ARRINSERT - insert with negative index matching exact position" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array [1,2,3,4,5]
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nins24\r\n$1\r\n$\r\n$13\r\n[1,2,3,4,5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Insert at index -3 (should be before index 2, which is before 3)
    const cmd = "*5\r\n$13\r\nJSON.ARRINSERT\r\n$5\r\nins24\r\n$1\r\n$\r\n$2\r\n-3\r\n$3\r\n2.5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with new length: [6]
    try testing.expectEqualStrings("*1\r\n:6\r\n", response);
}
