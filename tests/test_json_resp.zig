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
// 1. BASIC RESP MAPPING TESTS
// ============================================================================

test "JSON.RESP - null value returns bulk string null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set null value
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:01\r\n$1\r\n$\r\n$4\r\nnull\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:01\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string null
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.RESP - boolean true returns simple string" {
    const stream = try connectToServer();
    defer stream.close();

    // Set boolean true
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:02\r\n$1\r\n$\r\n$4\r\ntrue\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:02\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return simple string "true"
    try testing.expectEqualStrings("+true\r\n", response);
}

test "JSON.RESP - boolean false returns simple string" {
    const stream = try connectToServer();
    defer stream.close();

    // Set boolean false
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:03\r\n$1\r\n$\r\n$5\r\nfalse\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:03\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return simple string "false"
    try testing.expectEqualStrings("+false\r\n", response);
}

test "JSON.RESP - integer number returns integer reply" {
    const stream = try connectToServer();
    defer stream.close();

    // Set integer
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:04\r\n$1\r\n$\r\n$2\r\n42\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:04\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return integer 42
    try testing.expectEqualStrings(":42\r\n", response);
}

test "JSON.RESP - float number returns bulk string" {
    const stream = try connectToServer();
    defer stream.close();

    // Set float
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:05\r\n$1\r\n$\r\n$4\r\n3.14\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:05\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "3.14"
    try testing.expect(std.mem.startsWith(u8, response, "$"));
    try testing.expect(std.mem.indexOf(u8, response, "3.14") != null);
}

test "JSON.RESP - string returns bulk string" {
    const stream = try connectToServer();
    defer stream.close();

    // Set string
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:06\r\n$1\r\n$\r\n$7\r\n\"hello\"\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:06\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "hello"
    try testing.expectEqualStrings("$5\r\nhello\r\n", response);
}

// ============================================================================
// 2. ARRAY RESP MAPPING TESTS
// ============================================================================

test "JSON.RESP - array with marker" {
    const stream = try connectToServer();
    defer stream.close();

    // Set simple array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:07\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:07\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with "[" marker followed by elements
    // *4\r\n+[\r\n:1\r\n:2\r\n:3\r\n
    try testing.expect(std.mem.startsWith(u8, response, "*4\r\n+[\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, ":1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, ":2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, ":3\r\n") != null);
}

test "JSON.RESP - empty array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set empty array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:08\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:08\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with just "[" marker
    // *1\r\n+[\r\n
    try testing.expectEqualStrings("*1\r\n+[\r\n", response);
}

test "JSON.RESP - array with mixed types" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with different types
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$7\r\nresp:09\r\n$1\r\n$\r\n$20\r\n[1,\"text\",true,null]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$7\r\nresp:09\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with marker and mixed types
    // *5\r\n+[\r\n:1\r\n$4\r\ntext\r\n+true\r\n$-1\r\n
    try testing.expect(std.mem.startsWith(u8, response, "*5\r\n+[\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, ":1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$4\r\ntext\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "+true\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$-1\r\n") != null);
}

// ============================================================================
// 3. OBJECT RESP MAPPING TESTS
// ============================================================================

test "JSON.RESP - object with marker" {
    const stream = try connectToServer();
    defer stream.close();

    // Set simple object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:10\r\n$1\r\n$\r\n$11\r\n{\"a\":1,\"b\":2}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:10\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with "{" marker followed by key-value pairs
    // *5\r\n+{\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n
    try testing.expect(std.mem.startsWith(u8, response, "*5\r\n+{\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\na\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\nb\r\n") != null);
}

test "JSON.RESP - empty object" {
    const stream = try connectToServer();
    defer stream.close();

    // Set empty object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:11\r\n$1\r\n$\r\n$2\r\n{}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:11\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with just "{" marker
    // *1\r\n+{\r\n
    try testing.expectEqualStrings("*1\r\n+{\r\n", response);
}

// ============================================================================
// 4. NESTED STRUCTURES TESTS
// ============================================================================

test "JSON.RESP - nested object in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with nested object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:12\r\n$1\r\n$\r\n$13\r\n[{\"x\":1},{\"y\":2}]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:12\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return nested RESP structure
    // Outer array with "[" marker, inner objects with "{" markers
    try testing.expect(std.mem.startsWith(u8, response, "*3\r\n+[\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "+{\r\n") != null);
}

test "JSON.RESP - nested array in object" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object with nested array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:13\r\n$1\r\n$\r\n$15\r\n{\"arr\":[1,2,3]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:13\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return object with "{" marker, containing array with "[" marker
    try testing.expect(std.mem.startsWith(u8, response, "*3\r\n+{\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "$3\r\narr\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "+[\r\n") != null);
}

// ============================================================================
// 5. PATH TESTS
// ============================================================================

test "JSON.RESP - with explicit path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object with nested value
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:14\r\n$1\r\n$\r\n$18\r\n{\"nested\":{\"val\":42}}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form of nested value
    const cmd = "*3\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:14\r\n$12\r\n$.nested.val\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return integer 42
    try testing.expectEqualStrings(":42\r\n", response);
}

test "JSON.RESP - legacy path syntax" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:15\r\n$1\r\n$\r\n$13\r\n{\"field\":\"text\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get RESP form with legacy path
    const cmd = "*3\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:15\r\n$6\r\n.field\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "text"
    try testing.expectEqualStrings("$4\r\ntext\r\n", response);
}

// ============================================================================
// 6. ERROR TESTS
// ============================================================================

test "JSON.RESP - non-existent key returns error" {
    const stream = try connectToServer();
    defer stream.close();

    // Try to get RESP of non-existent key
    const cmd = "*2\r\n$9\r\nJSON.RESP\r\n$12\r\nresp:noexist\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

test "JSON.RESP - wrong arity error" {
    const stream = try connectToServer();
    defer stream.close();

    // Too few arguments
    const cmd = "*1\r\n$9\r\nJSON.RESP\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return arity error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "JSON.RESP - non-existent path returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\nresp:16\r\n$1\r\n$\r\n$10\r\n{\"a\":\"b\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Query non-existent path
    const cmd = "*3\r\n$9\r\nJSON.RESP\r\n$8\r\nresp:16\r\n$11\r\n$.noexist\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null (empty array for JSONPath with no matches)
    try testing.expectEqualStrings("*0\r\n", response);
}
