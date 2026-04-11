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

test "JSON.ARRPOP - pop from end (default index -1)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop01\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from end (should return 4)
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop01\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "4"
    try testing.expectEqualStrings("$1\r\n4\r\n", response);

    // Verify array is now [1,2,3] by getting it
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$8\r\narrpop01\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[1,2,3]") != null);
}

test "JSON.ARRPOP - pop from specific index 0" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop02\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from index 0 (should return 1)
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop02\r\n$1\r\n$\r\n$1\r\n0\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "1"
    try testing.expectEqualStrings("$1\r\n1\r\n", response);

    // Verify array is now [2,3,4]
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$8\r\narrpop02\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[2,3,4]") != null);
}

test "JSON.ARRPOP - pop with positive index 2" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop03\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from index 2 (should return 3)
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop03\r\n$1\r\n$\r\n$1\r\n2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "3"
    try testing.expectEqualStrings("$1\r\n3\r\n", response);

    // Verify array is now [1,2,4]
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$8\r\narrpop03\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[1,2,4]") != null);
}

test "JSON.ARRPOP - pop with negative index -2" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop04\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from index -2 (should return 3)
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop04\r\n$1\r\n$\r\n$2\r\n-2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "3"
    try testing.expectEqualStrings("$1\r\n3\r\n", response);

    // Verify array is now [1,2,4]
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$8\r\narrpop04\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[1,2,4]") != null);
}

test "JSON.ARRPOP - pop from nested path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object with nested array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop05\r\n$1\r\n$\r\n$29\r\n{\"a\":[10,20,30],\"b\":\"skip\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from nested array at index 1 (should return 20)
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop05\r\n$2\r\n$.a\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "20"
    try testing.expectEqualStrings("$2\r\n20\r\n", response);

    // Verify nested array is now [10,30]
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$8\r\narrpop05\r\n$2\r\n$.a\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[10,30]") != null);
}

test "JSON.ARRPOP - pop from empty array returns error" {
    const stream = try connectToServer();
    defer stream.close();

    // Set empty array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop06\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to pop from empty array
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop06\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error (cannot pop from empty array)
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - pop from non-array returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a string (not an array)
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop07\r\n$1\r\n$\r\n$7\r\n\"hello\"\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to pop from non-array
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop07\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.ARRPOP - pop from non-existent path returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set valid JSON
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop08\r\n$1\r\n$\r\n$17\r\n{\"items\":[1,2,3]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to pop from non-existent path
    const cmd = "*3\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop08\r\n$9\r\n$.missing\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.ARRPOP - wildcard path returns array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set object with multiple arrays
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop09\r\n$1\r\n$\r\n$29\r\n{\"a\":[1,2],\"b\":[3,4],\"c\":5}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from all fields with wildcard (should return [2, 4, null])
    const cmd = "*3\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop09\r\n$3\r\n$.*\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with 3 elements
    try testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
    // Check for the values (order may vary)
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\n2\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\n4\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$-1\r\n") != null);
}

test "JSON.ARRPOP - pop with string value" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with string values
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop10\r\n$1\r\n$\r\n$22\r\n[\"a\",\"b\",\"c\",\"d\"]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from end (should return "d")
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop10\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string "\"d\""
    try testing.expectEqualStrings("$3\r\n\"d\"\r\n", response);
}

test "JSON.ARRPOP - pop with nested object value" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with object values
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop11\r\n$1\r\n$\r\n$45\r\n[{\"x\":1,\"y\":2},{\"x\":3,\"y\":4},{\"x\":5}]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from end (should return {"x":5})
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop11\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return the object as JSON
    try testing.expect(std.mem.indexOf(u8, response, "{\"x\":5}") != null);
}

// ============================================================================
// 2. ERROR HANDLING TESTS
// ============================================================================

test "JSON.ARRPOP - too few arguments" {
    const stream = try connectToServer();
    defer stream.close();

    // Missing key argument
    const cmd = "*1\r\n$9\r\nJSON.ARRPOP\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - too many arguments" {
    const stream = try connectToServer();
    defer stream.close();

    // Too many arguments
    const cmd = "*5\r\n$9\r\nJSON.ARRPOP\r\n$3\r\nkey\r\n$1\r\n$\r\n$1\r\n0\r\n$5\r\nextra\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - invalid index not integer" {
    const stream = try connectToServer();
    defer stream.close();

    // Set valid array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop12\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use non-integer index
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop12\r\n$1\r\n$\r\n$3\r\nabc\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - out of range positive index" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with 3 elements
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop13\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to pop at index 999 (out of range)
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop13\r\n$1\r\n$\r\n$3\r\n999\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - out of range negative index" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with 3 elements
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop14\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to pop at index -999 (out of range)
    const cmd = "*4\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop14\r\n$1\r\n$\r\n$4\r\n-999\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - invalid path syntax" {
    const stream = try connectToServer();
    defer stream.close();

    // Set valid array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop15\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use invalid path syntax
    const cmd = "*3\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop15\r\n$7\r\ninvalid\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - non-existent key returns error" {
    const stream = try connectToServer();
    defer stream.close();

    // Try to pop from non-existent key
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$10\r\nnonexistent\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRPOP - wrong type (non-JSON)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a string value (not JSON)
    const cmd1 = "*3\r\n$3\r\nSET\r\n$8\r\narrpop16\r\n$5\r\nhello\r\n";
    const resp1 = try sendCommand(stream, cmd1);
    defer freeResponse(resp1);

    // Try to pop from it
    const cmd2 = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop16\r\n";
    const response = try sendCommand(stream, cmd2);
    defer freeResponse(response);

    // Should return WRONGTYPE error
    try testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

// ============================================================================
// 3. EDGE CASES
// ============================================================================

test "JSON.ARRPOP - single element array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set single element array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop17\r\n$1\r\n$\r\n$3\r\n[42]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop the only element
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop17\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return "42"
    try testing.expectEqualStrings("$2\r\n42\r\n", response);

    // Verify array is now empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$8\r\narrpop17\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[]") != null);
}

test "JSON.ARRPOP - double values" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with float values
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop18\r\n$1\r\n$\r\n$15\r\n[1.5,2.5,3.5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from end
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop18\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return "3.5"
    try testing.expectEqualStrings("$3\r\n3.5\r\n", response);
}

test "JSON.ARRPOP - null values in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with null values
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop19\r\n$1\r\n$\r\n$15\r\n[1,null,3,null]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from end (should return null)
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop19\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return JSON null
    try testing.expectEqualStrings("$4\r\nnull\r\n", response);
}

test "JSON.ARRPOP - boolean values in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array with booleans
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop20\r\n$1\r\n$\r\n$18\r\n[true,false,true]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Pop from end
    const cmd = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop20\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return "true"
    try testing.expectEqualStrings("$4\r\ntrue\r\n", response);
}

test "JSON.ARRPOP - multiple pops in sequence" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$8\r\narrpop21\r\n$1\r\n$\r\n$7\r\n[1,2,3]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // First pop
    const cmd1 = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop21\r\n";
    const resp1 = try sendCommand(stream, cmd1);
    defer freeResponse(resp1);
    try testing.expectEqualStrings("$1\r\n3\r\n", resp1);

    // Second pop
    const cmd2 = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop21\r\n";
    const resp2 = try sendCommand(stream, cmd2);
    defer freeResponse(resp2);
    try testing.expectEqualStrings("$1\r\n2\r\n", resp2);

    // Third pop
    const cmd3 = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop21\r\n";
    const resp3 = try sendCommand(stream, cmd3);
    defer freeResponse(resp3);
    try testing.expectEqualStrings("$1\r\n1\r\n", resp3);

    // Fourth pop should fail (empty array)
    const cmd4 = "*2\r\n$9\r\nJSON.ARRPOP\r\n$8\r\narrpop21\r\n";
    const resp4 = try sendCommand(stream, cmd4);
    defer freeResponse(resp4);
    try testing.expect(std.mem.startsWith(u8, resp4, "-ERR"));
}
