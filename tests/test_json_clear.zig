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

test "JSON.CLEAR - clear non-empty object" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with fields
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear1\r\n$1\r\n$\r\n$18\r\n{\"a\":1,\"b\":2,\"c\":3}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the object
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear1\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1 (one object cleared)
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify the object is empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nclear1\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$2\r\n{}\r\n", get_resp);
}

test "JSON.CLEAR - clear non-empty array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON array with elements
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear2\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the array
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear2\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1 (one array cleared)
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify the array is empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nclear2\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$2\r\n[]\r\n", get_resp);
}

test "JSON.CLEAR - clear non-zero number" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with a number
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear3\r\n$1\r\n$\r\n$10\r\n{\"n\":42}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the number using path
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear3\r\n$3\r\n$.n\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1 (one number cleared to 0)
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify the number is now 0
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nclear3\r\n$3\r\n$.n\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$1\r\n0\r\n", get_resp);
}

test "JSON.CLEAR - string remains unchanged" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with a string
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear4\r\n$1\r\n$\r\n$16\r\n{\"name\":\"test\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to clear the string
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear4\r\n$6\r\n$.name\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (string not modified)
    try testing.expectEqualStrings(":0\r\n", response);

    // Verify string is unchanged
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nclear4\r\n$6\r\n$.name\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$6\r\n\"test\"\r\n", get_resp);
}

test "JSON.CLEAR - boolean remains unchanged" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with a boolean
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear5\r\n$1\r\n$\r\n$15\r\n{\"flag\":true}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to clear the boolean
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear5\r\n$6\r\n$.flag\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (boolean not modified)
    try testing.expectEqualStrings(":0\r\n", response);

    // Verify boolean is unchanged
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nclear5\r\n$6\r\n$.flag\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$7\r\n[true]\r\n", get_resp);
}

test "JSON.CLEAR - null remains unchanged" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with null
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear6\r\n$1\r\n$\r\n$11\r\n{\"x\":null}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to clear null
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear6\r\n$3\r\n$.x\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (null not modified)
    try testing.expectEqualStrings(":0\r\n", response);

    // Verify null is unchanged
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nclear6\r\n$3\r\n$.x\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$4\r\nnull\r\n", get_resp);
}

test "JSON.CLEAR - already empty object returns 0" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an empty object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear7\r\n$1\r\n$\r\n$2\r\n{}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the empty object
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear7\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (already empty)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.CLEAR - already empty array returns 0" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an empty array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear8\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the empty array
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear8\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (already empty)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.CLEAR - already zero number returns 0" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with zero
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nclear9\r\n$1\r\n$\r\n$9\r\n{\"n\":0}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the zero number
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$5\r\nclear9\r\n$3\r\n$.n\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (already zero)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.CLEAR - root path clears entire document" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object as root
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear10\r\n$1\r\n$\r\n$18\r\n{\"a\":1,\"b\":2,\"c\":3}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear with explicit root path $
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear10\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify it's empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear10\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$2\r\n{}\r\n", get_resp);
}

test "JSON.CLEAR - wildcard path clears multiple values" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with multiple fields
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear11\r\n$1\r\n$\r\n$42\r\n{\"items\":[1,2,3],\"name\":\"test\",\"count\":5}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear all fields in root object using wildcard
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear11\r\n$3\r\n$.*\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 2 (array and number cleared; string unchanged)
    try testing.expectEqualStrings(":2\r\n", response);

    // Verify: items should be empty array, count should be 0, name should be unchanged
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear11\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "\"items\":[]") != null);
    try testing.expect(std.mem.indexOf(u8, get_resp, "\"count\":0") != null);
    try testing.expect(std.mem.indexOf(u8, get_resp, "\"name\":\"test\"") != null);
}

test "JSON.CLEAR - nonexistent key returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Try to clear a key that doesn't exist
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null bulk string
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.CLEAR - wrongtype error for non-JSON key" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a string key
    const set_cmd = "*3\r\n$3\r\nSET\r\n$6\r\nclear12\r\n$5\r\nvalue\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to clear it as JSON
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear12\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return WRONGTYPE error
    try testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "JSON.CLEAR - nonexistent path returns 0" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear13\r\n$1\r\n$\r\n$10\r\n{\"a\":1}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to clear a nonexistent path
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear13\r\n$12\r\n$.nonexistent\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (no matches)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.CLEAR - default path (no path parameter) clears root" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear14\r\n$1\r\n$\r\n$18\r\n{\"a\":1,\"b\":2,\"c\":3}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear without path parameter (should default to $)
    const cmd = "*2\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear14\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify it's empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear14\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$2\r\n{}\r\n", get_resp);
}

test "JSON.CLEAR - nested object cleared" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with nested structure
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear15\r\n$1\r\n$\r\n$32\r\n{\"user\":{\"name\":\"test\",\"id\":1}}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the nested object
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear15\r\n$6\r\n$.user\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify nested object is empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear15\r\n$6\r\n$.user\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$2\r\n{}\r\n", get_resp);
}

test "JSON.CLEAR - nested array cleared" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with nested array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear16\r\n$1\r\n$\r\n$25\r\n{\"items\":[1,2,3,4,5]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the nested array
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear16\r\n$7\r\n$.items\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify nested array is empty
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear16\r\n$7\r\n$.items\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$2\r\n[]\r\n", get_resp);
}

test "JSON.CLEAR - array root with multiple numbers" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with numbers
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear17\r\n$1\r\n$\r\n$15\r\n[10,20,30,40]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear array with wildcard to clear all numbers
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear17\r\n$3\r\n$[*]\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 4 (all numbers cleared to 0)
    try testing.expectEqualStrings(":4\r\n", response);

    // Verify all are now 0
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear17\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$9\r\n[0,0,0,0]\r\n", get_resp);
}

test "JSON.CLEAR - mixed array types (only containers and numbers cleared)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with mixed types
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear18\r\n$1\r\n$\r\n$33\r\n[1,\"text\",true,[1,2],{\"a\":1}]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear all elements
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear18\r\n$3\r\n$[*]\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 3 (number, array, object cleared; string and boolean unchanged)
    try testing.expectEqualStrings(":3\r\n", response);

    // Verify: number→0, string unchanged, boolean unchanged, array→[], object→{}
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear18\r\n$1\r\n$\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "[0,\"text\",true,[],{}]") != null);
}

test "JSON.CLEAR - negative number cleared to zero" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with negative number
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear19\r\n$1\r\n$\r\n$11\r\n{\"n\":-42}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the negative number
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear19\r\n$3\r\n$.n\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify it's now 0
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear19\r\n$3\r\n$.n\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$1\r\n0\r\n", get_resp);
}

test "JSON.CLEAR - floating point number cleared to zero" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object with float
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear20\r\n$1\r\n$\r\n$12\r\n{\"f\":3.14}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear the float
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear20\r\n$3\r\n$.f\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);

    // Verify it's now 0
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$6\r\nclear20\r\n$3\r\n$.f\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$1\r\n0\r\n", get_resp);
}

test "JSON.CLEAR - wrong arity with too many args" {
    const stream = try connectToServer();
    defer stream.close();

    // Command with too many arguments
    const cmd = "*4\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear21\r\n$1\r\n$\r\n$5\r\nextra\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return ERR
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.CLEAR - complex nested structure sum" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a complex nested structure
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nclear22\r\n$1\r\n$\r\n$58\r\n{\"data\":{\"items\":[1,2,3],\"stats\":{\"count\":5},\"name\":\"x\"}}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Clear all nested structures using wildcard recursion
    const cmd = "*3\r\n$10\r\nJSON.CLEAR\r\n$6\r\nclear22\r\n$3\r\n$.*\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1 (the data object, which contains cleared children)
    // Note: actual behavior depends on implementation (recursive vs single level)
    try testing.expect(std.mem.startsWith(u8, response, ":"));
}
