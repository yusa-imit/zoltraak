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

test "JSON.ARRINDEX - find element in array returns index" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr01\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Find element 3 (should be at index 2)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr01\r\n$1\r\n$\r\n$1\r\n3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return integer 2
    try testing.expectEqualStrings(":2\r\n", response);
}

test "JSON.ARRINDEX - element not found returns -1" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr02\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for element 99 (not in array)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr02\r\n$1\r\n$\r\n$2\r\n99\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return -1
    try testing.expectEqualStrings(":-1\r\n", response);
}

test "JSON.ARRINDEX - multiple matches returns first occurrence" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with duplicate elements
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr03\r\n$1\r\n$\r\n$13\r\n[5,3,5,2,5,1]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 5 (appears at indices 0, 2, 4)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr03\r\n$1\r\n$\r\n$1\r\n5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return first index: 0
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.ARRINDEX - find first element in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr04\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Find first element
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr04\r\n$1\r\n$\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.ARRINDEX - find last element in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr05\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Find last element
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr05\r\n$1\r\n$\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 3
    try testing.expectEqualStrings(":3\r\n", response);
}

// ============================================================================
// 2. TYPE HANDLING TESTS
// ============================================================================

test "JSON.ARRINDEX - non-array path returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a JSON object (not array)
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr06\r\n$1\r\n$\r\n$12\r\n{\"x\":1,\"y\":2}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to search in object (not array)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr06\r\n$1\r\n$\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null bulk string
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.ARRINDEX - type mismatch number vs string returns -1" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array of strings
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr07\r\n$1\r\n$\r\n$17\r\n[\"a\",\"b\",\"c\"]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for number 1 (array has strings)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr07\r\n$1\r\n$\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return -1 (not found, type mismatch)
    try testing.expectEqualStrings(":-1\r\n", response);
}

test "JSON.ARRINDEX - search for string in string array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array of strings
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr08\r\n$1\r\n$\r\n$17\r\n[\"a\",\"b\",\"c\"]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for string "b" (need to quote it as JSON)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr08\r\n$1\r\n$\r\n$3\r\n\"b\"\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.ARRINDEX - search for null in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with null
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr09\r\n$1\r\n$\r\n$13\r\n[1,null,2]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for null
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr09\r\n$1\r\n$\r\n$4\r\nnull\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.ARRINDEX - search for boolean in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with booleans
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr10\r\n$1\r\n$\r\n$17\r\n[true,false,true]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for false
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr10\r\n$1\r\n$\r\n$5\r\nfalse\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);
}

// ============================================================================
// 3. RANGE SEARCH TESTS
// ============================================================================

test "JSON.ARRINDEX - with start index only" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr11\r\n$1\r\n$\r\n$11\r\n[1,2,3,2,1]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 starting from index 2 (should find at index 3, skip index 1)
    const cmd = "*5\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr11\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 3 (second occurrence of 2, after index 2)
    try testing.expectEqualStrings(":3\r\n", response);
}

test "JSON.ARRINDEX - with start and stop indices" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr12\r\n$1\r\n$\r\n$13\r\n[1,2,3,2,1,2]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 in range [1, 4)
    const cmd = "*6\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr12\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n1\r\n$1\r\n4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should find 2 at index 1
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.ARRINDEX - negative start index counts from end" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr13\r\n$1\r\n$\r\n$13\r\n[1,2,3,2,1,2]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 starting from -2 (same as index 4 from end)
    const cmd = "*5\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr13\r\n$1\r\n$\r\n$1\r\n2\r\n$2\r\n-2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should find 2 at index 5 (last element)
    try testing.expectEqualStrings(":5\r\n", response);
}

test "JSON.ARRINDEX - negative stop index counts from end" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr14\r\n$1\r\n$\r\n$13\r\n[1,2,3,2,1,2]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 with stop at -1 (stop before last element)
    const cmd = "*6\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr14\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should find 2 at index 1 (before index -1)
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.ARRINDEX - inverse range (start > stop) returns -1" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr15\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 with invalid range (3, 1)
    const cmd = "*6\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr15\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return -1 (invalid range)
    try testing.expectEqualStrings(":-1\r\n", response);
}

test "JSON.ARRINDEX - start index out of bounds returns -1" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr16\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 starting from index 10 (way out of bounds)
    const cmd = "*5\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr16\r\n$1\r\n$\r\n$1\r\n2\r\n$2\r\n10\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return -1
    try testing.expectEqualStrings(":-1\r\n", response);
}

// ============================================================================
// 4. EDGE CASES
// ============================================================================

test "JSON.ARRINDEX - empty array returns -1" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an empty array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr17\r\n$1\r\n$\r\n$2\r\n[]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for anything in empty array
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr17\r\n$1\r\n$\r\n$1\r\n5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return -1
    try testing.expectEqualStrings(":-1\r\n", response);
}

test "JSON.ARRINDEX - single element array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a single-element array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr18\r\n$1\r\n$\r\n$3\r\n[5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for the element
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr18\r\n$1\r\n$\r\n$1\r\n5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.ARRINDEX - search with zero start and stop (full range)" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr19\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search with stop=0 (should search entire array)
    const cmd = "*6\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr19\r\n$1\r\n$\r\n$1\r\n3\r\n$1\r\n0\r\n$1\r\n0\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should find 3 at index 2
    try testing.expectEqualStrings(":2\r\n", response);
}

test "JSON.ARRINDEX - array with nested structures" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with mixed types including nested structures
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr20\r\n$1\r\n$\r\n$35\r\n[1,{\"a\":1},[1,2],\"str\",true]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for number 1 (should be first, not nested ones)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr20\r\n$1\r\n$\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (first element)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.ARRINDEX - floating point numbers" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with floats
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr21\r\n$1\r\n$\r\n$19\r\n[1.5,2.5,3.5,2.5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2.5
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr21\r\n$1\r\n$\r\n$3\r\n2.5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);
}

// ============================================================================
// 5. MULTIPLE PATHS (WILDCARD) TESTS
// ============================================================================

test "JSON.ARRINDEX - wildcard path matches multiple arrays" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a structure with multiple arrays
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr22\r\n$1\r\n$\r\n$41\r\n{\"arr1\":[1,2,3],\"arr2\":[4,5,6]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search in all arrays using wildcard
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr22\r\n$5\r\n$.*[*]\r\n$1\r\n2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array of indices [1, -1] (2 found in arr1, not in arr2)
    try testing.expect(std.mem.startsWith(u8, response, "*"));
}

test "JSON.ARRINDEX - recursive path searches nested arrays" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a deeply nested structure
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr23\r\n$1\r\n$\r\n$45\r\n{\"data\":{\"arr\":[1,2,3]},\"arr\":[4,5,6]}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search in all arrays at any depth
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr23\r\n$6\r\n$..arr\r\n$1\r\n2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array of indices
    try testing.expect(std.mem.startsWith(u8, response, "*"));
}

// ============================================================================
// 6. ERROR CONDITIONS
// ============================================================================

test "JSON.ARRINDEX - wrong arity too few args" {
    const stream = try connectToServer();
    defer stream.close();

    // Command with too few arguments
    const cmd = "*2\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr24\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return ERR
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRINDEX - wrong arity too many args" {
    const stream = try connectToServer();
    defer stream.close();

    // Command with too many arguments
    const cmd = "*8\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr25\r\n$1\r\n$\r\n$1\r\n1\r\n$1\r\n0\r\n$1\r\n5\r\n$5\r\nextra\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return ERR
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.ARRINDEX - nonexistent key returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Search in non-existent key
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return null bulk string
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.ARRINDEX - wrongtype error for non-JSON key" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a string key
    const set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\narr26\r\n$5\r\nvalue\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Try to search as JSON
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr26\r\n$1\r\n$\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return WRONGTYPE error
    try testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "JSON.ARRINDEX - invalid path syntax" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a valid JSON document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr27\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use invalid path syntax
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr27\r\n$10\r\n$.[invalid]\r\n$1\r\n1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error or empty array
    try testing.expect(std.mem.startsWith(u8, response, "-") or std.mem.startsWith(u8, response, "*"));
}

test "JSON.ARRINDEX - invalid start index type" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a valid JSON array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr28\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use non-numeric start index
    const cmd = "*5\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr28\r\n$1\r\n$\r\n$1\r\n2\r\n$3\r\nabc\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

test "JSON.ARRINDEX - invalid stop index type" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a valid JSON array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr29\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Use non-numeric stop index
    const cmd = "*6\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr29\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n0\r\n$3\r\nxyz\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
}

// ============================================================================
// 7. PATHLESS SEARCH (DEFAULT PATH) TESTS
// ============================================================================

test "JSON.ARRINDEX - array at root without explicit path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set array as root document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr30\r\n$1\r\n$\r\n$9\r\n[1,2,3,4]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search with explicit $ path
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr30\r\n$1\r\n$\r\n$1\r\n3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 2
    try testing.expectEqualStrings(":2\r\n", response);
}

// ============================================================================
// 8. COMPLEX SCENARIOS
// ============================================================================

test "JSON.ARRINDEX - search in array with mixed numeric types" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with integers and floats
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr31\r\n$1\r\n$\r\n$18\r\n[1,2.0,3,4.5,5]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for integer 2 (but stored as 2.0)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr31\r\n$1\r\n$\r\n$1\r\n2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should find at index 1 (2.0 should equal 2)
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.ARRINDEX - large array performance" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a large array (1-100)
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr32\r\n$1\r\n$\r\n$345\r\n[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for element near end
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr32\r\n$1\r\n$\r\n$2\r\n99\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 98 (0-indexed)
    try testing.expectEqualStrings(":98\r\n", response);
}

test "JSON.ARRINDEX - nested array path search" {
    const stream = try connectToServer();
    defer stream.close();

    // Set a structure with nested array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr33\r\n$1\r\n$\r\n$40\r\n{\"user\":{\"tags\":[\"a\",\"b\",\"c\"]}}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search in nested array
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr33\r\n$12\r\n$.user.tags\r\n$3\r\n\"b\"\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.ARRINDEX - search for deeply nested object in array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array with objects
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr34\r\n$1\r\n$\r\n$51\r\n[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for object (not typical, but should work or fail gracefully)
    const cmd = "*4\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr34\r\n$1\r\n$\r\n$16\r\n{\"id\":1,\"name\":\"a\"}\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Behavior depends on implementation (might find at 0 or -1)
    try testing.expect(std.mem.startsWith(u8, response, ":") or std.mem.startsWith(u8, response, "-"));
}

test "JSON.ARRINDEX - search with range narrower than array" {
    const stream = try connectToServer();
    defer stream.close();

    // Set an array
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\narr35\r\n$1\r\n$\r\n$13\r\n[1,2,3,2,1,2]\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Search for 2 in narrow range [2, 3)
    const cmd = "*6\r\n$11\r\nJSON.ARRINDEX\r\n$5\r\narr35\r\n$1\r\n$\r\n$1\r\n2\r\n$1\r\n2\r\n$1\r\n3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return -1 (3 not in range [2,3))
    try testing.expectEqualStrings(":-1\r\n", response);
}
