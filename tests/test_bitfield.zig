const std = @import("std");
const testing = std.testing;
const test_integration = @import("test_integration.zig");
const RespClient = test_integration.RespClient;
const TestServer = test_integration.TestServer;

// ============================================================================
// BITFIELD Command Tests
// ============================================================================

test "BITFIELD - GET unsigned 8-bit at offset 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a string value first
    _ = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "ABC" });

    // GET u8 at offset 0 (first byte 'A' = 65)
    const response = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "GET", "u8", "0" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*1\r\n:65\r\n", response);
}

test "BITFIELD - SET and GET signed 8-bit" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // SET i8 at offset 0 to -42, should return old value (0)
    const set_response = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "SET", "i8", "0", "-42" });
    defer testing.allocator.free(set_response);

    try testing.expectEqualStrings("*1\r\n:0\r\n", set_response);

    // GET i8 at offset 0, should return -42
    const get_response = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "GET", "i8", "0" });
    defer testing.allocator.free(get_response);

    try testing.expectEqualStrings("*1\r\n:-42\r\n", get_response);
}

test "BITFIELD - INCRBY with WRAP overflow" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set u8 to 250
    _ = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "SET", "u8", "0", "250" });

    // Increment by 10 with WRAP (default), should wrap to 4
    const response = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "INCRBY", "u8", "0", "10" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*1\r\n:4\r\n", response);
}

test "BITFIELD - INCRBY with SAT overflow" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set u8 to 250
    _ = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "SET", "u8", "0", "250" });

    // Increment by 10 with SAT, should saturate at 255
    const response = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "10" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*1\r\n:255\r\n", response);
}

test "BITFIELD - INCRBY with FAIL overflow" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set u8 to 250
    _ = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "SET", "u8", "0", "250" });

    // Increment by 10 with FAIL, should return nil
    const response = try client.sendCommand(&[_][]const u8{ "BITFIELD", "mykey", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "10" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*1\r\n$-1\r\n", response);
}

test "BITFIELD - Multiple operations in single command" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Multiple operations: SET, GET, INCRBY
    const response = try client.sendCommand(&[_][]const u8{
        "BITFIELD", "mykey",
        "SET",    "u8", "0", "100",
        "GET",    "u8", "0",
        "INCRBY", "u8", "0", "20",
    });
    defer testing.allocator.free(response);

    // Should return array with 3 results: 0 (old value), 100 (current), 120 (after increment)
    try testing.expectEqualStrings("*3\r\n:0\r\n:100\r\n:120\r\n", response);
}

// ============================================================================
// BITFIELD_RO Command Tests
// ============================================================================

test "BITFIELD_RO - GET unsigned 8-bit at offset 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a string value first
    _ = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "XYZ" });

    // GET u8 at offset 0 (first byte 'X' = 88)
    const response = try client.sendCommand(&[_][]const u8{ "BITFIELD_RO", "mykey", "GET", "u8", "0" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*1\r\n:88\r\n", response);
}

test "BITFIELD_RO - Multiple GET operations" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value
    _ = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "ABC" });

    // Multiple GETs
    const response = try client.sendCommand(&[_][]const u8{
        "BITFIELD_RO", "mykey",
        "GET", "u8", "0",
        "GET", "u8", "8",
        "GET", "u8", "16",
    });
    defer testing.allocator.free(response);

    // Should return A=65, B=66, C=67
    try testing.expectEqualStrings("*3\r\n:65\r\n:66\r\n:67\r\n", response);
}

test "BITFIELD_RO - rejects write operations" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to SET (should fail)
    const response = try client.sendCommand(&[_][]const u8{ "BITFIELD_RO", "mykey", "SET", "u8", "0", "42" });
    defer testing.allocator.free(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}
