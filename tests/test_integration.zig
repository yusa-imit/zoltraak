const std = @import("std");
const testing = std.testing;
const net = std.net;

// RESP Protocol helpers for integration testing
const RespClient = struct {
    allocator: std.mem.Allocator,
    stream: net.Stream,

    pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !RespClient {
        const address = try net.Address.parseIp(host, port);
        const stream = try net.tcpConnectToAddress(address);

        return RespClient{
            .allocator = allocator,
            .stream = stream,
        };
    }

    pub fn deinit(self: *RespClient) void {
        self.stream.close();
    }

    /// Send a command and receive response
    pub fn sendCommand(self: *RespClient, args: []const []const u8) ![]u8 {
        // Build RESP array command using a fixed buffer
        var cmd_buffer: [4096]u8 = undefined;
        var cmd_len: usize = 0;

        // Array header
        cmd_len += (try std.fmt.bufPrint(cmd_buffer[cmd_len..], "*{d}\r\n", .{args.len})).len;

        // Each argument as bulk string
        for (args) |arg| {
            cmd_len += (try std.fmt.bufPrint(cmd_buffer[cmd_len..], "${d}\r\n{s}\r\n", .{ arg.len, arg })).len;
        }

        // Send command
        try self.stream.writeAll(cmd_buffer[0..cmd_len]);

        // Read response
        var read_buffer: [4096]u8 = undefined;
        const bytes_read = try self.stream.read(&read_buffer);

        // Allocate and return response
        const response = try self.allocator.dupe(u8, read_buffer[0..bytes_read]);
        return response;
    }
};

// Helper to start server in background
const TestServer = struct {
    process: std.process.Child,
    allocator: std.mem.Allocator,

    pub fn start(allocator: std.mem.Allocator) !TestServer {
        var process = std.process.Child.init(&[_][]const u8{"./zig-out/bin/zoltraak"}, allocator);
        process.stdout_behavior = .Ignore;
        process.stderr_behavior = .Ignore;

        try process.spawn();

        // Wait a bit for server to start
        std.Thread.sleep(500 * std.time.ns_per_ms);

        return TestServer{
            .process = process,
            .allocator = allocator,
        };
    }

    pub fn stop(self: *TestServer) void {
        _ = self.process.kill() catch {};
        _ = self.process.wait() catch {};
    }
};

// ============================================================================
// PING Command Tests
// ============================================================================

test "PING - returns PONG without argument" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"PING"});
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+PONG\r\n", response);
}

test "PING - echoes message with argument" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "PING", "hello" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$5\r\nhello\r\n", response);
}

test "PING - case insensitive" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"ping"});
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+PONG\r\n", response);
}

test "PING - too many arguments returns error" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "PING", "arg1", "arg2" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

// ============================================================================
// SET Command Tests
// ============================================================================

test "SET - basic key-value storage" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SET - overwrites existing value" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "value1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Overwrite with new value
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "value2" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Verify new value
    {
        const response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$6\r\nvalue2\r\n", response);
    }
}

test "SET - with EX option (seconds)" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "expkey", "value", "EX", "60" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);

    // Verify key exists
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "expkey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$5\r\nvalue\r\n", get_response);
}

test "SET - with PX option (milliseconds)" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "expkey", "value", "PX", "5000" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SET - with NX (only if not exists) succeeds when key absent" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "newkey", "value", "NX" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SET - with NX fails when key exists" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create key first
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "existingkey", "value1" });
        defer testing.allocator.free(response);
    }

    // Try to set with NX - should fail
    const response = try client.sendCommand(&[_][]const u8{ "SET", "existingkey", "value2", "NX" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "SET - with XX (only if exists) succeeds when key present" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create key first
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "existingkey", "value1" });
        defer testing.allocator.free(response);
    }

    // Update with XX - should succeed
    const response = try client.sendCommand(&[_][]const u8{ "SET", "existingkey", "value2", "XX" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SET - with XX fails when key absent" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "nonexistent", "value", "XX" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "SET - NX and XX together returns error" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "key", "value", "NX", "XX" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR syntax error"));
}

test "SET - negative expiration returns error" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "key", "value", "EX", "-1" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR invalid expire time"));
}

test "SET - empty key and value" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "", "" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SET - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SET", "key" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// GET Command Tests
// ============================================================================

test "GET - retrieves existing key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "hello" });
        defer testing.allocator.free(response);
    }

    // Get the key
    const response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$5\r\nhello\r\n", response);
}

test "GET - returns null for non-existent key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "GET", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "GET - returns null for expired key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set key with very short expiration (1 millisecond)
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "expkey", "value", "PX", "1" });
        defer testing.allocator.free(response);
    }

    // Wait for expiration
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Try to get - should return null
    const response = try client.sendCommand(&[_][]const u8{ "GET", "expkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "GET - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"GET"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "GET - retrieves empty value" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set empty value
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "emptykey", "" });
        defer testing.allocator.free(response);
    }

    // Get empty value
    const response = try client.sendCommand(&[_][]const u8{ "GET", "emptykey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$0\r\n\r\n", response);
}

// ============================================================================
// DEL Command Tests
// ============================================================================

test "DEL - deletes single existing key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
        defer testing.allocator.free(response);
    }

    // Delete key
    const response = try client.sendCommand(&[_][]const u8{ "DEL", "key1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "key1" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DEL - returns 0 for non-existent key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "DEL", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "DEL - deletes multiple keys" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create keys
    {
        const r1 = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
        defer testing.allocator.free(r1);
        const r2 = try client.sendCommand(&[_][]const u8{ "SET", "key2", "value2" });
        defer testing.allocator.free(r2);
        const r3 = try client.sendCommand(&[_][]const u8{ "SET", "key3", "value3" });
        defer testing.allocator.free(r3);
    }

    // Delete multiple keys including non-existent
    const response = try client.sendCommand(&[_][]const u8{ "DEL", "key1", "key2", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":2\r\n", response);
}

test "DEL - duplicate keys counted once" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
        defer testing.allocator.free(response);
    }

    // Delete same key twice
    const response = try client.sendCommand(&[_][]const u8{ "DEL", "key1", "key1" });
    defer testing.allocator.free(response);

    // Should only count as 1 deletion (second one doesn't exist)
    try testing.expectEqualStrings(":1\r\n", response);
}

test "DEL - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"DEL"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// EXISTS Command Tests
// ============================================================================

test "EXISTS - returns 1 for existing key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
        defer testing.allocator.free(response);
    }

    // Check existence
    const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "key1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "EXISTS - returns 0 for non-existent key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "EXISTS - counts multiple keys" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create keys
    {
        const r1 = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
        defer testing.allocator.free(r1);
        const r2 = try client.sendCommand(&[_][]const u8{ "SET", "key2", "value2" });
        defer testing.allocator.free(r2);
    }

    // Check multiple keys including non-existent
    const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "key1", "key2", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":2\r\n", response);
}

test "EXISTS - duplicate keys counted multiple times" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
        defer testing.allocator.free(response);
    }

    // Check same key three times
    const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "key1", "key1", "key1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "EXISTS - returns 0 for expired key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set key with very short expiration
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "expkey", "value", "PX", "1" });
        defer testing.allocator.free(response);
    }

    // Wait for expiration
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Check existence - should return 0
    const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "expkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "EXISTS - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"EXISTS"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// Error Handling Tests
// ============================================================================

test "Unknown command returns error" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"UNKNOWNCMD"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR unknown command"));
}

test "Case insensitive commands work" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Test lowercase
    {
        const response = try client.sendCommand(&[_][]const u8{ "set", "key1", "value1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Test mixed case
    {
        const response = try client.sendCommand(&[_][]const u8{ "GeT", "key1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$6\r\nvalue1\r\n", response);
    }
}

// ============================================================================
// Complex Integration Tests
// ============================================================================

test "Integration - full workflow with multiple commands" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // PING to verify connection
    {
        const response = try client.sendCommand(&[_][]const u8{"PING"});
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+PONG\r\n", response);
    }

    // SET multiple keys
    {
        const r1 = try client.sendCommand(&[_][]const u8{ "SET", "user:1:name", "Alice" });
        defer testing.allocator.free(r1);
        const r2 = try client.sendCommand(&[_][]const u8{ "SET", "user:2:name", "Bob" });
        defer testing.allocator.free(r2);
        const r3 = try client.sendCommand(&[_][]const u8{ "SET", "session:123", "active", "EX", "3600" });
        defer testing.allocator.free(r3);
    }

    // GET values
    {
        const response = try client.sendCommand(&[_][]const u8{ "GET", "user:1:name" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$5\r\nAlice\r\n", response);
    }

    // EXISTS check
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "user:1:name", "user:2:name", "session:123" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // DEL one key
    {
        const response = try client.sendCommand(&[_][]const u8{ "DEL", "user:1:name" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify deletion
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "user:1:name", "user:2:name" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }
}

test "Integration - SET with multiple options combined" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // SET with NX and EX - should succeed
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "newkey", "value", "NX", "EX", "60" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Try to overwrite with NX - should fail
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "newkey", "newvalue", "NX" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$-1\r\n", response);
    }

    // Update with XX - should succeed
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "newkey", "updated", "XX" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Verify final value
    {
        const response = try client.sendCommand(&[_][]const u8{ "GET", "newkey" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$7\r\nupdated\r\n", response);
    }
}

test "Integration - large value storage and retrieval" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create a large value (1KB)
    var large_value: [1024]u8 = undefined;
    for (&large_value, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    // SET large value
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "largekey", &large_value });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // GET and verify
    {
        const response = try client.sendCommand(&[_][]const u8{ "GET", "largekey" });
        defer testing.allocator.free(response);

        // Verify bulk string prefix
        try testing.expect(std.mem.startsWith(u8, response, "$1024\r\n"));
    }
}

// ============================================================================
// LIST Command Tests - LPUSH/RPUSH
// ============================================================================

test "LPUSH - single element to new list" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "mylist", "hello" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "LPUSH - multiple elements to new list" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "mylist", "a", "b", "c" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "LPUSH - appending to existing list" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create list
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "mylist", "first" });
        defer testing.allocator.free(response);
    }

    // Add more elements
    const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "mylist", "second", "third" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "LPUSH - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try LPUSH on string
    const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "stringkey", "element" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "LPUSH - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"LPUSH"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "RPUSH - single element to new list" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "hello" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "RPUSH - multiple elements to new list" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "RPUSH - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try RPUSH on string
    const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "stringkey", "element" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

// ============================================================================
// LIST Command Tests - LPOP/RPOP
// ============================================================================

test "LPOP - without count parameter" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
        defer testing.allocator.free(response);
    }

    // Pop single element
    const response = try client.sendCommand(&[_][]const u8{ "LPOP", "mylist" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$1\r\na\r\n", response);
}

test "LPOP - with count parameter" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c", "d", "e" });
        defer testing.allocator.free(response);
    }

    // Pop 3 elements
    const response = try client.sendCommand(&[_][]const u8{ "LPOP", "mylist", "3" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "a") != null);
    try testing.expect(std.mem.indexOf(u8, response, "b") != null);
    try testing.expect(std.mem.indexOf(u8, response, "c") != null);
}

test "LPOP - on non-existent key returns nil" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "LPOP", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "LPOP - with count on non-existent key returns empty array" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "LPOP", "nosuchkey", "5" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*0\r\n", response);
}

test "LPOP - count greater than list length" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup small list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b" });
        defer testing.allocator.free(response);
    }

    // Try to pop more than available
    const response = try client.sendCommand(&[_][]const u8{ "LPOP", "mylist", "10" });
    defer testing.allocator.free(response);

    // Should return only 2 elements
    try testing.expect(std.mem.indexOf(u8, response, "*2\r\n") != null);
}

test "LPOP - empty list auto-deletion" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup single element list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "single" });
        defer testing.allocator.free(response);
    }

    // Pop the only element
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "mylist" });
        defer testing.allocator.free(response);
    }

    // Verify key no longer exists
    const exists_response = try client.sendCommand(&[_][]const u8{ "EXISTS", "mylist" });
    defer testing.allocator.free(exists_response);

    try testing.expectEqualStrings(":0\r\n", exists_response);
}

test "RPOP - without count parameter" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
        defer testing.allocator.free(response);
    }

    // Pop from tail
    const response = try client.sendCommand(&[_][]const u8{ "RPOP", "mylist" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$1\r\nc\r\n", response);
}

test "RPOP - with count parameter" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c", "d", "e" });
        defer testing.allocator.free(response);
    }

    // Pop 3 elements from tail
    const response = try client.sendCommand(&[_][]const u8{ "RPOP", "mylist", "3" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "e") != null);
    try testing.expect(std.mem.indexOf(u8, response, "d") != null);
    try testing.expect(std.mem.indexOf(u8, response, "c") != null);
}

test "RPOP - on non-existent key returns nil" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "RPOP", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

// ============================================================================
// LIST Command Tests - LRANGE
// ============================================================================

test "LRANGE - full list (0 -1)" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c", "d", "e" });
        defer testing.allocator.free(response);
    }

    // Get all elements
    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "0", "-1" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*5\r\n") != null);
}

test "LRANGE - with positive indices" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c", "d", "e" });
        defer testing.allocator.free(response);
    }

    // Get range [1, 3] (b, c, d)
    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "1", "3" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "b") != null);
    try testing.expect(std.mem.indexOf(u8, response, "c") != null);
    try testing.expect(std.mem.indexOf(u8, response, "d") != null);
}

test "LRANGE - with negative indices" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c", "d", "e" });
        defer testing.allocator.free(response);
    }

    // Get last 3 elements
    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "-3", "-1" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "c") != null);
    try testing.expect(std.mem.indexOf(u8, response, "d") != null);
    try testing.expect(std.mem.indexOf(u8, response, "e") != null);
}

test "LRANGE - out of bounds returns clamped range" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
        defer testing.allocator.free(response);
    }

    // Request range far exceeding list size
    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "-100", "100" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
}

test "LRANGE - start greater than stop returns empty array" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
        defer testing.allocator.free(response);
    }

    // Invalid range
    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "5", "1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*0\r\n", response);
}

test "LRANGE - on non-existent key returns empty array" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "nosuchkey", "0", "-1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*0\r\n", response);
}

test "LRANGE - single element (start == stop)" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
        defer testing.allocator.free(response);
    }

    // Get single element at index 1
    const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "1", "1" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "b") != null);
}

// ============================================================================
// LIST Command Tests - LLEN
// ============================================================================

test "LLEN - returns list length" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup list
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c", "d" });
        defer testing.allocator.free(response);
    }

    const response = try client.sendCommand(&[_][]const u8{ "LLEN", "mylist" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":4\r\n", response);
}

test "LLEN - on non-existent key returns 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "LLEN", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "LLEN - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try LLEN on string
    const response = try client.sendCommand(&[_][]const u8{ "LLEN", "stringkey" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "LLEN - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"LLEN"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// LIST Workflow Integration Tests
// ============================================================================

test "Lists - stack behavior (LPUSH + LPOP)" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Push elements to head
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "stack", "first", "second", "third" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Pop from head (LIFO order)
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "stack" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$5\r\nthird\r\n", response);
    }

    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "stack" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$6\r\nsecond\r\n", response);
    }

    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "stack" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$5\r\nfirst\r\n", response);
    }
}

test "Lists - queue behavior (RPUSH + LPOP)" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Enqueue at tail
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "queue", "job1", "job2", "job3" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Dequeue from head (FIFO order)
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "queue" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$4\r\njob1\r\n", response);
    }

    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "queue" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$4\r\njob2\r\n", response);
    }

    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "queue" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$4\r\njob3\r\n", response);
    }
}

test "Lists - combined LPUSH and RPUSH operations" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // RPUSH a b
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":2\r\n", response);
    }

    // LPUSH x y (prepends)
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "mylist", "x", "y" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":4\r\n", response);
    }

    // LRANGE should show [y, x, a, b]
    {
        const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "mylist", "0", "-1" });
        defer testing.allocator.free(response);

        try testing.expect(std.mem.indexOf(u8, response, "*4\r\n") != null);
        // Just verify we got 4 elements with the expected values
        try testing.expect(std.mem.indexOf(u8, response, "$1\r\ny\r\n") != null);
        try testing.expect(std.mem.indexOf(u8, response, "$1\r\nx\r\n") != null);
        try testing.expect(std.mem.indexOf(u8, response, "$1\r\na\r\n") != null);
        try testing.expect(std.mem.indexOf(u8, response, "$1\r\nb\r\n") != null);
    }
}

test "Lists - empty list after all pops" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create list with 2 elements
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "templist", "a", "b" });
        defer testing.allocator.free(response);
    }

    // Verify exists
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "templist" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Pop all elements
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "templist", "2" });
        defer testing.allocator.free(response);
    }

    // Verify list is deleted
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "templist" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // LLEN should return 0
    {
        const response = try client.sendCommand(&[_][]const u8{ "LLEN", "templist" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }
}

test "Lists - complex workflow with mixed operations" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Build list: RPUSH 1 2 3
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "workflow", "1", "2", "3" });
        defer testing.allocator.free(response);
    }

    // LPUSH 0 (prepend)
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "workflow", "0" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":4\r\n", response);
    }

    // RPUSH 4 5 (append)
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPUSH", "workflow", "4", "5" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":6\r\n", response);
    }

    // Check length
    {
        const response = try client.sendCommand(&[_][]const u8{ "LLEN", "workflow" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":6\r\n", response);
    }

    // LRANGE middle section [1, 4] -> "1", "2", "3", "4"
    {
        const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "workflow", "1", "4" });
        defer testing.allocator.free(response);
        try testing.expect(std.mem.indexOf(u8, response, "*4\r\n") != null);
    }

    // LPOP 2 from head
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPOP", "workflow", "2" });
        defer testing.allocator.free(response);
        // Should get "0" and "1"
        try testing.expect(std.mem.indexOf(u8, response, "0") != null);
        try testing.expect(std.mem.indexOf(u8, response, "1") != null);
    }

    // RPOP 1 from tail
    {
        const response = try client.sendCommand(&[_][]const u8{ "RPOP", "workflow" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$1\r\n5\r\n", response);
    }

    // Final LRANGE should show [2, 3, 4]
    {
        const response = try client.sendCommand(&[_][]const u8{ "LRANGE", "workflow", "0", "-1" });
        defer testing.allocator.free(response);
        try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    }

    // Final length
    {
        const response = try client.sendCommand(&[_][]const u8{ "LLEN", "workflow" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }
}

// ============================================================================
// SET Command Tests - SADD
// ============================================================================

test "SADD - single member to new set" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "hello" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "SADD - multiple members to new set" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two", "three" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "SADD - duplicate member returns 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Add member first time
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "hello" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Add same member again
    const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "hello" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "SADD - mixed new and existing members" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Add initial members
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two" });
        defer testing.allocator.free(response);
    }

    // Add mix of new and existing
    const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "two", "three", "four" });
    defer testing.allocator.free(response);

    // Should count only "three" and "four" as new
    try testing.expectEqualStrings(":2\r\n", response);
}

test "SADD - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try SADD on string
    const response = try client.sendCommand(&[_][]const u8{ "SADD", "stringkey", "member" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "SADD - on list key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create list key
    {
        const response = try client.sendCommand(&[_][]const u8{ "LPUSH", "listkey", "element" });
        defer testing.allocator.free(response);
    }

    // Try SADD on list
    const response = try client.sendCommand(&[_][]const u8{ "SADD", "listkey", "member" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "SADD - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// SET Command Tests - SREM
// ============================================================================

test "SREM - single member from set" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two", "three" });
        defer testing.allocator.free(response);
    }

    // Remove one member
    const response = try client.sendCommand(&[_][]const u8{ "SREM", "myset", "two" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "SREM - multiple members from set" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two", "three", "four" });
        defer testing.allocator.free(response);
    }

    // Remove multiple members
    const response = try client.sendCommand(&[_][]const u8{ "SREM", "myset", "one", "three" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":2\r\n", response);
}

test "SREM - non-existent member returns 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two" });
        defer testing.allocator.free(response);
    }

    // Remove non-existent member
    const response = try client.sendCommand(&[_][]const u8{ "SREM", "myset", "three" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "SREM - on non-existent key returns 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SREM", "nosuchkey", "member" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "SREM - empty set auto-deletion" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup single member set
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "single" });
        defer testing.allocator.free(response);
    }

    // Remove the only member
    {
        const response = try client.sendCommand(&[_][]const u8{ "SREM", "myset", "single" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify key no longer exists
    const exists_response = try client.sendCommand(&[_][]const u8{ "EXISTS", "myset" });
    defer testing.allocator.free(exists_response);

    try testing.expectEqualStrings(":0\r\n", exists_response);
}

test "SREM - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try SREM on string
    const response = try client.sendCommand(&[_][]const u8{ "SREM", "stringkey", "member" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "SREM - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SREM", "myset" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// SET Command Tests - SISMEMBER
// ============================================================================

test "SISMEMBER - returns 1 for existing member" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "hello", "world" });
        defer testing.allocator.free(response);
    }

    const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "myset", "hello" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "SISMEMBER - returns 0 for non-existent member" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "hello" });
        defer testing.allocator.free(response);
    }

    const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "myset", "world" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "SISMEMBER - returns 0 for non-existent key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "nosuchkey", "member" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "SISMEMBER - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try SISMEMBER on string
    const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "stringkey", "member" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "SISMEMBER - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "myset" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// SET Command Tests - SMEMBERS
// ============================================================================

test "SMEMBERS - returns all members" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two", "three" });
        defer testing.allocator.free(response);
    }

    const response = try client.sendCommand(&[_][]const u8{ "SMEMBERS", "myset" });
    defer testing.allocator.free(response);

    // Should return array with 3 elements
    try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "one") != null);
    try testing.expect(std.mem.indexOf(u8, response, "two") != null);
    try testing.expect(std.mem.indexOf(u8, response, "three") != null);
}

test "SMEMBERS - returns empty array for non-existent key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SMEMBERS", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*0\r\n", response);
}

test "SMEMBERS - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try SMEMBERS on string
    const response = try client.sendCommand(&[_][]const u8{ "SMEMBERS", "stringkey" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "SMEMBERS - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"SMEMBERS"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// SET Command Tests - SCARD
// ============================================================================

test "SCARD - returns cardinality of set" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Setup
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two", "three", "four" });
        defer testing.allocator.free(response);
    }

    const response = try client.sendCommand(&[_][]const u8{ "SCARD", "myset" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":4\r\n", response);
}

test "SCARD - returns 0 for non-existent key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "SCARD", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "SCARD - on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "stringkey", "value" });
        defer testing.allocator.free(response);
    }

    // Try SCARD on string
    const response = try client.sendCommand(&[_][]const u8{ "SCARD", "stringkey" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "SCARD - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"SCARD"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

// ============================================================================
// SET Workflow Integration Tests
// ============================================================================

test "Sets - tag system workflow" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Add tags to article
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "article:1:tags", "redis", "database", "nosql" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Check if article has "redis" tag
    {
        const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "article:1:tags", "redis" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Check if article has "sql" tag
    {
        const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "article:1:tags", "sql" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // Get all tags
    {
        const response = try client.sendCommand(&[_][]const u8{ "SMEMBERS", "article:1:tags" });
        defer testing.allocator.free(response);
        try testing.expect(std.mem.indexOf(u8, response, "*3\r\n") != null);
    }

    // Count tags
    {
        const response = try client.sendCommand(&[_][]const u8{ "SCARD", "article:1:tags" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Remove a tag
    {
        const response = try client.sendCommand(&[_][]const u8{ "SREM", "article:1:tags", "nosql" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify tag count after removal
    {
        const response = try client.sendCommand(&[_][]const u8{ "SCARD", "article:1:tags" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":2\r\n", response);
    }
}

test "Sets - unique visitors tracking" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Track visitors
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "page:visitors", "user:123" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "page:visitors", "user:456" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Same user visits again - should not increment
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "page:visitors", "user:123" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // Get unique visitor count
    {
        const response = try client.sendCommand(&[_][]const u8{ "SCARD", "page:visitors" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":2\r\n", response);
    }

    // Check if specific user visited
    {
        const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "page:visitors", "user:456" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }
}

test "Sets - combined operations with multiple sets" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create first set
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "set1", "a", "b", "c" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Create second set
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "set2", "c", "d", "e" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Verify cardinalities
    {
        const r1 = try client.sendCommand(&[_][]const u8{ "SCARD", "set1" });
        defer testing.allocator.free(r1);
        try testing.expectEqualStrings(":3\r\n", r1);

        const r2 = try client.sendCommand(&[_][]const u8{ "SCARD", "set2" });
        defer testing.allocator.free(r2);
        try testing.expectEqualStrings(":3\r\n", r2);
    }

    // Check common element
    {
        const r1 = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "set1", "c" });
        defer testing.allocator.free(r1);
        try testing.expectEqualStrings(":1\r\n", r1);

        const r2 = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "set2", "c" });
        defer testing.allocator.free(r2);
        try testing.expectEqualStrings(":1\r\n", r2);
    }

    // Remove element from first set
    {
        const response = try client.sendCommand(&[_][]const u8{ "SREM", "set1", "c" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify element still in second set
    {
        const response = try client.sendCommand(&[_][]const u8{ "SISMEMBER", "set2", "c" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }
}

test "Sets - duplicate handling in SADD" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Add members with duplicates in same command
    const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "a", "b", "a", "c", "b", "d" });
    defer testing.allocator.free(response);

    // Should only count unique members added (a, b, c, d = 4)
    try testing.expectEqualStrings(":4\r\n", response);

    // Verify cardinality
    const card_response = try client.sendCommand(&[_][]const u8{ "SCARD", "myset" });
    defer testing.allocator.free(card_response);

    try testing.expectEqualStrings(":4\r\n", card_response);
}

test "Sets - all members removed deletes key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create set
    {
        const response = try client.sendCommand(&[_][]const u8{ "SADD", "myset", "one", "two", "three" });
        defer testing.allocator.free(response);
    }

    // Verify it exists
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "myset" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Remove all members at once
    {
        const response = try client.sendCommand(&[_][]const u8{ "SREM", "myset", "one", "two", "three" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Verify key no longer exists
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "myset" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // SCARD should return 0
    {
        const response = try client.sendCommand(&[_][]const u8{ "SCARD", "myset" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // SMEMBERS should return empty array
    {
        const response = try client.sendCommand(&[_][]const u8{ "SMEMBERS", "myset" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("*0\r\n", response);
    }
}

// ============================================================================
// Hash Commands Tests
// ============================================================================

test "Hash - HSET creates new hash and field" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":1\r\n", response);
}

test "Hash - HSET with multiple field-value pairs" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1", "field2", "value2", "field3", "value3" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "Hash - HSET update existing field returns 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Update same field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "newvalue" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }
}

test "Hash - HGET retrieves field value" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "Hello World" });
        defer testing.allocator.free(response);
    }

    // Get field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HGET", "myhash", "field1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$11\r\nHello World\r\n", response);
    }
}

test "Hash - HGET non-existent field returns null" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "HGET", "myhash", "nosuchfield" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "Hash - HGET non-existent key returns null" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "HGET", "nosuchkey", "field1" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "Hash - HDEL deletes field" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set multiple fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1", "field2", "value2" });
        defer testing.allocator.free(response);
    }

    // Delete one field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HDEL", "myhash", "field1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify field is gone
    {
        const response = try client.sendCommand(&[_][]const u8{ "HGET", "myhash", "field1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$-1\r\n", response);
    }

    // Verify other field still exists
    {
        const response = try client.sendCommand(&[_][]const u8{ "HGET", "myhash", "field2" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$6\r\nvalue2\r\n", response);
    }
}

test "Hash - HDEL multiple fields" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3" });
        defer testing.allocator.free(response);
    }

    // Delete multiple
    {
        const response = try client.sendCommand(&[_][]const u8{ "HDEL", "myhash", "f1", "f3" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":2\r\n", response);
    }
}

test "Hash - HGETALL returns all fields and values" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1", "field2", "value2" });
        defer testing.allocator.free(response);
    }

    // Get all
    {
        const response = try client.sendCommand(&[_][]const u8{ "HGETALL", "myhash" });
        defer testing.allocator.free(response);
        
        // Response should be array with 4 elements (field1, value1, field2, value2)
        try testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));
    }
}

test "Hash - HGETALL on non-existent key returns empty array" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "HGETALL", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("*0\r\n", response);
}

test "Hash - HKEYS returns all field names" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1", "field2", "value2" });
        defer testing.allocator.free(response);
    }

    // Get keys
    {
        const response = try client.sendCommand(&[_][]const u8{ "HKEYS", "myhash" });
        defer testing.allocator.free(response);
        
        // Response should be array with 2 elements
        try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
    }
}

test "Hash - HVALS returns all values" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1", "field2", "value2" });
        defer testing.allocator.free(response);
    }

    // Get values
    {
        const response = try client.sendCommand(&[_][]const u8{ "HVALS", "myhash" });
        defer testing.allocator.free(response);
        
        // Response should be array with 2 elements
        try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
    }
}

test "Hash - HEXISTS checks field existence" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1" });
        defer testing.allocator.free(response);
    }

    // Check existing field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HEXISTS", "myhash", "field1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Check non-existent field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HEXISTS", "myhash", "field2" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // Check non-existent key
    {
        const response = try client.sendCommand(&[_][]const u8{ "HEXISTS", "nosuchkey", "field1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }
}

test "Hash - HLEN returns field count" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3" });
        defer testing.allocator.free(response);
    }

    // Get length
    {
        const response = try client.sendCommand(&[_][]const u8{ "HLEN", "myhash" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }
}

test "Hash - HLEN on non-existent key returns 0" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "HLEN", "nosuchkey" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "Hash - HSET on string key returns WRONGTYPE" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "value" });
        defer testing.allocator.free(response);
    }

    // Try HSET on string key
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "mykey", "field", "value" });
        defer testing.allocator.free(response);
        try testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
    }
}

test "Hash - deleting all fields deletes key" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create hash
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "myhash", "field1", "value1" });
        defer testing.allocator.free(response);
    }

    // Verify it exists
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "myhash" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Delete all fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HDEL", "myhash", "field1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify key no longer exists
    {
        const response = try client.sendCommand(&[_][]const u8{ "EXISTS", "myhash" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }
}

test "Hash - comprehensive workflow" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create user profile
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "user:1", "name", "Alice", "email", "alice@example.com", "age", "30" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Get name
    {
        const response = try client.sendCommand(&[_][]const u8{ "HGET", "user:1", "name" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$5\r\nAlice\r\n", response);
    }

    // Update age
    {
        const response = try client.sendCommand(&[_][]const u8{ "HSET", "user:1", "age", "31" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }

    // Verify updated age
    {
        const response = try client.sendCommand(&[_][]const u8{ "HGET", "user:1", "age" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("$2\r\n31\r\n", response);
    }

    // Get all fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "HKEYS", "user:1" });
        defer testing.allocator.free(response);
        try testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
    }

    // Check field count
    {
        const response = try client.sendCommand(&[_][]const u8{ "HLEN", "user:1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":3\r\n", response);
    }

    // Delete email field
    {
        const response = try client.sendCommand(&[_][]const u8{ "HDEL", "user:1", "email" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":1\r\n", response);
    }

    // Verify field count decreased
    {
        const response = try client.sendCommand(&[_][]const u8{ "HLEN", "user:1" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":2\r\n", response);
    }

    // Email field should not exist
    {
        const response = try client.sendCommand(&[_][]const u8{ "HEXISTS", "user:1", "email" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings(":0\r\n", response);
    }
}
