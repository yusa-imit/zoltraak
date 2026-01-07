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
