const std = @import("std");
const testing = std.testing;
const net = std.net;

// RESP Protocol helpers for integration testing
pub const RespClient = struct {
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

    /// Send a command and parse integer response
    pub fn sendCommandExpectInteger(self: *RespClient, args: []const []const u8) !i64 {
        const response = try self.sendCommand(args);
        defer self.allocator.free(response);

        // Parse integer response (format: ":number\r\n")
        if (response[0] != ':') {
            std.debug.print("Unexpected response: {s}\n", .{response});
            return error.UnexpectedResponse;
        }

        return std.fmt.parseInt(i64, response[1 .. response.len - 2], 10);
    }

    /// Send a command and parse bulk string response
    pub fn sendCommandExpectBulkString(self: *RespClient, args: []const []const u8) !?[]const u8 {
        const response = try self.sendCommand(args);
        defer self.allocator.free(response);

        // Parse bulk string response
        // Format: $<length>\r\n<data>\r\n or $-1\r\n (nil)
        if (response[0] == '$') {
            if (response[1] == '-' and response[2] == '1') {
                return null; // Nil response
            }

            // Find the length
            var i: usize = 1;
            while (i < response.len and response[i] != '\r') : (i += 1) {}
            const len = try std.fmt.parseInt(usize, response[1..i], 10);

            // Return the bulk string (must be freed by caller)
            return try self.allocator.dupe(u8, response[i + 2 .. i + 2 + len]);
        }

        return error.UnexpectedResponse;
    }

    /// Send a command and parse simple string response
    pub fn sendCommandExpectSimpleString(self: *RespClient, args: []const []const u8) ![]const u8 {
        const response = try self.sendCommand(args);
        defer self.allocator.free(response);

        // Parse simple string response (format: "+OK\r\n")
        if (response[0] != '+') {
            std.debug.print("Unexpected response: {s}\n", .{response});
            return error.UnexpectedResponse;
        }

        return try self.allocator.dupe(u8, response[1 .. response.len - 2]);
    }

    /// Send a command and expect an error response
    pub fn sendCommandExpectError(self: *RespClient, args: []const []const u8) ![]const u8 {
        const response = try self.sendCommand(args);
        defer self.allocator.free(response);

        // Parse error response (format: "-ERR message\r\n")
        if (response[0] != '-') {
            std.debug.print("Expected error, got: {s}\n", .{response});
            return error.UnexpectedResponse;
        }

        return try self.allocator.dupe(u8, response[1 .. response.len - 2]);
    }
};

// Helper to start server in background
pub const TestServer = struct {
    process: std.process.Child,
    allocator: std.mem.Allocator,

    pub fn start(allocator: std.mem.Allocator) !TestServer {
        // Clean up persistence files so each test starts with fresh state
        std.fs.cwd().deleteFile("dump.rdb") catch {};
        std.fs.cwd().deleteFile("appendonly.aof") catch {};

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
        // Clean up persistence files after test
        std.fs.cwd().deleteFile("dump.rdb") catch {};
        std.fs.cwd().deleteFile("appendonly.aof") catch {};
    }
};

// ============================================================================
// DELEX Command Tests
// ============================================================================

test "DELEX - unconditional delete returns 1 when key exists" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "testkey", "testvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Unconditionally delete
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "testkey" });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "testkey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX - unconditional delete returns 0 when key doesn't exist" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to delete non-existent key
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "nonexistentkey" });
    try testing.expectEqual(@as(i64, 0), del_result);
}

test "DELEX IFEQ - deletes when value matches" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with matching IFEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFEQ", "myvalue" });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX IFEQ - does not delete when value doesn't match" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Try to delete with non-matching IFEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFEQ", "wrongvalue" });
    try testing.expectEqual(@as(i64, 0), del_result);

    // Verify key still exists
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    const value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "mykey" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("myvalue", v);
    }
}

test "DELEX IFEQ - returns 0 when key doesn't exist" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to delete non-existent key with IFEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "nonexistentkey", "IFEQ", "somevalue" });
    try testing.expectEqual(@as(i64, 0), del_result);
}

test "DELEX IFNE - deletes when value doesn't match" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with IFNE when value doesn't match
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFNE", "differentvalue" });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX IFNE - does not delete when value matches" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Try to delete with matching IFNE (should not delete)
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFNE", "myvalue" });
    try testing.expectEqual(@as(i64, 0), del_result);

    // Verify key still exists
    const value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "mykey" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("myvalue", v);
    }
}

test "DELEX IFDEQ - deletes when digest matches" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "testvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Calculate the expected digest using Wyhash
    const expected_digest = std.hash.Wyhash.hash(0, "testvalue");
    var digest_buf: [32]u8 = undefined;
    const digest_str = try std.fmt.bufPrint(&digest_buf, "{x:0>16}", .{expected_digest});

    // Delete with matching IFDEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFDEQ", digest_str });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX IFDEQ - does not delete when digest doesn't match" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "testvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Use a wrong digest
    const wrong_digest = "0000000000000000";

    // Try to delete with non-matching IFDEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFDEQ", wrong_digest });
    try testing.expectEqual(@as(i64, 0), del_result);

    // Verify key still exists
    const value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "mykey" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("testvalue", v);
    }
}

test "DELEX IFDNE - deletes when digest doesn't match" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "testvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Use a wrong digest for IFDNE
    const wrong_digest = "0000000000000000";

    // Delete with non-matching IFDNE
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFDNE", wrong_digest });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX IFDNE - does not delete when digest matches" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "testvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Calculate the expected digest
    const expected_digest = std.hash.Wyhash.hash(0, "testvalue");
    var digest_buf: [32]u8 = undefined;
    const digest_str = try std.fmt.bufPrint(&digest_buf, "{x:0>16}", .{expected_digest});

    // Try to delete with matching IFDNE (should not delete)
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFDNE", digest_str });
    try testing.expectEqual(@as(i64, 0), del_result);

    // Verify key still exists
    const value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "mykey" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("testvalue", v);
    }
}

test "DELEX - wrong arity with just command" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // DELEX with no arguments should fail
    const err = try client.sendCommandExpectError(&[_][]const u8{"DELEX"});
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "wrong number"));
}

test "DELEX - multiple conditions error" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value first
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);

    // Try to specify multiple conditions
    const err = try client.sendCommandExpectError(&[_][]const u8{ "DELEX", "mykey", "IFEQ", "myvalue", "IFNE", "other" });
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "multiple conditions"));
}

test "DELEX IFEQ - missing value argument" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // IFEQ without value should fail
    const err = try client.sendCommandExpectError(&[_][]const u8{ "DELEX", "mykey", "IFEQ" });
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "requires a value"));
}

test "DELEX IFNE - missing value argument" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // IFNE without value should fail
    const err = try client.sendCommandExpectError(&[_][]const u8{ "DELEX", "mykey", "IFNE" });
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "requires a value"));
}

test "DELEX IFDEQ - missing digest argument" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // IFDEQ without digest should fail
    const err = try client.sendCommandExpectError(&[_][]const u8{ "DELEX", "mykey", "IFDEQ" });
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "requires a digest"));
}

test "DELEX IFDNE - missing digest argument" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // IFDNE without digest should fail
    const err = try client.sendCommandExpectError(&[_][]const u8{ "DELEX", "mykey", "IFDNE" });
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "requires a digest"));
}

test "DELEX - empty string value with IFEQ" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set an empty string value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "emptykey", "" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with matching empty string IFEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "emptykey", "IFEQ", "" });
    try testing.expectEqual(@as(i64, 1), del_result);
}

test "DELEX - case-sensitive IFEQ comparison" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value with mixed case
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "MyValue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Try with different case (should not match)
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFEQ", "myvalue" });
    try testing.expectEqual(@as(i64, 0), del_result);

    // Verify key still exists
    const value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "mykey" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("MyValue", v);
    }
}

test "DELEX - long string value with IFEQ" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create a long string
    const long_value = "The quick brown fox jumps over the lazy dog. " ** 10;

    // Set the long value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "longkey", long_value });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with matching IFEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "longkey", "IFEQ", long_value });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "longkey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX - numeric string value with IFEQ" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a numeric value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "numkey", "42" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with matching numeric value
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "numkey", "IFEQ", "42" });
    try testing.expectEqual(@as(i64, 1), del_result);
}

test "DELEX - special characters in value with IFEQ" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value with special characters
    const special_value = "!@#$%^&*()_+-=[]{}|;:',.<>?/";
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "specialkey", special_value });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with matching special characters value
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "specialkey", "IFEQ", special_value });
    try testing.expectEqual(@as(i64, 1), del_result);
}

test "DELEX - unknown option" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value first
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);

    // Try with unknown option
    const err = try client.sendCommandExpectError(&[_][]const u8{ "DELEX", "mykey", "UNKNOWN", "value" });
    defer testing.allocator.free(err);
    try testing.expect(std.mem.containsAtLeast(u8, err, 1, "unknown option"));
}

test "DELEX - case-insensitive flag names" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "myvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Try with lowercase flag (should work because Redis commands are case-insensitive)
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "ifeq", "myvalue" });
    try testing.expectEqual(@as(i64, 1), del_result);
}

test "DELEX - sequential operations" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set first key
    var set_response = try client.sendCommand(&[_][]const u8{ "SET", "key1", "value1" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Set second key
    set_response = try client.sendCommand(&[_][]const u8{ "SET", "key2", "value2" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete first key with condition
    var del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "key1", "IFEQ", "value1" });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Second key should still exist
    var value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "key2" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("value2", v);
    }

    // Delete second key with wrong condition
    del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "key2", "IFEQ", "wrongvalue" });
    try testing.expectEqual(@as(i64, 0), del_result);

    // Second key should still exist
    value = try client.sendCommandExpectBulkString(&[_][]const u8{ "GET", "key2" });
    if (value) |v| {
        defer testing.allocator.free(v);
        try testing.expectEqualStrings("value2", v);
    }
}

test "DELEX - digest consistency across multiple operations" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "consistentvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Calculate digest
    const digest = std.hash.Wyhash.hash(0, "consistentvalue");
    var digest_buf: [32]u8 = undefined;
    const digest_str = try std.fmt.bufPrint(&digest_buf, "{x:0>16}", .{digest});

    // First deletion attempt with matching digest should succeed
    var del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFDEQ", digest_str });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    const get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);
}

test "DELEX - overwrites and conditional delete" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set initial value
    var set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "oldvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Try to delete with wrong condition (should fail)
    var del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFEQ", "oldvalue" });
    try testing.expectEqual(@as(i64, 1), del_result);

    // Verify key is gone
    var get_response = try client.sendCommand(&[_][]const u8{ "GET", "mykey" });
    defer testing.allocator.free(get_response);
    try testing.expectEqualStrings("$-1\r\n", get_response);

    // Set new value
    set_response = try client.sendCommand(&[_][]const u8{ "SET", "mykey", "newvalue" });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with new value condition
    del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "mykey", "IFEQ", "newvalue" });
    try testing.expectEqual(@as(i64, 1), del_result);
}

test "DELEX IFNE - returns 0 when key doesn't exist" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to delete non-existent key with IFNE
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "nonexistentkey", "IFNE", "somevalue" });
    try testing.expectEqual(@as(i64, 0), del_result);
}

test "DELEX IFDEQ - returns 0 when key doesn't exist" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to delete non-existent key with IFDEQ
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "nonexistentkey", "IFDEQ", "somedigest" });
    try testing.expectEqual(@as(i64, 0), del_result);
}

test "DELEX IFDNE - returns 0 when key doesn't exist" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to delete non-existent key with IFDNE
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "nonexistentkey", "IFDNE", "somedigest" });
    try testing.expectEqual(@as(i64, 0), del_result);
}

test "DELEX - newline in value with IFEQ" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set a value with newline
    const value_with_newline = "line1\nline2\nline3";
    const set_response = try client.sendCommand(&[_][]const u8{ "SET", "multilinekey", value_with_newline });
    defer testing.allocator.free(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Delete with matching value containing newlines
    const del_result = try client.sendCommandExpectInteger(&[_][]const u8{ "DELEX", "multilinekey", "IFEQ", value_with_newline });
    try testing.expectEqual(@as(i64, 1), del_result);
}
