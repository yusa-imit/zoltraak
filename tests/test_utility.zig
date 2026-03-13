const std = @import("std");
const testing = std.testing;
const net = std.net;
const protocol = @import("../src/protocol/parser.zig");

/// Integration tests for utility commands (ECHO, QUIT, TIME, LASTSAVE, MONITOR, DEBUG)

test "ECHO command - returns message" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send ECHO command
    try stream.writeAll("*2\r\n$4\r\nECHO\r\n$11\r\nHello World\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return bulk string
    try testing.expectEqualStrings("$11\r\nHello World\r\n", response);
}

test "ECHO command - wrong number of arguments" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send ECHO command with no argument
    try stream.writeAll("*1\r\n$4\r\nECHO\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "QUIT command - returns OK" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send QUIT command
    try stream.writeAll("*1\r\n$4\r\nQUIT\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "TIME command - returns array of two integers" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send TIME command
    try stream.writeAll("*1\r\n$4\r\nTIME\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return array of 2 elements
    try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));

    // Should contain two bulk strings (seconds and microseconds)
    try testing.expect(std.mem.indexOf(u8, response, "$") != null);
}

test "LASTSAVE command - returns integer timestamp" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send LASTSAVE command
    try stream.writeAll("*1\r\n$8\r\nLASTSAVE\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return integer
    try testing.expect(std.mem.startsWith(u8, response, ":"));
}

test "MONITOR command - returns OK (stub)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send MONITOR command
    try stream.writeAll("*1\r\n$7\r\nMONITOR\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK (stub implementation)
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "DEBUG OBJECT command - shows object info" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Set a key first
    try stream.writeAll("*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$9\r\ntestvalue\r\n");
    var buf: [1024]u8 = undefined;
    _ = try stream.read(&buf);

    // Send DEBUG OBJECT command
    try stream.writeAll("*3\r\n$5\r\nDEBUG\r\n$6\r\nOBJECT\r\n$7\r\ntestkey\r\n");

    // Read response
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return bulk string containing type info
    try testing.expect(std.mem.startsWith(u8, response, "$"));
    try testing.expect(std.mem.indexOf(u8, response, "string") != null);
}

test "DEBUG OBJECT command - key not found" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send DEBUG OBJECT command for non-existent key
    try stream.writeAll("*3\r\n$5\r\nDEBUG\r\n$6\r\nOBJECT\r\n$11\r\nnonexistent\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR no such key"));
}

test "DEBUG HELP command - returns help text" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send DEBUG HELP command
    try stream.writeAll("*2\r\n$5\r\nDEBUG\r\n$4\r\nHELP\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return bulk string with help text
    try testing.expect(std.mem.startsWith(u8, response, "$"));
    try testing.expect(std.mem.indexOf(u8, response, "OBJECT") != null);
}

test "SHUTDOWN command - accepts SAVE modifier" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SHUTDOWN SAVE command (note: won't actually shut down, just acknowledges)
    try stream.writeAll("*2\r\n$8\r\nSHUTDOWN\r\n$4\r\nSAVE\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SHUTDOWN command - accepts NOSAVE modifier" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SHUTDOWN NOSAVE command
    try stream.writeAll("*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SHUTDOWN command - rejects invalid modifier" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SHUTDOWN with invalid modifier
    try stream.writeAll("*2\r\n$8\r\nSHUTDOWN\r\n$7\r\nINVALID\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR syntax error"));
}

test "SELECT command - database 0 is accepted" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SELECT 0 command
    try stream.writeAll("*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SELECT command - other databases are rejected" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SELECT 1 command
    try stream.writeAll("*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR DB index is out of range"));
}

test "SELECT command - negative index rejected" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SELECT -1 command
    try stream.writeAll("*2\r\n$6\r\nSELECT\r\n$2\r\n-1\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR DB index is out of range"));
}

test "SELECT command - invalid argument" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SELECT with non-numeric argument
    try stream.writeAll("*2\r\n$6\r\nSELECT\r\n$3\r\nabc\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR invalid DB index"));
}

test "SELECT command - wrong number of arguments" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SELECT with no argument
    try stream.writeAll("*1\r\n$6\r\nSELECT\r\n");

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "SELECT command - data persists across SELECT 0" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    var buf: [1024]u8 = undefined;

    // Set a key
    try stream.writeAll("*3\r\n$3\r\nSET\r\n$7\r\nselectkey\r\n$10\r\nselectval\r\n");
    _ = try stream.read(&buf);

    // SELECT 0 (should be no-op)
    try stream.writeAll("*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");
    const n1 = try stream.read(&buf);
    try testing.expectEqualStrings("+OK\r\n", buf[0..n1]);

    // Verify key still exists
    try stream.writeAll("*2\r\n$3\r\nGET\r\n$9\r\nselectkey\r\n");
    const n2 = try stream.read(&buf);
    const response = buf[0..n2];
    try testing.expectEqualStrings("$10\r\nselectval\r\n", response);

    // Cleanup
    try stream.writeAll("*2\r\n$3\r\nDEL\r\n$9\r\nselectkey\r\n");
    _ = try stream.read(&buf);
}

// ── DEBUG command integration tests ───────────────────────────────────────────

test "DEBUG SET-ACTIVE-EXPIRE - toggle active expiration" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    var buf: [1024]u8 = undefined;

    // Disable active expiration
    try stream.writeAll("*3\r\n$5\r\nDEBUG\r\n$18\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n0\r\n");
    const n1 = try stream.read(&buf);
    try testing.expectEqualStrings(":0\r\n", buf[0..n1]);

    // Enable active expiration
    try stream.writeAll("*3\r\n$5\r\nDEBUG\r\n$18\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n1\r\n");
    const n2 = try stream.read(&buf);
    try testing.expectEqualStrings(":1\r\n", buf[0..n2]);
}

test "DEBUG SLEEP - sleeps for specified duration" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const start = std.time.milliTimestamp();

    // Sleep for 0.1 seconds (100ms)
    try stream.writeAll("*3\r\n$5\r\nDEBUG\r\n$5\r\nSLEEP\r\n$3\r\n0.1\r\n");

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const elapsed = std.time.milliTimestamp() - start;

    try testing.expectEqualStrings("+OK\r\n", buf[0..n]);
    try testing.expect(elapsed >= 100); // At least 100ms
}

test "DEBUG POPULATE - creates test keys" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    var buf: [1024]u8 = undefined;

    // Populate 5 keys with prefix "dbgtest:" and size 16
    try stream.writeAll("*5\r\n$5\r\nDEBUG\r\n$8\r\nPOPULATE\r\n$1\r\n5\r\n$8\r\ndbgtest:\r\n$2\r\n16\r\n");
    const n1 = try stream.read(&buf);
    try testing.expectEqualStrings("+OK\r\n", buf[0..n1]);

    // Check that key exists
    try stream.writeAll("*2\r\n$3\r\nGET\r\n$9\r\ndbgtest:0\r\n");
    const n2 = try stream.read(&buf);
    const response = buf[0..n2];
    try testing.expect(std.mem.startsWith(u8, response, "$16\r\n")); // Size 16

    // Cleanup
    try stream.writeAll("*6\r\n$3\r\nDEL\r\n$9\r\ndbgtest:0\r\n$9\r\ndbgtest:1\r\n$9\r\ndbgtest:2\r\n$9\r\ndbgtest:3\r\n$9\r\ndbgtest:4\r\n");
    _ = try stream.read(&buf);
}

test "DEBUG CHANGE-REPL-ID - returns OK (stub)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send DEBUG CHANGE-REPL-ID command
    try stream.writeAll("*2\r\n$5\r\nDEBUG\r\n$14\r\nCHANGE-REPL-ID\r\n");

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    try testing.expectEqualStrings("+OK\r\n", buf[0..n]);
}
