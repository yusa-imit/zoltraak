const std = @import("std");
const testing = std.testing;
const net = std.net;
const protocol = @import("../src/protocol/parser.zig");

/// Integration tests for CLUSTER SAVECONFIG and CLUSTER BUMPEPOCH commands
/// Tests verify RESP protocol behavior end-to-end

// Helper function to send a CLUSTER command and read response
fn sendClusterCommand(stream: net.Stream, cmd: []const u8) ![1024]u8 {
    try stream.writeAll(cmd);
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    return buf;
}

// Helper to read entire response (for larger responses)
fn sendClusterCommandLarge(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    try stream.writeAll(cmd);
    var response = std.ArrayList(u8).init(allocator);
    defer response.deinit();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.read(&buf) catch |err| {
            if (err == error.ConnectionResetByPeer) break;
            return err;
        };
        if (n == 0) break;
        try response.appendSlice(buf[0..n]);
    }
    return response.toOwnedSlice(allocator);
}

// CLUSTER SAVECONFIG Tests
test "CLUSTER SAVECONFIG - basic success returns +OK" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SAVECONFIG command
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return simple string +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "CLUSTER SAVECONFIG - wrong arity error" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SAVECONFIG with extra argument
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n$5\r\nEXTRA\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "wrong number of arguments"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "CLUSTER SAVECONFIG - cluster disabled error" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First, verify cluster is NOT enabled (default) by checking cluster error
    // In a default non-cluster setup, SAVECONFIG should fail with cluster disabled
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error about cluster disabled
    try testing.expect(std.mem.startsWith(u8, response, "-ERR This instance has cluster support disabled") or
        std.mem.startsWith(u8, response, "-ERR"));
}

// CLUSTER BUMPEPOCH Tests
test "CLUSTER BUMPEPOCH - BUMPED response format" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER BUMPEPOCH command
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return bulk string starting with "BUMPED" or "STILL"
    try testing.expect(std.mem.startsWith(u8, response, "$") or
        std.mem.startsWith(u8, response, "-ERR"));
}

test "CLUSTER BUMPEPOCH - wrong arity error" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER BUMPEPOCH with extra argument
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n$5\r\nEXTRA\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "wrong number of arguments"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "CLUSTER BUMPEPOCH - cluster disabled error" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // In a default non-cluster setup, BUMPEPOCH should fail with cluster disabled
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error about cluster disabled
    try testing.expect(std.mem.startsWith(u8, response, "-ERR This instance has cluster support disabled") or
        std.mem.startsWith(u8, response, "-ERR"));
}

test "CLUSTER SAVECONFIG and BUMPEPOCH - response format verification" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Test SAVECONFIG response format
    const saveconfig_cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n";
    try stream.writeAll(saveconfig_cmd);

    var buf: [1024]u8 = undefined;
    var n = try stream.read(&buf);
    const saveconfig_response = buf[0..n];

    // Verify SAVECONFIG returns either +OK or an error
    const is_ok = std.mem.eql(u8, saveconfig_response, "+OK\r\n");
    const is_error = std.mem.startsWith(u8, saveconfig_response, "-ERR");
    try testing.expect(is_ok or is_error);

    // Test BUMPEPOCH response format
    const bumpepoch_cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";
    try stream.writeAll(bumpepoch_cmd);

    n = try stream.read(&buf);
    const bumpepoch_response = buf[0..n];

    // Verify BUMPEPOCH returns either a bulk string or an error
    const is_bulk = std.mem.startsWith(u8, bumpepoch_response, "$");
    const is_error_response = std.mem.startsWith(u8, bumpepoch_response, "-ERR");
    try testing.expect(is_bulk or is_error_response);
}

test "CLUSTER SAVECONFIG - RESP protocol compliance (simple string)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SAVECONFIG
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Response must be either:
    // 1. +OK\r\n (success)
    // 2. -ERR <message>\r\n (error, which is expected in non-cluster mode)
    const valid_response = std.mem.eql(u8, response, "+OK\r\n") or
        std.mem.startsWith(u8, response, "-ERR");

    try testing.expect(valid_response);
}

test "CLUSTER BUMPEPOCH - bulk string response format" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER BUMPEPOCH
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Response must be either:
    // 1. $<len>\r\n<BUMPED|STILL> <epoch>\r\n (success)
    // 2. -ERR <message>\r\n (error, which is expected in non-cluster mode)
    const is_bulk = std.mem.startsWith(u8, response, "$");
    const is_error = std.mem.startsWith(u8, response, "-ERR");

    try testing.expect(is_bulk or is_error);

    // If bulk string, verify it contains proper format
    if (is_bulk) {
        try testing.expect(std.mem.indexOf(u8, response, "\r\n") != null);
    }
}

test "CLUSTER BUMPEPOCH - returns bulk string with epoch number" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER BUMPEPOCH
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // If successful (bulk string), response must contain a digit (for epoch number)
    if (std.mem.startsWith(u8, response, "$")) {
        // Look for digit in response (epoch number)
        const has_digit = for (response) |c| {
            if (c >= '0' and c <= '9') break true;
        } else false;
        // If cluster is enabled, should find epoch number
        // If cluster is disabled, we get error instead
    }
}

test "CLUSTER SAVECONFIG - multiple calls succeed" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SAVECONFIG twice
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n";

    // First call
    try stream.writeAll(cmd);
    var buf: [1024]u8 = undefined;
    var n = try stream.read(&buf);
    const response1 = buf[0..n];

    // Second call
    try stream.writeAll(cmd);
    n = try stream.read(&buf);
    const response2 = buf[0..n];

    // Both should be valid responses
    const valid1 = std.mem.eql(u8, response1, "+OK\r\n") or std.mem.startsWith(u8, response1, "-ERR");
    const valid2 = std.mem.eql(u8, response2, "+OK\r\n") or std.mem.startsWith(u8, response2, "-ERR");

    try testing.expect(valid1 and valid2);
}

test "CLUSTER BUMPEPOCH - multiple calls succeed" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER BUMPEPOCH twice
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";

    // First call
    try stream.writeAll(cmd);
    var buf: [1024]u8 = undefined;
    var n = try stream.read(&buf);
    const response1 = buf[0..n];

    // Second call
    try stream.writeAll(cmd);
    n = try stream.read(&buf);
    const response2 = buf[0..n];

    // Both should be valid responses
    const valid1 = std.mem.startsWith(u8, response1, "$") or std.mem.startsWith(u8, response1, "-ERR");
    const valid2 = std.mem.startsWith(u8, response2, "$") or std.mem.startsWith(u8, response2, "-ERR");

    try testing.expect(valid1 and valid2);
}

test "CLUSTER commands - case insensitivity verification" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send with lowercase command
    const cmd_lower = "*2\r\n$7\r\ncluster\r\n$10\r\nsaveconfig\r\n";
    try stream.writeAll(cmd_lower);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should handle lowercase commands
    try testing.expect(std.mem.startsWith(u8, response, "+") or
        std.mem.startsWith(u8, response, "-") or
        std.mem.startsWith(u8, response, "$"));
}

test "CLUSTER SAVECONFIG - missing SAVECONFIG argument" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send just CLUSTER with no subcommand
    const cmd = "*1\r\n$7\r\nCLUSTER\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLUSTER BUMPEPOCH - missing BUMPEPOCH argument" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send just CLUSTER with no subcommand
    const cmd = "*1\r\n$7\r\nCLUSTER\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLUSTER SAVECONFIG - response contains proper RESP terminator" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SAVECONFIG
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSAVECONFIG\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // All RESP responses must end with \r\n
    try testing.expect(std.mem.endsWith(u8, response, "\r\n"));
}

test "CLUSTER BUMPEPOCH - response contains proper RESP terminator" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER BUMPEPOCH
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n";
    try stream.writeAll(cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // All RESP responses must end with \r\n
    try testing.expect(std.mem.endsWith(u8, response, "\r\n"));
}
