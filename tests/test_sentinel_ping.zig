const std = @import("std");
const net = std.net;

/// Helper to send a RESP command and receive response
fn sendCommand(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    _ = try stream.write(cmd);
    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    return try allocator.dupe(u8, buf[0..n]);
}

test "SENTINEL PING returns PONG" {
    const allocator = std.testing.allocator;

    // Connect to server
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SENTINEL PING command
    // RESP format: *2\r\n$8\r\nSENTINEL\r\n$4\r\nPING\r\n
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$4\r\nPING\r\n");
    defer allocator.free(resp);

    // Should return +PONG\r\n (simple string)
    try std.testing.expectEqualStrings("+PONG\r\n", resp);
}

test "SENTINEL PING works when sentinel disabled" {
    const allocator = std.testing.allocator;

    // Connect to server (sentinel mode not enabled by default)
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SENTINEL PING command
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$4\r\nPING\r\n");
    defer allocator.free(resp);

    // Should still return +PONG\r\n (graceful fallback)
    // SENTINEL PING is a health check command that works regardless of mode
    try std.testing.expectEqualStrings("+PONG\r\n", resp);
}

test "SENTINEL PING rejects extra arguments" {
    const allocator = std.testing.allocator;

    // Connect to server
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send SENTINEL PING with extra argument (should fail)
    // RESP format: *3\r\n$8\r\nSENTINEL\r\n$4\r\nPING\r\n$5\r\nextra\r\n
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$4\r\nPING\r\n$5\r\nextra\r\n");
    defer allocator.free(resp);

    // Should return error about wrong number of arguments
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null or
        std.mem.indexOf(u8, resp, "syntax error") != null);
}

test "SENTINEL PING case insensitive" {
    const allocator = std.testing.allocator;

    // Connect to server
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send sentinel ping (lowercase)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nsentinel\r\n$4\r\nping\r\n");
    defer allocator.free(resp);

    // Should return +PONG\r\n (commands are case-insensitive)
    try std.testing.expectEqualStrings("+PONG\r\n", resp);
}

test "SENTINEL PING with mixed case" {
    const allocator = std.testing.allocator;

    // Connect to server
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send Sentinel Ping (mixed case)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSentinel\r\n$4\r\nPing\r\n");
    defer allocator.free(resp);

    // Should return +PONG\r\n
    try std.testing.expectEqualStrings("+PONG\r\n", resp);
}
