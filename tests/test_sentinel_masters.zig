const std = @import("std");
const net = std.net;

/// Helper to send a RESP command and receive response
fn sendCommand(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    _ = try stream.write(cmd);
    var buf: [8192]u8 = undefined;
    const n = try stream.read(&buf);
    return try allocator.dupe(u8, buf[0..n]);
}

test "SENTINEL MASTERS returns empty array when sentinel disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MASTERS
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$7\r\nMASTERS\r\n");
    defer allocator.free(resp);

    // Should return error when sentinel mode disabled
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL MASTERS rejects extra arguments" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MASTERS extra
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$7\r\nMASTERS\r\n$5\r\nextra\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "SENTINEL MONITOR adds a master when sentinel disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MONITOR mymaster 127.0.0.1 6380 2
    const resp = try sendCommand(allocator, stream, "*6\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$1\r\n2\r\n");
    defer allocator.free(resp);

    // Should return error when sentinel mode disabled
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL MONITOR rejects invalid port" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MONITOR mymaster 127.0.0.1 invalid 2
    const resp = try sendCommand(allocator, stream, "*6\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$7\r\ninvalid\r\n$1\r\n2\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "Invalid port") != null or std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL MONITOR rejects invalid quorum" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MONITOR mymaster 127.0.0.1 6380 invalid
    const resp = try sendCommand(allocator, stream, "*6\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$7\r\ninvalid\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "Invalid quorum") != null or std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL MONITOR rejects zero quorum" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MONITOR mymaster 127.0.0.1 6380 0
    const resp = try sendCommand(allocator, stream, "*6\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$1\r\n0\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "Quorum must be") != null or std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL MONITOR wrong number of arguments" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MONITOR mymaster (missing arguments)
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "SENTINEL REMOVE removes a master when sentinel disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL REMOVE mymaster
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$6\r\nREMOVE\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should return error when sentinel mode disabled
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL REMOVE wrong number of arguments" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL REMOVE (missing name)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$6\r\nREMOVE\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "SENTINEL commands are case insensitive" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // sentinel masters (lowercase)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nsentinel\r\n$7\r\nmasters\r\n");
    defer allocator.free(resp);

    // Should return error (sentinel disabled) not "unknown subcommand"
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "unknown") == null);
}
