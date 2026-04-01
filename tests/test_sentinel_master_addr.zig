const std = @import("std");
const net = std.net;

/// Helper to send a RESP command and receive response
fn sendCommand(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    _ = try stream.write(cmd);
    var buf: [8192]u8 = undefined;
    const n = try stream.read(&buf);
    return try allocator.dupe(u8, buf[0..n]);
}

test "SENTINEL MASTER returns error when sentinel disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MASTER mymaster
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$6\r\nMASTER\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should return error when sentinel mode disabled
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL MASTER rejects wrong number of arguments" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL MASTER (no name)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$6\r\nMASTER\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "SENTINEL REPLICAS returns error when sentinel disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL REPLICAS mymaster
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nREPLICAS\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should return error when sentinel mode disabled
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL REPLICAS rejects wrong number of arguments" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL REPLICAS (no name)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$8\r\nREPLICAS\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "SENTINEL GET-MASTER-ADDR-BY-NAME returns error when sentinel disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$24\r\nGET-MASTER-ADDR-BY-NAME\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should return error when sentinel mode disabled
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL GET-MASTER-ADDR-BY-NAME rejects wrong number of arguments" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL GET-MASTER-ADDR-BY-NAME (no name)
    const resp = try sendCommand(allocator, stream, "*2\r\n$8\r\nSENTINEL\r\n$24\r\nGET-MASTER-ADDR-BY-NAME\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "SENTINEL GET-MASTER-ADDR-BY-NAME returns nil for nonexistent master" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL GET-MASTER-ADDR-BY-NAME nonexistent
    // Even when sentinel is disabled, we should get error first
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$24\r\nGET-MASTER-ADDR-BY-NAME\r\n$11\r\nnonexistent\r\n");
    defer allocator.free(resp);

    // Should return error about sentinel mode (or nil if enabled)
    const is_error = std.mem.startsWith(u8, resp, "-ERR");
    const is_nil = std.mem.eql(u8, resp, "$-1\r\n");
    try std.testing.expect(is_error or is_nil);
}

test "SENTINEL MASTER case insensitive subcommand" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL master mymaster (lowercase)
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$6\r\nmaster\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should process command (error about sentinel mode)
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL REPLICAS case insensitive subcommand" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL replicas mymaster (lowercase)
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nreplicas\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should process command (error about sentinel mode)
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}

test "SENTINEL GET-MASTER-ADDR-BY-NAME case insensitive subcommand" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // SENTINEL get-master-addr-by-name mymaster (lowercase)
    const resp = try sendCommand(allocator, stream, "*3\r\n$8\r\nSENTINEL\r\n$24\r\nget-master-addr-by-name\r\n$8\r\nmymaster\r\n");
    defer allocator.free(resp);

    // Should process command (error about sentinel mode)
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "sentinel mode disabled") != null);
}
