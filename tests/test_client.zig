const std = @import("std");
const net = std.net;

/// Helper to send a RESP command and receive response
fn sendCommand(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    _ = try stream.write(cmd);
    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    return try allocator.dupe(u8, buf[0..n]);
}

test "CLIENT ID returns unique connection ID" {
    const allocator = std.testing.allocator;

    // Connect to server
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT ID
    const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n");
    defer allocator.free(resp);

    // Should return integer ID (e.g., ":1\r\n")
    try std.testing.expect(std.mem.startsWith(u8, resp, ":"));
    try std.testing.expect(std.mem.endsWith(u8, resp, "\r\n"));
}

test "CLIENT GETNAME/SETNAME workflow" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT GETNAME (initially null)
    {
        const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("$-1\r\n", resp);
    }

    // CLIENT SETNAME test-client
    {
        const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$11\r\ntest-client\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // CLIENT GETNAME (now returns test-client)
    {
        const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("$11\r\ntest-client\r\n", resp);
    }
}

test "CLIENT SETNAME rejects spaces" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT SETNAME with spaces should fail
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$11\r\ntest client\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "spaces") != null);
}

test "CLIENT LIST shows active connections" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Set client name
    {
        const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$9\r\nmy-client\r\n");
        defer allocator.free(resp);
    }

    // CLIENT LIST
    const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n");
    defer allocator.free(resp);

    // Should be bulk string containing connection info
    try std.testing.expect(std.mem.startsWith(u8, resp, "$"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "name=my-client") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "addr=") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "fd=") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "age=") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "idle=") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "flags=") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "cmd=") != null);
}

test "CLIENT LIST with TYPE normal filter" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT LIST TYPE normal
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n");
    defer allocator.free(resp);

    // Should show normal clients
    try std.testing.expect(std.mem.startsWith(u8, resp, "$"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "flags=N") != null);
}

test "CLIENT command updates last_cmd timestamp" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Execute PING
    {
        const resp = try sendCommand(allocator, stream, "*1\r\n$4\r\nPING\r\n");
        defer allocator.free(resp);
    }

    // CLIENT LIST should show cmd=PING
    const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.indexOf(u8, resp, "cmd=PING") != null);
}
