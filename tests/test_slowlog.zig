const std = @import("std");
const Server = @import("../src/server.zig").Server;
const Parser = @import("../src/protocol/parser.zig").Parser;

/// Helper to send a command and get response
fn sendCommand(
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    command: []const u8,
) ![]const u8 {
    // Send command
    _ = try stream.write(command);

    // Read response
    var buf: [8192]u8 = undefined;
    const n = try stream.read(&buf);

    return try allocator.dupe(u8, buf[0..n]);
}

/// Helper to extract integer from RESP response
fn extractInt(response: []const u8) !i64 {
    if (response.len < 2 or response[0] != ':') {
        return error.InvalidResponse;
    }
    const end = std.mem.indexOf(u8, response[1..], "\r\n") orelse return error.InvalidResponse;
    return try std.fmt.parseInt(i64, response[1..][0..end], 10);
}

test "SLOWLOG basic commands" {
    const allocator = std.testing.allocator;

    // Start server
    const port: u16 = 6380;
    var server = try Server.init(allocator, port, "127.0.0.1");
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.run, .{&server});
    defer {
        server.stop();
        server_thread.join();
    }

    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Connect client
    const addr = try std.net.Address.parseIp("127.0.0.1", port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    // Initial state: SLOWLOG LEN should be 0
    {
        const response = try sendCommand(allocator, stream, "*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
        defer allocator.free(response);

        const len = try extractInt(response);
        try std.testing.expectEqual(@as(i64, 0), len);
    }

    // SLOWLOG GET should return empty array
    {
        const response = try sendCommand(allocator, stream, "*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n");
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "*0\r\n"));
    }

    // SLOWLOG RESET should return OK
    {
        const response = try sendCommand(allocator, stream, "*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n");
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK\r\n"));
    }
}

test "SLOWLOG GET with count parameter" {
    const allocator = std.testing.allocator;

    const port: u16 = 6381;
    var server = try Server.init(allocator, port, "127.0.0.1");
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.run, .{&server});
    defer {
        server.stop();
        server_thread.join();
    }

    std.Thread.sleep(100 * std.time.ns_per_ms);

    const addr = try std.net.Address.parseIp("127.0.0.1", port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    // SLOWLOG GET 10 should work
    {
        const response = try sendCommand(allocator, stream, "*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$2\r\n10\r\n");
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "*0\r\n"));
    }

    // SLOWLOG GET with invalid count should return error
    {
        const response = try sendCommand(allocator, stream, "*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$3\r\nabc\r\n");
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    }
}

test "SLOWLOG HELP command" {
    const allocator = std.testing.allocator;

    const port: u16 = 6382;
    var server = try Server.init(allocator, port, "127.0.0.1");
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.run, .{&server});
    defer {
        server.stop();
        server_thread.join();
    }

    std.Thread.sleep(100 * std.time.ns_per_ms);

    const addr = try std.net.Address.parseIp("127.0.0.1", port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    const response = try sendCommand(allocator, stream, "*2\r\n$7\r\nSLOWLOG\r\n$4\r\nHELP\r\n");
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "*"));
    try std.testing.expect(std.mem.indexOf(u8, response, "SLOWLOG GET") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "SLOWLOG LEN") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "SLOWLOG RESET") != null);
}

test "SLOWLOG unknown subcommand" {
    const allocator = std.testing.allocator;

    const port: u16 = 6383;
    var server = try Server.init(allocator, port, "127.0.0.1");
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.run, .{&server});
    defer {
        server.stop();
        server_thread.join();
    }

    std.Thread.sleep(100 * std.time.ns_per_ms);

    const addr = try std.net.Address.parseIp("127.0.0.1", port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    const response = try sendCommand(allocator, stream, "*2\r\n$7\r\nSLOWLOG\r\n$7\r\nINVALID\r\n");
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "unknown subcommand") != null);
}

test "SLOWLOG with no arguments" {
    const allocator = std.testing.allocator;

    const port: u16 = 6384;
    var server = try Server.init(allocator, port, "127.0.0.1");
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.run, .{&server});
    defer {
        server.stop();
        server_thread.join();
    }

    std.Thread.sleep(100 * std.time.ns_per_ms);

    const addr = try std.net.Address.parseIp("127.0.0.1", port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    const response = try sendCommand(allocator, stream, "*1\r\n$7\r\nSLOWLOG\r\n");
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}
