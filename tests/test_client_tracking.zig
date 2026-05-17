const std = @import("std");
const net = std.net;

/// Helper to send a raw RESP command and receive response
fn sendCommand(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    _ = try stream.writeAll(cmd);
    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    return try allocator.dupe(u8, buf[0..n]);
}

/// Helper to build a RESP array command from string arguments
fn buildCommand(allocator: std.mem.Allocator, args: []const []const u8) ![]u8 {
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    // Write array header: *N\r\n
    try buffer.writer().print("*{d}\r\n", .{args.len});

    // Write each argument as bulk string: $len\r\ndata\r\n
    for (args) |arg| {
        try buffer.writer().print("${d}\r\n{s}\r\n", .{ arg.len, arg });
    }

    return buffer.toOwnedSlice();
}

/// Helper to get client ID from current connection
fn getClientId(allocator: std.mem.Allocator, stream: net.Stream) !i64 {
    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "ID" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    // Parse integer response ":123\r\n"
    if (!std.mem.startsWith(u8, response, ":")) {
        return error.UnexpectedResponse;
    }
    const num_str = response[1 .. std.mem.indexOf(u8, response, "\r\n") orelse return error.ParseError];
    return try std.fmt.parseInt(i64, num_str, 10);
}

// ============================================================================
// CLIENT TRACKING - Basic Functionality Tests
// ============================================================================

test "CLIENT TRACKING ON - enables tracking" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING OFF - disables tracking" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First enable tracking
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const resp = try sendCommand(allocator, stream, cmd);
        defer allocator.free(resp);
    }

    // Then disable it
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "OFF" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }
}

test "CLIENT TRACKING - case insensitive" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "client", "tracking", "on" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - missing ON/OFF argument returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{"CLIENT", "TRACKING"});
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT TRACKING - invalid ON/OFF value returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "INVALID" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

// ============================================================================
// CLIENT TRACKING - REDIRECT Option Tests
// ============================================================================

test "CLIENT TRACKING - REDIRECT to same client" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const my_id = try getClientId(allocator, stream);

    const id_str = try std.fmt.allocPrint(allocator, "{d}", .{my_id});
    defer allocator.free(id_str);

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT", id_str });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - REDIRECT with invalid client ID returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT", "999999" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "invalid") != null or std.mem.indexOf(u8, response, "client") != null);
}

test "CLIENT TRACKING - REDIRECT with non-numeric value returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT", "not-a-number" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT TRACKING - REDIRECT without client ID returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

// ============================================================================
// CLIENT TRACKING - PREFIX Option Tests
// ============================================================================

test "CLIENT TRACKING - PREFIX with single prefix" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX", "user:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - PREFIX with multiple prefixes" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX", "user:", "PREFIX", "session:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - PREFIX without value returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

// ============================================================================
// CLIENT TRACKING - BCAST Mode Tests
// ============================================================================

test "CLIENT TRACKING - BCAST mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "BCAST" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - BCAST with PREFIX" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "cache:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - OPTIN Mode Tests
// ============================================================================

test "CLIENT TRACKING - OPTIN mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTIN" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - OPTIN with PREFIX" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTIN", "PREFIX", "opt:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - OPTOUT Mode Tests
// ============================================================================

test "CLIENT TRACKING - OPTOUT mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTOUT" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - OPTOUT with PREFIX" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTOUT", "PREFIX", "notrack:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - NOLOOP Flag Tests
// ============================================================================

test "CLIENT TRACKING - NOLOOP flag" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - NOLOOP with other options" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "BCAST", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - Complex Option Combinations Tests
// ============================================================================

test "CLIENT TRACKING - BCAST + PREFIX + NOLOOP" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "app:", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - OPTIN + NOLOOP" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTIN", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - OPTOUT + NOLOOP" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTOUT", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - REDIRECT + PREFIX + NOLOOP" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const my_id = try getClientId(allocator, stream);
    const id_str = try std.fmt.allocPrint(allocator, "{d}", .{my_id});
    defer allocator.free(id_str);

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT", id_str, "PREFIX", "redir:", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - Multiple PREFIX options" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX", "user:", "PREFIX", "session:", "PREFIX", "cache:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - OPTIN vs OPTOUT Conflict Tests
// ============================================================================

test "CLIENT TRACKING - OPTIN and OPTOUT together returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

// ============================================================================
// CLIENT TRACKING - Option Order Independence Tests
// ============================================================================

test "CLIENT TRACKING - options in different order (NOLOOP first)" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "NOLOOP", "BCAST", "PREFIX", "key:" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - options in different order (PREFIX first)" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX", "app:", "BCAST", "NOLOOP" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - Idempotency Tests
// ============================================================================

test "CLIENT TRACKING - multiple ON commands" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First ON
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // Second ON (should succeed, idempotent)
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }
}

test "CLIENT TRACKING - ON then OFF then ON" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // ON
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // OFF
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "OFF" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // ON again
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }
}

test "CLIENT TRACKING - multiple OFF commands" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First OFF
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "OFF" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // Second OFF (should succeed, idempotent)
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "OFF" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }
}

// ============================================================================
// CLIENT TRACKING - Edge Cases Tests
// ============================================================================

test "CLIENT TRACKING - empty prefix string" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX", "" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    // Empty prefix should be accepted
    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - very long prefix" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Create a long prefix
    const long_prefix = try allocator.alloc(u8, 256);
    defer allocator.free(long_prefix);
    @memset(long_prefix, 'a');

    const args = [_][]const u8{ "CLIENT", "TRACKING", "ON", "PREFIX", long_prefix };
    const cmd = try buildCommand(allocator, args[0..]);
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT TRACKING - negative redirect ID" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT", "-1" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    // Negative redirect should be an error (invalid client ID)
    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT TRACKING - zero redirect ID" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "REDIRECT", "0" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    // Zero redirect should be valid (means self)
    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ============================================================================
// CLIENT TRACKING - Integration with other commands
// ============================================================================

test "CLIENT TRACKING ON then get client info" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Enable tracking
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // Get client list
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "LIST" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);

        // Should contain bulk string with client info
        try std.testing.expect(std.mem.startsWith(u8, response, "$"));
        // Response should contain "addr=" (all clients have addresses)
        try std.testing.expect(std.mem.indexOf(u8, response, "addr=") != null);
    }
}

test "CLIENT TRACKING OFF then verify tracking is off" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Enable tracking
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
    }

    // Disable tracking
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "OFF" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // Should still be able to execute other commands
    {
        const cmd = try buildCommand(allocator, &[_][]const u8{ "PING" });
        defer allocator.free(cmd);
        const response = try sendCommand(allocator, stream, cmd);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+PONG\r\n", response);
    }
}

// ============================================================================
// CLIENT TRACKING - Syntax Error Cases
// ============================================================================

test "CLIENT TRACKING - unrecognized option returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    const cmd = try buildCommand(allocator, &[_][]const u8{ "CLIENT", "TRACKING", "ON", "UNKNOWN" });
    defer allocator.free(cmd);

    const response = try sendCommand(allocator, stream, cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT TRACKING - invalid command format" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send raw bytes that form an invalid command
    const raw_cmd = "*2\r\n$6\r\nCLIENT\r\n$10\r\nTRACKINGXX\r\n";
    const response = try sendCommand(allocator, stream, raw_cmd);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-"));
}
