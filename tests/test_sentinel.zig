const std = @import("std");
const net = std.net;
const testing = std.testing;

// Helper function to connect to Zoltraak server
fn connectToServer() !net.Stream {
    const addr = try net.Address.parseIp("127.0.0.1", 6379);
    return try net.tcpConnectToAddress(addr);
}

// Helper function to send command and receive response
fn sendCommand(stream: net.Stream, command: []const u8) ![]u8 {
    _ = try stream.write(command);

    var buf: [4096]u8 = undefined;
    const bytes_read = try stream.read(&buf);
    if (bytes_read == 0) return error.ConnectionClosed;

    const allocator = testing.allocator;
    return try allocator.dupe(u8, buf[0..bytes_read]);
}

// ============================================================================
// SENTINEL RESET Integration Tests
// ============================================================================

test "SENTINEL RESET: resets master by exact name" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Monitor a master
    var response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    // Reset by exact name
    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$5\r\nRESET\r\n$8\r\nmymaster\r\n");
    defer allocator.free(response);

    // Should return :1 (integer 1)
    try testing.expectEqualStrings(":1\r\n", response);
}

test "SENTINEL RESET: resets multiple masters with glob pattern" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Monitor multiple masters with "my" prefix
    var response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$9\r\nmymaster1\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$9\r\nmymaster2\r\n$9\r\n127.0.0.2\r\n$4\r\n6380\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$5\r\nother\r\n$9\r\n127.0.0.3\r\n$4\r\n6381\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    // Reset with glob pattern "my*"
    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$5\r\nRESET\r\n$3\r\nmy*\r\n");
    defer allocator.free(response);

    // Should return :2 (integer 2)
    try testing.expectEqualStrings(":2\r\n", response);
}

test "SENTINEL RESET: resets all masters with wildcard" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Monitor multiple masters
    var response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$7\r\nmaster1\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$7\r\nmaster2\r\n$9\r\n127.0.0.2\r\n$4\r\n6380\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$7\r\nmaster3\r\n$9\r\n127.0.0.3\r\n$4\r\n6381\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    // Reset with wildcard "*"
    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$5\r\nRESET\r\n$1\r\n*\r\n");
    defer allocator.free(response);

    // Should return :3 (integer 3)
    try testing.expectEqualStrings(":3\r\n", response);
}

test "SENTINEL RESET: returns 0 for no matches" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Monitor a master
    var response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    // Reset with pattern that doesn't match
    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$5\r\nRESET\r\n$11\r\nnonexistent\r\n");
    defer allocator.free(response);

    // Should return :0 (integer 0)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "SENTINEL RESET: arity error with missing pattern" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // RESET without pattern argument
    const response = try sendCommand(stream, "*2\r\n$8\r\nSENTINEL\r\n$5\r\nRESET\r\n");
    defer allocator.free(response);

    // Should return error (starts with -)
    try testing.expect(response[0] == '-');
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

// ============================================================================
// SENTINEL FAILOVER Integration Tests
// ============================================================================

test "SENTINEL FAILOVER: forces failover for existing master" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Monitor a master
    var response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    // Force failover
    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n$8\r\nmymaster\r\n");
    defer allocator.free(response);

    // Should return +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SENTINEL FAILOVER: returns error for unknown master" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Attempt failover on nonexistent master
    const response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n$11\r\nnonexistent\r\n");
    defer allocator.free(response);

    // Should return error (starts with -)
    try testing.expect(response[0] == '-');
    try testing.expect(std.mem.indexOf(u8, response, "No such master") != null or std.mem.indexOf(u8, response, "not found") != null);
}

test "SENTINEL FAILOVER: can be called multiple times" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // Monitor a master
    var response = try sendCommand(stream, "*5\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$8\r\nmymaster\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n$1\r\n2\r\n");
    defer allocator.free(response);

    // Force failover multiple times
    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n$8\r\nmymaster\r\n");
    defer allocator.free(response);
    try testing.expectEqualStrings("+OK\r\n", response);

    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n$8\r\nmymaster\r\n");
    defer allocator.free(response);
    try testing.expectEqualStrings("+OK\r\n", response);

    response = try sendCommand(stream, "*3\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n$8\r\nmymaster\r\n");
    defer allocator.free(response);
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "SENTINEL FAILOVER: arity error with missing master name" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // FAILOVER without master name argument
    const response = try sendCommand(stream, "*2\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n");
    defer allocator.free(response);

    // Should return error (starts with -)
    try testing.expect(response[0] == '-');
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "SENTINEL FAILOVER: arity error with extra arguments" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Enable sentinel mode
    const config_resp = try sendCommand(stream, "*2\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nsentinel-mode\r\n$3\r\nyes\r\n");
    defer allocator.free(config_resp);

    // FAILOVER with extra argument
    const response = try sendCommand(stream, "*4\r\n$8\r\nSENTINEL\r\n$8\r\nFAILOVER\r\n$8\r\nmymaster\r\n$5\r\nextra\r\n");
    defer allocator.free(response);

    // Should return error (starts with -)
    try testing.expect(response[0] == '-');
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}
