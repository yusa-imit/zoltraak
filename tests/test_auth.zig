const std = @import("std");
const net = std.net;
const testing = std.testing;

/// Integration test for AUTH command
/// Tests AUTH command behavior with a real server connection
test "AUTH command integration" {
    const allocator = testing.allocator;

    // Start server in background (assumes server runs on 127.0.0.1:6379)
    // Note: For real integration tests, we'd spawn the server programmatically
    // For now, this test documents the expected behavior

    // Connect to server
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    var stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Test 1: AUTH with default user (nopass) should succeed
    {
        const auth_cmd = "*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r\n";
        _ = try stream.write(auth_cmd);

        var buf: [1024]u8 = undefined;
        const n = try stream.read(&buf);
        const response = buf[0..n];

        // Should return +OK
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Test 2: PING should work after AUTH
    {
        const ping_cmd = "*1\r\n$4\r\nPING\r\n";
        _ = try stream.write(ping_cmd);

        var buf: [1024]u8 = undefined;
        const n = try stream.read(&buf);
        const response = buf[0..n];

        // Should return +PONG
        try testing.expectEqualStrings("+PONG\r\n", response);
    }
}

/// Test AUTH with wrong credentials
test "AUTH command with wrong credentials" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    var stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Note: This test would need a user with a password to be useful
    // For now, it documents the expected error format

    // AUTH with non-existent user
    const auth_cmd = "*3\r\n$4\r\nAUTH\r\n$10\r\nnonexistent\r\n$8\r\nwrongpass\r\n";
    _ = try stream.write(auth_cmd);

    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return -WRONGPASS error
    try testing.expect(std.mem.startsWith(u8, response, "-WRONGPASS"));
}
