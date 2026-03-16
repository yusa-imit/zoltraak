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

test "CLIENT KILL by ID kills specific client" {
    const allocator = std.testing.allocator;

    // Open two connections
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream1 = try net.tcpConnectToAddress(address);
    defer stream1.close();

    const stream2 = try net.tcpConnectToAddress(address);
    defer stream2.close();

    // Get client2's ID
    var client2_id: []const u8 = undefined;
    {
        const resp = try sendCommand(allocator, stream2, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n");
        defer allocator.free(resp);
        // Parse ID from ":N\r\n" format
        const id_start = std.mem.indexOf(u8, resp, ":").?;
        const id_end = std.mem.indexOf(u8, resp, "\r").?;
        client2_id = try allocator.dupe(u8, resp[id_start + 1 .. id_end]);
    }
    defer allocator.free(client2_id);

    // From stream1, kill client2 by ID
    var kill_cmd_buf: [256]u8 = undefined;
    const kill_cmd = try std.fmt.bufPrint(&kill_cmd_buf, "*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${d}\r\n{s}\r\n", .{ client2_id.len, client2_id });
    const resp = try sendCommand(allocator, stream1, kill_cmd);
    defer allocator.free(resp);

    // Should return :1\r\n (1 client killed)
    try std.testing.expectEqualStrings(":1\r\n", resp);
}

test "CLIENT KILL by ADDR kills matching client" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream1 = try net.tcpConnectToAddress(address);
    defer stream1.close();

    const stream2 = try net.tcpConnectToAddress(address);
    defer stream2.close();

    // Get stream2's address from CLIENT LIST
    const list_resp = try sendCommand(allocator, stream2, "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n");
    defer allocator.free(list_resp);

    // From stream1, kill all clients of TYPE normal (will kill both by default)
    // Use SKIPME YES to skip caller
    const kill_resp = try sendCommand(allocator, stream1, "*6\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n$6\r\nSKIPME\r\n$3\r\nYES\r\n");
    defer allocator.free(kill_resp);

    // Should kill at least 1 client (stream2)
    try std.testing.expect(std.mem.startsWith(u8, kill_resp, ":"));
}

test "CLIENT KILL SKIPME YES skips caller" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Get own ID
    var own_id: []const u8 = undefined;
    {
        const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n");
        defer allocator.free(resp);
        const id_start = std.mem.indexOf(u8, resp, ":").?;
        const id_end = std.mem.indexOf(u8, resp, "\r").?;
        own_id = try allocator.dupe(u8, resp[id_start + 1 .. id_end]);
    }
    defer allocator.free(own_id);

    // Try to kill self with SKIPME YES
    var kill_cmd_buf: [256]u8 = undefined;
    const kill_cmd = try std.fmt.bufPrint(&kill_cmd_buf, "*6\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${d}\r\n{s}\r\n$6\r\nSKIPME\r\n$3\r\nYES\r\n", .{ own_id.len, own_id });
    const resp = try sendCommand(allocator, stream, kill_cmd);
    defer allocator.free(resp);

    // Should return :0\r\n (0 clients killed - caller skipped)
    try std.testing.expectEqualStrings(":0\r\n", resp);
}

test "CLIENT KILL SKIPME NO includes caller" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Get own ID
    var own_id: []const u8 = undefined;
    {
        const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n");
        defer allocator.free(resp);
        const id_start = std.mem.indexOf(u8, resp, ":").?;
        const id_end = std.mem.indexOf(u8, resp, "\r").?;
        own_id = try allocator.dupe(u8, resp[id_start + 1 .. id_end]);
    }
    defer allocator.free(own_id);

    // Try to kill self with SKIPME NO
    var kill_cmd_buf: [256]u8 = undefined;
    const kill_cmd = try std.fmt.bufPrint(&kill_cmd_buf, "*6\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${d}\r\n{s}\r\n$6\r\nSKIPME\r\n$2\r\nNO\r\n", .{ own_id.len, own_id });
    const resp = try sendCommand(allocator, stream, kill_cmd);
    defer allocator.free(resp);

    // Should return :1\r\n (1 client killed - self)
    try std.testing.expectEqualStrings(":1\r\n", resp);
}

test "CLIENT KILL old format kills by addr:port" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream1 = try net.tcpConnectToAddress(address);
    defer stream1.close();

    const stream2 = try net.tcpConnectToAddress(address);
    defer stream2.close();

    // Get stream2's address from CLIENT LIST
    const list_resp = try sendCommand(allocator, stream2, "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n");
    defer allocator.free(list_resp);

    // Extract addr=<address> from list
    const addr_start = std.mem.indexOf(u8, list_resp, "addr=").?;
    const addr_value_start = addr_start + 5; // "addr=".len
    const addr_end = std.mem.indexOfPos(u8, list_resp, addr_value_start, " ").?;
    const target_addr = list_resp[addr_value_start..addr_end];

    // From stream1, kill stream2 using old format CLIENT KILL addr:port
    var kill_cmd_buf: [256]u8 = undefined;
    const kill_cmd = try std.fmt.bufPrint(&kill_cmd_buf, "*3\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n${d}\r\n{s}\r\n", .{ target_addr.len, target_addr });
    const resp = try sendCommand(allocator, stream1, kill_cmd);
    defer allocator.free(resp);

    // Should return +OK\r\n
    try std.testing.expectEqualStrings("+OK\r\n", resp);
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

test "CLIENT PAUSE command - WRITE mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT PAUSE 1000 WRITE (pause write commands for 1 second)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$4\r\n1000\r\n$5\r\nWRITE\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Read commands should still work (PING)
    const ping_resp = try sendCommand(allocator, stream, "*1\r\n$4\r\nPING\r\n");
    defer allocator.free(ping_resp);
    try std.testing.expectEqualStrings("+PONG\r\n", ping_resp);

    // Unpause immediately
    const unpause_resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n");
    defer allocator.free(unpause_resp);
    try std.testing.expectEqualStrings("+OK\r\n", unpause_resp);
}

test "CLIENT PAUSE command - ALL mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT PAUSE 1000 ALL (pause all commands for 1 second)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$4\r\n1000\r\n$3\r\nALL\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Unpause immediately so we don't block subsequent commands
    const unpause_resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n");
    defer allocator.free(unpause_resp);
    try std.testing.expectEqualStrings("+OK\r\n", unpause_resp);
}

test "CLIENT PAUSE command - default WRITE mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT PAUSE 1000 (should default to WRITE mode)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$4\r\n1000\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Unpause
    const unpause_resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n");
    defer allocator.free(unpause_resp);
    try std.testing.expectEqualStrings("+OK\r\n", unpause_resp);
}

test "CLIENT UNPAUSE command" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Pause clients
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$5\r\n10000\r\n$3\r\nALL\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Unpause
    {
        const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Verify commands work again
    const ping_resp = try sendCommand(allocator, stream, "*1\r\n$4\r\nPING\r\n");
    defer allocator.free(ping_resp);
    try std.testing.expectEqualStrings("+PONG\r\n", ping_resp);
}

test "CLIENT PAUSE command - zero timeout" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT PAUSE 0 (pause expires immediately)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$1\r\n0\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Commands should work immediately
    const ping_resp = try sendCommand(allocator, stream, "*1\r\n$4\r\nPING\r\n");
    defer allocator.free(ping_resp);
    try std.testing.expectEqualStrings("+PONG\r\n", ping_resp);
}

test "CLIENT PAUSE command - negative timeout rejected" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT PAUSE -1 (should be rejected)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$2\r\n-1\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "non-negative") != null);
}

test "CLIENT PAUSE command - invalid mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT PAUSE 1000 INVALID
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$4\r\n1000\r\n$7\r\nINVALID\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "WRITE or ALL") != null);
}

test "CLIENT UNBLOCK command - client not blocked" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT UNBLOCK 9999 (non-existent or non-blocked client)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$4\r\n9999\r\n");
    defer allocator.free(resp);

    // Should return 0 (not found or not blocked)
    try std.testing.expectEqualStrings(":0\r\n", resp);
}

test "CLIENT UNBLOCK command - invalid client ID" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT UNBLOCK not-a-number
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$12\r\nnot-a-number\r\n");
    defer allocator.free(resp);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "invalid client ID") != null);
}

test "CLIENT UNBLOCK command - invalid mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT UNBLOCK 1 INVALID
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$1\r\n1\r\n$7\r\nINVALID\r\n");
    defer allocator.free(resp);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "TIMEOUT or ERROR") != null);
}

test "CLIENT UNBLOCK command - TIMEOUT mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT UNBLOCK 1 TIMEOUT
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$1\r\n1\r\n$7\r\nTIMEOUT\r\n");
    defer allocator.free(resp);

    // Should return 0 or 1 depending on whether client 1 is blocked
    try std.testing.expect(std.mem.startsWith(u8, resp, ":"));
}

test "CLIENT UNBLOCK command - ERROR mode" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT UNBLOCK 1 ERROR
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$1\r\n1\r\n$5\r\nERROR\r\n");
    defer allocator.free(resp);

    // Should return 0 or 1 depending on whether client 1 is blocked
    try std.testing.expect(std.mem.startsWith(u8, resp, ":"));
}

// ────────────────────────────────────────────────────────────────────────────
// CLIENT NO-TOUCH and CLIENT SETINFO Integration Tests
// ────────────────────────────────────────────────────────────────────────────

test "CLIENT NO-TOUCH - enable/disable via integration" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16381;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    // Enable NO-TOUCH
    {
        const cmd = "CLIENT NO-TOUCH ON\r\n";
        _ = try conn.write(cmd);
        const response = try readResponse(allocator, &conn);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // Check status (should be on)
    {
        const cmd = "CLIENT NO-TOUCH\r\n";
        _ = try conn.write(cmd);
        const response = try readResponse(allocator, &conn);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+on\r\n", response);
    }

    // Disable NO-TOUCH
    {
        const cmd = "CLIENT NO-TOUCH OFF\r\n";
        _ = try conn.write(cmd);
        const response = try readResponse(allocator, &conn);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+OK\r\n", response);
    }

    // Check status (should be off)
    {
        const cmd = "CLIENT NO-TOUCH\r\n";
        _ = try conn.write(cmd);
        const response = try readResponse(allocator, &conn);
        defer allocator.free(response);
        try std.testing.expectEqualStrings("+off\r\n", response);
    }
}

test "CLIENT NO-TOUCH - invalid argument" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16382;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    const cmd = "CLIENT NO-TOUCH INVALID\r\n";
    _ = try conn.write(cmd);
    const response = try readResponse(allocator, &conn);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "ON") != null or std.mem.indexOf(u8, response, "OFF") != null);
}

test "CLIENT SETINFO - LIB-NAME" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16383;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    const cmd = "CLIENT SETINFO LIB-NAME redis-py\r\n";
    _ = try conn.write(cmd);
    const response = try readResponse(allocator, &conn);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT SETINFO - LIB-VER" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16384;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    const cmd = "CLIENT SETINFO LIB-VER 4.5.1\r\n";
    _ = try conn.write(cmd);
    const response = try readResponse(allocator, &conn);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT SETINFO - invalid attribute" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16385;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    const cmd = "CLIENT SETINFO INVALID value\r\n";
    _ = try conn.write(cmd);
    const response = try readResponse(allocator, &conn);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "LIB-NAME") != null or std.mem.indexOf(u8, response, "LIB-VER") != null);
}

test "CLIENT SETINFO - value with space (rejected)" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16386;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    const cmd = "CLIENT SETINFO LIB-NAME redis py\r\n";
    _ = try conn.write(cmd);
    const response = try readResponse(allocator, &conn);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "invalid characters") != null);
}

test "CLIENT SETINFO - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const host = "127.0.0.1";
    const port: u16 = 16387;

    var storage = try Storage.init(allocator, port, host);
    defer storage.deinit();

    var server_thread = try startServerThread(allocator, &storage);
    defer stopServerThread(&server_thread, &storage);

    var conn = try connectToServer(allocator, host, port);
    defer conn.close();

    const cmd = "CLIENT SETINFO LIB-NAME\r\n";
    _ = try conn.write(cmd);
    const response = try readResponse(allocator, &conn);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "CLIENT GETREDIR returns -1 when tracking disabled" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CLIENT GETREDIR (tracking disabled by default)
    const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$8\r\nGETREDIR\r\n");
    defer allocator.free(resp);

    // Should return -1
    try std.testing.expectEqualStrings(":-1\r\n", resp);
}

test "CLIENT GETREDIR returns redirect ID when enabled" {
    const allocator = std.testing.allocator;

    // Create two connections
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream1 = try net.tcpConnectToAddress(address);
    defer stream1.close();
    const stream2 = try net.tcpConnectToAddress(address);
    defer stream2.close();

    // Get client ID of stream2 (redirect target)
    const id_resp = try sendCommand(allocator, stream2, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n");
    defer allocator.free(id_resp);

    // Parse the integer ID (format: ":123\r\n")
    const id_str = id_resp[1 .. id_resp.len - 2]; // Strip ":" and "\r\n"
    const redirect_id = try std.fmt.parseInt(u64, id_str, 10);

    // Enable tracking on stream1 with REDIRECT to stream2
    {
        var cmd_buf: [256]u8 = undefined;
        const cmd = try std.fmt.bufPrint(&cmd_buf, "*4\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${d}\r\n{d}\r\n", .{ id_str.len, redirect_id });
        const resp = try sendCommand(allocator, stream1, cmd);
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // CLIENT GETREDIR on stream1
    {
        const resp = try sendCommand(allocator, stream1, "*2\r\n$6\r\nCLIENT\r\n$8\r\nGETREDIR\r\n");
        defer allocator.free(resp);

        // Should return redirect_id
        var expected_buf: [64]u8 = undefined;
        const expected = try std.fmt.bufPrint(&expected_buf, ":{d}\r\n", .{redirect_id});
        try std.testing.expectEqualStrings(expected, resp);
    }
}

test "CLIENT GETREDIR returns -1 when tracking enabled without REDIRECT" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Enable tracking without REDIRECT
    {
        const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // CLIENT GETREDIR (should return -1 since redirect is 0 = self)
    {
        const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCLIENT\r\n$8\r\nGETREDIR\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings(":-1\r\n", resp);
    }
}
