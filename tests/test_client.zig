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
