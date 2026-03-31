const std = @import("std");
const testing = std.testing;
const net = std.net;
const protocol = @import("../src/protocol/parser.zig");

/// Integration tests for CLUSTER SET-CONFIG-EPOCH command
/// Tests verify RESP protocol behavior end-to-end

test "CLUSTER SET-CONFIG-EPOCH - basic success returns +OK" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH with epoch 42
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$2\r\n42\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return simple string +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "CLUSTER SET-CONFIG-EPOCH - wrong arity error (no epoch)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH without epoch argument
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "wrong number of arguments"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "CLUSTER SET-CONFIG-EPOCH - wrong arity error (too many args)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH with extra argument
    const cmd = "*4\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$2\r\n42\r\n$5\r\nEXTRA\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "wrong number of arguments"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "CLUSTER SET-CONFIG-EPOCH - invalid epoch (not a number)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH with invalid epoch
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$3\r\nABC\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "Invalid epoch"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR Invalid epoch"));
}

test "CLUSTER SET-CONFIG-EPOCH - invalid epoch (negative number)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH with negative epoch
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$2\r\n-1\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "Invalid epoch"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR Invalid epoch"));
}

test "CLUSTER SET-CONFIG-EPOCH - error if node has assigned slots" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First, assign a slot to the node
    const addslots_cmd = "*3\r\n$7\r\nCLUSTER\r\n$8\r\nADDSLOTS\r\n$3\r\n100\r\n";
    try stream.writeAll(addslots_cmd);

    // Read response (should be +OK)
    var buf1: [1024]u8 = undefined;
    _ = try stream.read(&buf1);

    // Now try to set config epoch (should fail)
    const setepoch_cmd = "*3\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$2\r\n50\r\n";
    try stream.writeAll(setepoch_cmd);

    // Read response
    var buf2: [1024]u8 = undefined;
    const n = try stream.read(&buf2);
    const response = buf2[0..n];

    // Should return error about not knowing other nodes (Redis's message)
    try testing.expect(std.mem.startsWith(u8, response, "-ERR The user can assign a config epoch only when the node does not know any other node"));

    // Cleanup: remove the slot
    const delslots_cmd = "*3\r\n$7\r\nCLUSTER\r\n$8\r\nDELSLOTS\r\n$3\r\n100\r\n";
    try stream.writeAll(delslots_cmd);
    var buf3: [1024]u8 = undefined;
    _ = try stream.read(&buf3);
}

test "CLUSTER SET-CONFIG-EPOCH - accepts zero epoch" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH with epoch 0
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$1\r\n0\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK (epoch 0 is valid)
    try testing.expectEqualStrings("+OK\r\n", response);
}

test "CLUSTER SET-CONFIG-EPOCH - accepts large epoch value" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SET-CONFIG-EPOCH with large epoch (u64 max - 1)
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$17\r\nSET-CONFIG-EPOCH\r\n$20\r\n18446744073709551614\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return +OK
    try testing.expectEqualStrings("+OK\r\n", response);
}
