const std = @import("std");
const testing = std.testing;
const net = std.net;

/// Integration tests for CLUSTER MYSHARDID command (Redis 7.2)
/// Tests verify RESP protocol behavior end-to-end

test "CLUSTER MYSHARDID - returns 40-char bulk string" {
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER MYSHARDID command
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nMYSHARDID\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return bulk string: $40\r\n<40-char-hex>\r\n
    try testing.expect(response.len >= 47); // $40\r\n + 40 chars + \r\n
    try testing.expect(std.mem.startsWith(u8, response, "$40\r\n"));

    // Extract shard_id (skip "$40\r\n", take 40 chars)
    const shard_id = response[5..45];
    try testing.expectEqual(@as(usize, 40), shard_id.len);

    // Verify all characters are valid lowercase hex (0-9, a-f)
    for (shard_id) |char| {
        const is_valid = (char >= '0' and char <= '9') or (char >= 'a' and char <= 'f');
        try testing.expect(is_valid);
    }

    // Verify ends with \r\n
    try testing.expect(std.mem.endsWith(u8, response[0..n], "\r\n"));
}

test "CLUSTER MYSHARDID - wrong arity error" {
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER MYSHARDID with extra argument
    const cmd = "*3\r\n$7\r\nCLUSTER\r\n$10\r\nMYSHARDID\r\n$5\r\nEXTRA\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "wrong number of arguments"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "CLUSTER MYSHARDID - consistent across calls" {
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER MYSHARDID first time
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nMYSHARDID\r\n";
    try stream.writeAll(cmd);

    var buf1: [1024]u8 = undefined;
    const n1 = try stream.read(&buf1);
    const response1 = buf1[0..n1];
    const shard_id1 = response1[5..45];

    // Send CLUSTER MYSHARDID second time
    try stream.writeAll(cmd);
    var buf2: [1024]u8 = undefined;
    const n2 = try stream.read(&buf2);
    const response2 = buf2[0..n2];
    const shard_id2 = response2[5..45];

    // Shard ID should be consistent across calls
    try testing.expectEqualSlices(u8, shard_id1, shard_id2);
}

test "CLUSTER MYSHARDID - different from CLUSTER MYID" {
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Get node ID via CLUSTER MYID
    const myid_cmd = "*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n";
    try stream.writeAll(myid_cmd);

    var buf1: [1024]u8 = undefined;
    const n1 = try stream.read(&buf1);
    const myid_response = buf1[0..n1];
    const node_id = myid_response[5..45];

    // Get shard ID via CLUSTER MYSHARDID
    const myshardid_cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nMYSHARDID\r\n";
    try stream.writeAll(myshardid_cmd);

    var buf2: [1024]u8 = undefined;
    const n2 = try stream.read(&buf2);
    const myshardid_response = buf2[0..n2];
    const shard_id = myshardid_response[5..45];

    // For a standalone master, node_id and shard_id should be different
    // (they are independently generated random values)
    // Note: In rare cases they COULD be equal (1 in 2^160 chance), but we check != here
    // as it's the expected behavior for independent random generation
    const are_different = !std.mem.eql(u8, node_id, shard_id);
    try testing.expect(are_different);
}
