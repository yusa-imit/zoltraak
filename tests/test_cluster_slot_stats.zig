const std = @import("std");
const testing = std.testing;
const net = std.net;
const protocol = @import("../src/protocol/parser.zig");

/// Integration tests for CLUSTER SLOT-STATS command (Redis 8.2)
/// Tests verify RESP protocol behavior end-to-end

test "CLUSTER SLOT-STATS SLOTSRANGE - basic range returns array of arrays" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First, assign slots 0-10 to current node
    const addslots_cmd = "*13\r\n$7\r\nCLUSTER\r\n$8\r\nADDSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n$1\r\n8\r\n$1\r\n9\r\n$2\r\n10\r\n";
    try stream.writeAll(addslots_cmd);
    var buf_addslots: [1024]u8 = undefined;
    _ = try stream.read(&buf_addslots);

    // Add some keys to different slots
    const set1_cmd = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";
    try stream.writeAll(set1_cmd);
    var buf_set1: [1024]u8 = undefined;
    _ = try stream.read(&buf_set1);

    const set2_cmd = "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n";
    try stream.writeAll(set2_cmd);
    var buf_set2: [1024]u8 = undefined;
    _ = try stream.read(&buf_set2);

    // Send CLUSTER SLOT-STATS SLOTSRANGE 0 10
    const cmd = "*5\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$10\r\nSLOTSRANGE\r\n$1\r\n0\r\n$2\r\n10\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return array of 11 elements (slots 0-10)
    // Format: *11\r\n*6\r\n:0\r\n:<key_count>\r\n:0\r\n:0\r\n:0\r\n:0\r\n...
    try testing.expect(std.mem.startsWith(u8, response, "*11\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "*6\r\n") != null); // Each slot has 6 metrics

    // Cleanup
    const delslots_cmd = "*13\r\n$7\r\nCLUSTER\r\n$8\r\nDELSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n$1\r\n8\r\n$1\r\n9\r\n$2\r\n10\r\n";
    try stream.writeAll(delslots_cmd);
    var buf_cleanup: [1024]u8 = undefined;
    _ = try stream.read(&buf_cleanup);
}

test "CLUSTER SLOT-STATS SLOTSRANGE - invalid start slot" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SLOT-STATS SLOTSRANGE with invalid start slot (>= 16384)
    const cmd = "*5\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$10\r\nSLOTSRANGE\r\n$5\r\n16384\r\n$5\r\n16385\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "Slot 16384 is out of range"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR Slot 16384 is out of range"));
}

test "CLUSTER SLOT-STATS SLOTSRANGE - invalid end slot" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SLOT-STATS SLOTSRANGE with invalid end slot
    const cmd = "*5\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$10\r\nSLOTSRANGE\r\n$1\r\n0\r\n$5\r\n20000\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "Slot 20000 is out of range"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR Slot 20000 is out of range"));
}

test "CLUSTER SLOT-STATS SLOTSRANGE - start > end" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SLOT-STATS SLOTSRANGE with start > end
    const cmd = "*5\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$10\r\nSLOTSRANGE\r\n$3\r\n100\r\n$2\r\n50\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "start slot number 100 is greater than end slot number 50"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR start slot number 100 is greater than end slot number 50"));
}

test "CLUSTER SLOT-STATS ORDERBY - key-count ASC" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First, assign some slots
    const addslots_cmd = "*5\r\n$7\r\nCLUSTER\r\n$8\r\nADDSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n";
    try stream.writeAll(addslots_cmd);
    var buf_addslots: [1024]u8 = undefined;
    _ = try stream.read(&buf_addslots);

    // Send CLUSTER SLOT-STATS ORDERBY KEY-COUNT ASC
    const cmd = "*5\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$7\r\nORDERBY\r\n$9\r\nKEY-COUNT\r\n$3\r\nASC\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return array (slots assigned to this node)
    try testing.expect(std.mem.startsWith(u8, response, "*"));
    try testing.expect(std.mem.indexOf(u8, response, "*6\r\n") != null); // Each slot has 6 metrics

    // Cleanup
    const delslots_cmd = "*5\r\n$7\r\nCLUSTER\r\n$8\r\nDELSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n";
    try stream.writeAll(delslots_cmd);
    var buf_cleanup: [1024]u8 = undefined;
    _ = try stream.read(&buf_cleanup);
}

test "CLUSTER SLOT-STATS ORDERBY - key-count DESC with LIMIT" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First, assign some slots
    const addslots_cmd = "*7\r\n$7\r\nCLUSTER\r\n$8\r\nADDSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n";
    try stream.writeAll(addslots_cmd);
    var buf_addslots: [1024]u8 = undefined;
    _ = try stream.read(&buf_addslots);

    // Send CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 3 DESC
    const cmd = "*7\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$7\r\nORDERBY\r\n$9\r\nKEY-COUNT\r\n$5\r\nLIMIT\r\n$1\r\n3\r\n$4\r\nDESC\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return array of 3 elements (limited)
    try testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));

    // Cleanup
    const delslots_cmd = "*7\r\n$7\r\nCLUSTER\r\n$8\r\nDELSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n";
    try stream.writeAll(delslots_cmd);
    var buf_cleanup: [1024]u8 = undefined;
    _ = try stream.read(&buf_cleanup);
}

test "CLUSTER SLOT-STATS ORDERBY - invalid metric" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SLOT-STATS ORDERBY with invalid metric
    const cmd = "*4\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$7\r\nORDERBY\r\n$14\r\nINVALID-METRIC\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "unknown metric"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR unknown metric"));
}

test "CLUSTER SLOT-STATS - wrong arity (no subcommand)" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SLOT-STATS without subcommand
    const cmd = "*2\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "wrong number of arguments"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR wrong number of arguments"));
}

test "CLUSTER SLOT-STATS ORDERBY - supports all metric names" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // First, assign a slot
    const addslots_cmd = "*3\r\n$7\r\nCLUSTER\r\n$8\r\nADDSLOTS\r\n$1\r\n0\r\n";
    try stream.writeAll(addslots_cmd);
    var buf_addslots: [1024]u8 = undefined;
    _ = try stream.read(&buf_addslots);

    // Test all valid metrics
    const metrics = [_][]const u8{ "CPU-USEC", "MEMORY-BYTES", "NETWORK-BYTES-IN", "NETWORK-BYTES-OUT" };

    for (metrics) |metric| {
        const metric_len = try std.fmt.allocPrint(allocator, "{d}", .{metric.len});
        defer allocator.free(metric_len);

        const cmd_str = try std.fmt.allocPrint(allocator, "*4\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$7\r\nORDERBY\r\n${s}\r\n{s}\r\n", .{ metric_len, metric });
        defer allocator.free(cmd_str);

        try stream.writeAll(cmd_str);

        // Read response (should be valid array, not error)
        var buf: [4096]u8 = undefined;
        const n = try stream.read(&buf);
        const response = buf[0..n];

        // Should return array, not error
        try testing.expect(std.mem.startsWith(u8, response, "*"));
    }

    // Cleanup
    const delslots_cmd = "*3\r\n$7\r\nCLUSTER\r\n$8\r\nDELSLOTS\r\n$1\r\n0\r\n";
    try stream.writeAll(delslots_cmd);
    var buf_cleanup: [1024]u8 = undefined;
    _ = try stream.read(&buf_cleanup);
}

test "CLUSTER SLOT-STATS ORDERBY - invalid LIMIT value" {
    const allocator = testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send CLUSTER SLOT-STATS ORDERBY with invalid LIMIT
    const cmd = "*6\r\n$7\r\nCLUSTER\r\n$10\r\nSLOT-STATS\r\n$7\r\nORDERBY\r\n$9\r\nKEY-COUNT\r\n$5\r\nLIMIT\r\n$3\r\nABC\r\n";
    try stream.writeAll(cmd);

    // Read response
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    const response = buf[0..n];

    // Should return error with "invalid limit value"
    try testing.expect(std.mem.startsWith(u8, response, "-ERR invalid limit value"));
}
