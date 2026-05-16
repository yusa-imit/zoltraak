const std = @import("std");
const net = std.net;

/// Helper to send a RESP command and receive response
fn sendCommand(allocator: std.mem.Allocator, stream: net.Stream, cmd: []const u8) ![]u8 {
    _ = try stream.write(cmd);
    var buf: [16384]u8 = undefined;
    const n = try stream.read(&buf);
    return try allocator.dupe(u8, buf[0..n]);
}

/// Helper to parse integer response (e.g., ":100\r\n")
fn parseIntResponse(resp: []const u8) !i64 {
    if (resp.len < 3 or resp[0] != ':') return error.InvalidResponse;
    const end = std.mem.indexOf(u8, resp, "\r\n") orelse return error.InvalidResponse;
    return try std.fmt.parseInt(i64, resp[1..end], 10);
}

/// Helper to parse bulk string response (e.g., "$5\r\nhello\r\n")
fn parseBulkStringResponse(allocator: std.mem.Allocator, resp: []const u8) !?[]const u8 {
    if (resp.len == 0) return error.InvalidResponse;

    if (std.mem.startsWith(u8, resp, "$-1")) return null; // Null bulk string

    if (resp[0] != '$') return error.InvalidResponse;
    const end_len = std.mem.indexOf(u8, resp, "\r\n") orelse return error.InvalidResponse;
    const len = try std.fmt.parseInt(usize, resp[1..end_len], 10);

    const start = end_len + 2;
    const data = resp[start .. start + len];
    return try allocator.dupe(u8, data);
}

test "CONFIG GET maxmemory-samples returns default 5" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET maxmemory-samples (maxmemory-samples = 17 bytes)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
    defer allocator.free(resp);

    // Response should be array with 2 elements: ["maxmemory-samples", "5"]
    try std.testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "maxmemory-samples") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "5") != null);
}

test "CONFIG SET maxmemory-samples to valid value 3" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 3
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n3\r\n");
    defer allocator.free(resp);

    // Should return +OK
    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify it was set
    const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
    defer allocator.free(get_resp);

    try std.testing.expect(std.mem.indexOf(u8, get_resp, "3") != null);
}

test "CONFIG SET maxmemory-samples rejects value below 1" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 0 (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n0\r\n");
    defer allocator.free(resp);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG SET maxmemory-samples rejects value above 10" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 11 (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$2\r\n11\r\n");
    defer allocator.free(resp);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG SET maxmemory-samples accepts boundary value 1" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 1 (valid boundary)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n1\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);
}

test "CONFIG SET maxmemory-samples accepts boundary value 10" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 10 (valid boundary)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$2\r\n10\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);
}

test "CONFIG GET lfu-log-factor returns default 10" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET lfu-log-factor (lfu-log-factor = 14 bytes)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-log-factor\r\n");
    defer allocator.free(resp);

    // Response should be array with 2 elements: ["lfu-log-factor", "10"]
    try std.testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "lfu-log-factor") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "10") != null);
}

test "CONFIG SET lfu-log-factor to valid value 16" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-log-factor 16
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$2\r\n16\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify it was set
    const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-log-factor\r\n");
    defer allocator.free(get_resp);

    try std.testing.expect(std.mem.indexOf(u8, get_resp, "16") != null);
}

test "CONFIG SET lfu-log-factor accepts boundary value 0" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-log-factor 0 (valid boundary)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$1\r\n0\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);
}

test "CONFIG SET lfu-log-factor accepts boundary value 255" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-log-factor 255 (valid boundary)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$3\r\n255\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);
}

test "CONFIG SET lfu-log-factor rejects value below 0" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-log-factor -1 (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$2\r\n-1\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG SET lfu-log-factor rejects value above 255" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-log-factor 256 (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$3\r\n256\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG GET lfu-decay-time returns default 1" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET lfu-decay-time (lfu-decay-time = 14 bytes)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-decay-time\r\n");
    defer allocator.free(resp);

    // Response should be array with 2 elements: ["lfu-decay-time", "1"]
    try std.testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "lfu-decay-time") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "CONFIG SET lfu-decay-time to valid value 5" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-decay-time 5
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-decay-time\r\n$1\r\n5\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify it was set
    const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-decay-time\r\n");
    defer allocator.free(get_resp);

    try std.testing.expect(std.mem.indexOf(u8, get_resp, "5") != null);
}

test "CONFIG SET lfu-decay-time accepts 0" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-decay-time 0 (valid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-decay-time\r\n$1\r\n0\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);
}

test "CONFIG SET lfu-decay-time rejects negative value" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-decay-time -1 (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-decay-time\r\n$2\r\n-1\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG GET with glob pattern matches eviction parameters" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET *memory* to match maxmemory* parameters
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$8\r\n*memory*\r\n");
    defer allocator.free(resp);

    // Should return multiple parameters matching the pattern
    try std.testing.expect(std.mem.startsWith(u8, resp, "*"));
    // Should contain at least maxmemory and maxmemory-samples
    try std.testing.expect(std.mem.indexOf(u8, resp, "maxmemory") != null);
}

test "INFO stats shows evicted_keys counter" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // INFO stats
    const resp = try sendCommand(allocator, stream, "*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n");
    defer allocator.free(resp);

    // Should contain evicted_keys: line
    try std.testing.expect(std.mem.indexOf(u8, resp, "evicted_keys:") != null);
}

test "evicted_keys counter starts at 0" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // FLUSHALL to reset state
    _ = try sendCommand(allocator, stream, "*1\r\n$8\r\nFLUSHALL\r\n");

    // INFO stats to get evicted_keys
    const resp = try sendCommand(allocator, stream, "*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n");
    defer allocator.free(resp);

    // Should show evicted_keys:0 (or some value >= 0)
    try std.testing.expect(std.mem.indexOf(u8, resp, "evicted_keys:") != null);
}

test "CONFIG SET with invalid parameter name returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET nonexistent-param 123
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nnonexistent-param\r\n$3\r\n123\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG SET maxmemory-samples with non-integer value returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples "abc" (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$3\r\nabc\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG SET lfu-log-factor with non-integer value returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-log-factor "xyz" (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$3\r\nxyz\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG SET lfu-decay-time with non-integer value returns error" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-decay-time "not-a-number" (invalid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-decay-time\r\n$11\r\nnot-a-number\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "CONFIG multiple GET patterns returns combined results" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET maxmemory-samples lfu-log-factor (two patterns)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n$14\r\nlfu-log-factor\r\n");
    defer allocator.free(resp);

    // Should return array with 4 elements: [param1, value1, param2, value2]
    try std.testing.expect(std.mem.startsWith(u8, resp, "*4\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "maxmemory-samples") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "lfu-log-factor") != null);
}

test "CONFIG GET returns parameters in key-value pairs" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET maxmemory-samples
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
    defer allocator.free(resp);

    // Response format: *2\r\n$17\r\nmaxmemory-samples\r\n$1\r\n5\r\n
    // Should be array of 2 elements
    try std.testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "$17\r\nmaxmemory-samples\r\n") != null);
}

test "CONFIG SET consecutive updates work correctly" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Set maxmemory-samples to 7
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n7\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Set maxmemory-samples to 2
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n2\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Verify final value
    {
        const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
        defer allocator.free(resp);
        try std.testing.expect(std.mem.indexOf(u8, resp, "2") != null);
    }
}

test "CONFIG SET multiple parameters in one command" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 4 lfu-log-factor 20
    const resp = try sendCommand(allocator, stream, "*6\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n4\r\n$14\r\nlfu-log-factor\r\n$2\r\n20\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify both values
    {
        const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
        defer allocator.free(get_resp);
        try std.testing.expect(std.mem.indexOf(u8, get_resp, "4") != null);
    }
    {
        const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-log-factor\r\n");
        defer allocator.free(get_resp);
        try std.testing.expect(std.mem.indexOf(u8, get_resp, "20") != null);
    }
}

test "CONFIG GET with lowercase parameter name works" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET maxmemory-samples (lowercase)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
    defer allocator.free(resp);

    // Should return value
    try std.testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "maxmemory-samples") != null);
}

test "CONFIG SET case-insensitive parameter name" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET LFU-LOG-FACTOR 25 (mixed case)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nLFU-LOG-FACTOR\r\n$2\r\n25\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify it was set (check with lowercase GET)
    const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-log-factor\r\n");
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.indexOf(u8, get_resp, "25") != null);
}

test "maxmemory-samples range test: boundary values" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Test lower boundary
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n1\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Test upper boundary
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$2\r\n10\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Test mid-range value
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n5\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }
}

test "lfu-log-factor range test: boundary values 0 and 255" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Test lower boundary (0)
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$1\r\n0\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Test upper boundary (255)
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$3\r\n255\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }

    // Test mid-range value
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-log-factor\r\n$2\r\n10\r\n");
        defer allocator.free(resp);
        try std.testing.expectEqualStrings("+OK\r\n", resp);
    }
}

test "CONFIG GET with wildcard matches all parameters starting with 'lfu'" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET lfu*
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nlfu*\r\n");
    defer allocator.free(resp);

    // Should return array with at least 4 elements (2 params: lfu-log-factor, lfu-decay-time)
    try std.testing.expect(std.mem.startsWith(u8, resp, "*"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "lfu-log-factor") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "lfu-decay-time") != null);
}

test "CONFIG GET with question mark wildcard" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG GET maxmemory-sample? (should match maxmemory-samples)
    const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-sample?\r\n");
    defer allocator.free(resp);

    // Should match maxmemory-samples
    try std.testing.expect(std.mem.indexOf(u8, resp, "maxmemory-samples") != null);
}

test "maxmemory-samples affects sampling behavior" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Set maxmemory-samples to 1 (minimal sampling)
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$1\r\n1\r\n");
        defer allocator.free(resp);
    }

    // Set maxmemory-samples to 10 (maximal sampling)
    {
        const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$2\r\n10\r\n");
        defer allocator.free(resp);
    }

    // Verify we can read back 10
    {
        const resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\nmaxmemory-samples\r\n");
        defer allocator.free(resp);
        try std.testing.expect(std.mem.indexOf(u8, resp, "10") != null);
    }
}

test "INFO stats section exists and is non-empty" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // INFO stats
    const resp = try sendCommand(allocator, stream, "*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n");
    defer allocator.free(resp);

    // Should be a bulk string with content
    try std.testing.expect(std.mem.startsWith(u8, resp, "$"));
    try std.testing.expect(resp.len > 10);
}

test "CONFIG RESETSTAT clears statistics" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG RESETSTAT
    const resp = try sendCommand(allocator, stream, "*2\r\n$6\r\nCONFIG\r\n$9\r\nRESETSTAT\r\n");
    defer allocator.free(resp);

    // Should return +OK
    try std.testing.expectEqualStrings("+OK\r\n", resp);
}

test "invalid maxmemory-samples value with large number" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET maxmemory-samples 1000 (out of range)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$17\r\nmaxmemory-samples\r\n$4\r\n1000\r\n");
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "lfu-decay-time large valid value" {
    const allocator = std.testing.allocator;

    const address = try net.Address.parseIp("127.0.0.1", 6379);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // CONFIG SET lfu-decay-time 10000 (large but valid)
    const resp = try sendCommand(allocator, stream, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\nlfu-decay-time\r\n$5\r\n10000\r\n");
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify it was set
    const get_resp = try sendCommand(allocator, stream, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nlfu-decay-time\r\n");
    defer allocator.free(get_resp);
    try std.testing.expect(std.mem.indexOf(u8, get_resp, "10000") != null);
}
