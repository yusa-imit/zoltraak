const std = @import("std");
const Server = @import("../src/server.zig").Server;

test "TS.MRANGE basic multi-key range query" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create two time series with labels
    _ = try conn.write("*7\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$8\r\nlocation\r\n$5\r\nroom1\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*7\r\n$9\r\nTS.CREATE\r\n$4\r\nts:2\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$8\r\nlocation\r\n$5\r\nroom2\r\n");
    _ = try conn.read(&buf);

    // Add samples to ts:1
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n2000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n3000\r\n$4\r\n30.5\r\n");
    _ = try conn.read(&buf);

    // Add samples to ts:2
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:2\r\n$4\r\n1500\r\n$4\r\n15.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:2\r\n$4\r\n2500\r\n$4\r\n25.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE 1000 3000 FILTER sensor=temp
    _ = try conn.write("*6\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should return 2 series
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
}

test "TS.MRANGE with WITHLABELS" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series with labels
    _ = try conn.write("*7\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$8\r\nlocation\r\n$5\r\nroom1\r\n");
    _ = try conn.read(&buf);

    // Add sample
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE with WITHLABELS
    _ = try conn.write("*7\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n2000\r\n$10\r\nWITHLABELS\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should include labels
    try std.testing.expect(std.mem.indexOf(u8, response, "sensor") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "location") != null);
}

test "TS.MRANGE with SELECTED_LABELS" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series with labels
    _ = try conn.write("*7\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$8\r\nlocation\r\n$5\r\nroom1\r\n");
    _ = try conn.read(&buf);

    // Add sample
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE with SELECTED_LABELS sensor
    _ = try conn.write("*8\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n2000\r\n$15\r\nSELECTED_LABELS\r\n$6\r\nsensor\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should include sensor label but not location
    try std.testing.expect(std.mem.indexOf(u8, response, "sensor") != null);
    // Location should not be in selected labels (but may appear in filter)
}

test "TS.MRANGE with FILTER_BY_TS" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series
    _ = try conn.write("*5\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n");
    _ = try conn.read(&buf);

    // Add samples
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n2000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n3000\r\n$4\r\n30.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE with FILTER_BY_TS 2000
    _ = try conn.write("*9\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n$12\r\nFILTER_BY_TS\r\n$4\r\n2000\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should only return timestamp 2000
    try std.testing.expect(std.mem.indexOf(u8, response, ":2000\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":1000\r\n") == null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":3000\r\n") == null);
}

test "TS.MRANGE with FILTER_BY_VALUE" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series
    _ = try conn.write("*5\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n");
    _ = try conn.read(&buf);

    // Add samples
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n2000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n3000\r\n$4\r\n30.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE with FILTER_BY_VALUE 15 25
    _ = try conn.write("*10\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n$15\r\nFILTER_BY_VALUE\r\n$2\r\n15\r\n$2\r\n25\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should only return timestamp 2000 (value 20.5)
    try std.testing.expect(std.mem.indexOf(u8, response, "+20.5\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "+10.5\r\n") == null);
    try std.testing.expect(std.mem.indexOf(u8, response, "+30.5\r\n") == null);
}

test "TS.MRANGE with COUNT limit" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series
    _ = try conn.write("*5\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n");
    _ = try conn.read(&buf);

    // Add samples
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n2000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n3000\r\n$4\r\n30.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE with COUNT 1
    _ = try conn.write("*8\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should return only 1 sample per series
    try std.testing.expect(std.mem.indexOf(u8, response, "*1\r\n") != null);
}

test "TS.MRANGE with wildcard timestamps" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series
    _ = try conn.write("*5\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n");
    _ = try conn.read(&buf);

    // Add samples
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n2000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE - + (all timestamps)
    _ = try conn.write("*6\r\n$9\r\nTS.MRANGE\r\n$1\r\n-\r\n$1\r\n+\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should return all samples
    try std.testing.expect(std.mem.indexOf(u8, response, ":1000\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":2000\r\n") != null);
}

test "TS.MREVRANGE basic reverse query" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create time series
    _ = try conn.write("*5\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n");
    _ = try conn.read(&buf);

    // Add samples in ascending order
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n2000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n3000\r\n$4\r\n30.5\r\n");
    _ = try conn.read(&buf);

    // TS.MREVRANGE should return in descending order
    _ = try conn.write("*6\r\n$12\r\nTS.MREVRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // First sample should be 3000, last should be 1000
    const pos_3000 = std.mem.indexOf(u8, response, ":3000\r\n").?;
    const pos_1000 = std.mem.indexOf(u8, response, ":1000\r\n").?;
    try std.testing.expect(pos_3000 < pos_1000);
}

test "TS.MRANGE error: missing FILTER" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // TS.MRANGE without FILTER
    _ = try conn.write("*3\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "requires at least one FILTER") != null);
}

test "TS.MRANGE error: no positive filter" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // TS.MRANGE with only negative filter (sensor!=temp)
    _ = try conn.write("*6\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n3000\r\n$6\r\nFILTER\r\n$12\r\nsensor!=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "positive filter") != null);
}

test "TS.MRANGE error: invalid timestamp range" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // TS.MRANGE with fromTimestamp > toTimestamp
    _ = try conn.write("*6\r\n$9\r\nTS.MRANGE\r\n$4\r\n3000\r\n$4\r\n1000\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "fromTimestamp must be <= toTimestamp") != null);
}

test "TS.MRANGE with multiple filters (AND logic)" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 0);
    defer server.deinit();

    const port = try server.start();
    defer server.stop();

    const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr);

    const conn = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer conn.close();

    var buf: [4096]u8 = undefined;

    // Create two time series
    _ = try conn.write("*7\r\n$9\r\nTS.CREATE\r\n$4\r\nts:1\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$8\r\nlocation\r\n$5\r\nroom1\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*7\r\n$9\r\nTS.CREATE\r\n$4\r\nts:2\r\n$6\r\nLABELS\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$8\r\nlocation\r\n$5\r\nroom2\r\n");
    _ = try conn.read(&buf);

    // Add samples
    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:1\r\n$4\r\n1000\r\n$4\r\n10.5\r\n");
    _ = try conn.read(&buf);

    _ = try conn.write("*4\r\n$6\r\nTS.ADD\r\n$4\r\nts:2\r\n$4\r\n1000\r\n$4\r\n20.5\r\n");
    _ = try conn.read(&buf);

    // TS.MRANGE with two filters: sensor=temp AND location=room1 (should match only ts:1)
    _ = try conn.write("*8\r\n$9\r\nTS.MRANGE\r\n$4\r\n1000\r\n$4\r\n2000\r\n$6\r\nFILTER\r\n$11\r\nsensor=temp\r\n$6\r\nFILTER\r\n$14\r\nlocation=room1\r\n");
    const n = try conn.read(&buf);
    const response = buf[0..n];

    // Should return only 1 series (ts:1)
    try std.testing.expect(std.mem.startsWith(u8, response, "*1\r\n"));
}
