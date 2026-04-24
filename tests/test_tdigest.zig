const std = @import("std");
const testing = std.testing;
const memory_mod = @import("../src/storage/memory.zig");
const tdigest_mod = @import("../src/storage/tdigest.zig");

const TDigestValue = tdigest_mod.TDigestValue;

test "TDigestValue.init with valid compression" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try testing.expectEqual(100, td.compression);
    try testing.expectEqual(0, td.total_count);
    try testing.expectEqual(std.math.inf(f64), td.min);
    try testing.expectEqual(-std.math.inf(f64), td.max);
}

test "TDigestValue.init with zero compression rejects" {
    const allocator = testing.allocator;
    const result = TDigestValue.init(allocator, 0);
    try testing.expectError(tdigest_mod.TDigestError.InvalidCompression, result);
}

test "TDigestValue.add single value" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.5);

    try testing.expectEqual(1, td.total_count);
    try testing.expectEqual(42.5, td.min);
    try testing.expectEqual(42.5, td.max);
    try testing.expectEqual(1, td.centroids.items.len);
    try testing.expectEqual(42.5, td.centroids.items[0].mean);
    try testing.expectEqual(1, td.centroids.items[0].count);
}

test "TDigestValue.add multiple values" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(5.0);
    try td.add(30.0);

    try testing.expectEqual(4, td.total_count);
    try testing.expectEqual(5.0, td.min);
    try testing.expectEqual(30.0, td.max);
    try testing.expectEqual(4, td.centroids.items.len);
}

test "TDigestValue.add negative values" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(-10.5);
    try td.add(5.0);
    try td.add(-20.0);

    try testing.expectEqual(3, td.total_count);
    try testing.expectEqual(-20.0, td.min);
    try testing.expectEqual(5.0, td.max);
}

test "TDigestValue.add infinity and special values" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(std.math.inf(f64));
    try td.add(-std.math.inf(f64));
    try td.add(0.0);

    try testing.expectEqual(3, td.total_count);
    try testing.expectEqual(-std.math.inf(f64), td.min);
    try testing.expectEqual(std.math.inf(f64), td.max);
}

test "TDigestValue.reset clears centroids" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(1.0);
    try td.add(2.0);
    try td.add(3.0);

    try testing.expectEqual(3, td.total_count);
    td.reset();

    try testing.expectEqual(0, td.total_count);
    try testing.expectEqual(0, td.centroids.items.len);
    try testing.expectEqual(std.math.inf(f64), td.min);
    try testing.expectEqual(-std.math.inf(f64), td.max);
    try testing.expectEqual(100, td.compression); // Preserved
}

test "TDigestValue.reset after reset" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(1.0);
    td.reset();
    try td.add(5.0);
    td.reset();

    try testing.expectEqual(0, td.total_count);
    try testing.expectEqual(0, td.centroids.items.len);
}

test "TDigestValue different compression values" {
    const allocator = testing.allocator;
    var td1 = try TDigestValue.init(allocator, 50);
    defer td1.deinit();
    var td2 = try TDigestValue.init(allocator, 200);
    defer td2.deinit();

    try testing.expectEqual(50, td1.compression);
    try testing.expectEqual(200, td2.compression);
}

test "TDigestValue.add large batch" {
    const allocator = testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    var i: f64 = 0;
    while (i < 1000) : (i += 1.0) {
        try td.add(i);
    }

    try testing.expectEqual(1000, td.total_count);
    try testing.expectEqual(0.0, td.min);
    try testing.expectEqual(999.0, td.max);
    try testing.expectEqual(1000, td.centroids.items.len);
}

// ============================================================================
// Integration Tests — RESP Protocol
// ============================================================================

const server_mod = @import("../src/server.zig");
const Server = server_mod.Server;

const test_allocator = testing.allocator;

fn startTestServer(allocator: std.mem.Allocator) !*Server {
    const server = try allocator.create(Server);
    server.* = try Server.init(allocator, "127.0.0.1", 16390);
    const thread = try std.Thread.spawn(.{}, Server.start, .{server});
    thread.detach();
    std.time.sleep(100 * std.time.ns_per_ms);
    return server;
}

fn sendCommand(allocator: std.mem.Allocator, stream: std.net.Stream, parts: []const []const u8) ![]u8 {
    var cmd_buf = std.ArrayList(u8).init(allocator);
    defer cmd_buf.deinit(allocator);

    const writer = cmd_buf.writer(allocator);
    try writer.print("*{d}\r\n", .{parts.len});
    for (parts) |part| {
        try writer.print("${d}\r\n{s}\r\n", .{ part.len, part });
    }

    const cmd = try cmd_buf.toOwnedSlice(allocator);
    defer allocator.free(cmd);

    _ = try stream.write(cmd);

    var response = std.ArrayList(u8).init(allocator);
    errdefer response.deinit(allocator);

    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);
    try response.appendSlice(allocator, buf[0..n]);

    return try response.toOwnedSlice(allocator);
}

test "TDIGEST.MERGE: basic merge of 2 sketches" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create source sketch 1
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "src1", "100" });
    test_allocator.free(resp);

    // Add values to src1
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "src1", "1.0", "2.0", "3.0" });
    test_allocator.free(resp);

    // Create source sketch 2
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "src2", "100" });
    test_allocator.free(resp);

    // Add values to src2
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "src2", "4.0", "5.0", "6.0" });
    test_allocator.free(resp);

    // Merge into new destination
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "2", "src1", "src2" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: merge of 3+ sketches" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create 4 source sketches
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "s1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "s1", "1.0" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "s2", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "s2", "2.0" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "s3", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "s3", "3.0" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "s4", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "s4", "4.0" });
    test_allocator.free(resp);

    // Merge 4 sketches
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "merged", "4", "s1", "s2", "s3", "s4" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: COMPRESSION parameter override" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create source sketches
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "a1", "50" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "a1", "10.0" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "a2", "50" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "a2", "20.0" });
    test_allocator.free(resp);

    // Merge with COMPRESSION override
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "result", "2", "a1", "a2", "COMPRESSION", "200" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: OVERRIDE flag allows overwriting existing key" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create destination and sources
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "existing", "100" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "b1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "b1", "1.0" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "b2", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "b2", "2.0" });
    test_allocator.free(resp);

    // Merge with OVERRIDE flag
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "existing", "2", "b1", "b2", "OVERRIDE" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: error when destination exists without OVERRIDE" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create destination
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "busy", "100" });
    test_allocator.free(resp);

    // Create sources
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "c1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "c2", "100" });
    test_allocator.free(resp);

    // Merge without OVERRIDE should fail
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "busy", "2", "c1", "c2" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-BUSYKEY"));
}

test "TDIGEST.MERGE: error on numkeys mismatch" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create sources
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "d1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "d2", "100" });
    test_allocator.free(resp);

    // Claim 3 keys but provide only 2
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "result", "3", "d1", "d2" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "numkeys does not match") != null);
}

test "TDIGEST.MERGE: error on zero numkeys" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "0" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "must be greater than 0") != null);
}

test "TDIGEST.MERGE: error on invalid numkeys (non-integer)" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "abc" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "must be an integer") != null);
}

test "TDIGEST.MERGE: error on nonexistent source key" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create only one source
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "e1", "100" });
    test_allocator.free(resp);

    // Reference nonexistent key
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "2", "e1", "nonexistent" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "no such key") != null);
}

test "TDIGEST.MERGE: WRONGTYPE error when source is not t-digest" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create one valid source and one string
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "f1", "100" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "SET", "notdigest", "stringvalue" });
    test_allocator.free(resp);

    // Try to merge with wrong type
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "2", "f1", "notdigest" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-WRONGTYPE"));
}

test "TDIGEST.MERGE: error on invalid compression value (zero)" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create sources
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "g1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "g2", "100" });
    test_allocator.free(resp);

    // Invalid compression
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "2", "g1", "g2", "COMPRESSION", "0" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "compression must be greater than 0") != null);
}

test "TDIGEST.MERGE: error on invalid compression value (non-integer)" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create sources
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "h1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "h2", "100" });
    test_allocator.free(resp);

    // Non-integer compression
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "2", "h1", "h2", "COMPRESSION", "abc" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "must be an integer") != null);
}

test "TDIGEST.MERGE: merge empty source sketches" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create empty sources (no ADD calls)
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "empty1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "empty2", "100" });
    test_allocator.free(resp);

    // Merge empty sketches
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "emptyresult", "2", "empty1", "empty2" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: large number of sources" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create 10 source sketches
    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        const key = try std.fmt.allocPrint(test_allocator, "src{d}", .{i});
        defer test_allocator.free(key);

        var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", key, "100" });
        test_allocator.free(resp);

        const val = try std.fmt.allocPrint(test_allocator, "{d}.0", .{i});
        defer test_allocator.free(val);
        resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", key, val });
        test_allocator.free(resp);
    }

    // Merge all 10 sketches
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{
        "TDIGEST.MERGE",
        "bigmerge",
        "10",
        "src0",
        "src1",
        "src2",
        "src3",
        "src4",
        "src5",
        "src6",
        "src7",
        "src8",
        "src9",
    });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: OVERRIDE with COMPRESSION" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Create existing destination
    var resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "target", "50" });
    test_allocator.free(resp);

    // Create sources
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "i1", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "i1", "1.0" });
    test_allocator.free(resp);

    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.CREATE", "i2", "100" });
    test_allocator.free(resp);
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.ADD", "i2", "2.0" });
    test_allocator.free(resp);

    // Merge with both OVERRIDE and COMPRESSION
    resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "target", "2", "i1", "i2", "OVERRIDE", "COMPRESSION", "150" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TDIGEST.MERGE: arity error (too few arguments)" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16390);
    defer stream.close();

    // Missing source keys
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TDIGEST.MERGE", "dest", "1" });
    defer test_allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}
