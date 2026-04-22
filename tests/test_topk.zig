const std = @import("std");
const server_mod = @import("../src/server.zig");
const Server = server_mod.Server;

const test_allocator = std.testing.allocator;

fn startTestServer(allocator: std.mem.Allocator) !*Server {
    const server = try allocator.create(Server);
    server.* = try Server.init(allocator, "127.0.0.1", 16385);
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

test "TOPK.RESERVE: create with default parameters" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TOPK.RESERVE: create with custom parameters" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk2", "5", "10", "8", "0.95" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.eql(u8, resp, "+OK\r\n"));
}

test "TOPK.RESERVE: rejects zero k" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "0" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK.RESERVE: rejects invalid decay (zero)" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3", "8", "7", "0.0" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK.RESERVE: rejects invalid decay (one)" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3", "8", "7", "1.0" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK.RESERVE: rejects existing key" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK.ADD: add single item" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.eql(u8, resp, "*1\r\n$-1\r\n"));
}

test "TOPK.ADD: add multiple items" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple", "banana", "cherry" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "*3\r\n"));
}

test "TOPK.ADD: nonexistent key returns error" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "nonexistent", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK.ADD: WRONGTYPE for non-topk key" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "SET", "mystring", "value" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mystring", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-WRONGTYPE"));
}

test "TOPK.QUERY: single item in top-k" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "mytopk", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.eql(u8, resp, "*1\r\n:1\r\n"));
}

test "TOPK.QUERY: single item not in top-k" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "mytopk", "banana" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.eql(u8, resp, "*1\r\n:0\r\n"));
}

test "TOPK.QUERY: multiple items" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple", "banana" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "mytopk", "apple", "cherry", "banana" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "*3\r\n"));
}

test "TOPK.QUERY: nonexistent key returns error" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "nonexistent", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK.COUNT: single item" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.COUNT", "mytopk", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "*1\r\n:"));
}

test "TOPK.COUNT: nonexistent item returns zero" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.COUNT", "mytopk", "nonexistent" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.eql(u8, resp, "*1\r\n:0\r\n"));
}

test "TOPK.COUNT: multiple items" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "3" });
    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "apple", "banana" });
    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.COUNT", "mytopk", "apple", "banana", "cherry" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "*3\r\n"));
}

test "TOPK.COUNT: nonexistent key returns error" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    const resp = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.COUNT", "nonexistent", "apple" });
    defer test_allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "TOPK: full workflow with frequent items" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.RESERVE", "mytopk", "2" });

    // Add items with different frequencies
    for (0..10) |_| {
        _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "frequent1" });
    }
    for (0..8) |_| {
        _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "frequent2" });
    }
    for (0..2) |_| {
        _ = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk", "rare" });
    }

    // Query should show frequent items in top-k
    const resp1 = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "mytopk", "frequent1" });
    defer test_allocator.free(resp1);
    try std.testing.expect(std.mem.eql(u8, resp1, "*1\r\n:1\r\n"));

    const resp2 = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "mytopk", "frequent2" });
    defer test_allocator.free(resp2);
    try std.testing.expect(std.mem.eql(u8, resp2, "*1\r\n:1\r\n"));
}

test "TOPK: arity validation" {
    const server = try startTestServer(test_allocator);
    defer {
        server.shutdown();
        test_allocator.destroy(server);
    }

    const stream = try std.net.tcpConnectToHost(test_allocator, "127.0.0.1", 16385);
    defer stream.close();

    // TOPK.RESERVE missing arguments
    const resp1 = try sendCommand(test_allocator, stream, &[_][]const u8{"TOPK.RESERVE"});
    defer test_allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR"));

    // TOPK.ADD missing arguments
    const resp2 = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.ADD", "mytopk" });
    defer test_allocator.free(resp2);
    try std.testing.expect(std.mem.startsWith(u8, resp2, "-ERR"));

    // TOPK.QUERY missing arguments
    const resp3 = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.QUERY", "mytopk" });
    defer test_allocator.free(resp3);
    try std.testing.expect(std.mem.startsWith(u8, resp3, "-ERR"));

    // TOPK.COUNT missing arguments
    const resp4 = try sendCommand(test_allocator, stream, &[_][]const u8{ "TOPK.COUNT", "mytopk" });
    defer test_allocator.free(resp4);
    try std.testing.expect(std.mem.startsWith(u8, resp4, "-ERR"));
}
