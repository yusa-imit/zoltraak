const std = @import("std");
const Server = @import("../src/server.zig").Server;
const parser = @import("../src/protocol/parser.zig");

fn sendCommand(stream: std.net.Stream, allocator: std.mem.Allocator, args: []const []const u8) !void {
    var msg = std.ArrayList(u8).init(allocator);
    defer msg.deinit();

    const writer = msg.writer();
    try writer.print("*{d}\r\n", .{args.len});
    for (args) |arg| {
        try writer.print("${d}\r\n{s}\r\n", .{ arg.len, arg });
    }

    _ = try stream.write(msg.items);
}

fn readResponse(stream: std.net.Stream, allocator: std.mem.Allocator) ![]const u8 {
    var buffer: [8192]u8 = undefined;
    const n = try stream.read(&buffer);
    return try allocator.dupe(u8, buffer[0..n]);
}

test "FUNCTION DELETE deletes library" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16379, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16379);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Load a library
    const code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code });
    const load_resp = try readResponse(stream, allocator);
    defer allocator.free(load_resp);
    try std.testing.expect(std.mem.indexOf(u8, load_resp, "mylib") != null);

    // Delete the library
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "DELETE", "mylib" });
    const delete_resp = try readResponse(stream, allocator);
    defer allocator.free(delete_resp);
    try std.testing.expectEqualStrings("+OK\r\n", delete_resp);

    // Verify FCALL fails after deletion
    try sendCommand(stream, allocator, &[_][]const u8{ "FCALL", "myfunc", "0" });
    const fcall_resp = try readResponse(stream, allocator);
    defer allocator.free(fcall_resp);
    try std.testing.expect(std.mem.indexOf(u8, fcall_resp, "-ERR Function not found") != null);
}

test "FUNCTION DELETE nonexistent library returns error" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16380, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16380);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "DELETE", "nonexistent" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR Library not found") != null);
}

test "FUNCTION LIST returns all libraries" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16381, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16381);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Load two libraries
    const code1 = "#!lua name=lib1\nredis.register_function('func1', function(keys, args) return 'one' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code1 });
    _ = try readResponse(stream, allocator);

    const code2 = "#!lua name=lib2\nredis.register_function('func2', function(keys, args) return 'two' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code2 });
    const load2_resp = try readResponse(stream, allocator);
    allocator.free(load2_resp);

    // List all libraries
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LIST" });
    const list_resp = try readResponse(stream, allocator);
    defer allocator.free(list_resp);

    // Should be an array containing both libraries
    try std.testing.expect(std.mem.startsWith(u8, list_resp, "*2\r\n")); // 2 libraries
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "lib1") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "lib2") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "func1") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "func2") != null);
}

test "FUNCTION LIST LIBRARYNAME filters by name" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16382, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16382);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Load two libraries
    const code1 = "#!lua name=lib1\nredis.register_function('func1', function(keys, args) return 'one' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code1 });
    _ = try readResponse(stream, allocator);

    const code2 = "#!lua name=lib2\nredis.register_function('func2', function(keys, args) return 'two' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code2 });
    const load2_resp = try readResponse(stream, allocator);
    allocator.free(load2_resp);

    // List only lib1
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LIST", "LIBRARYNAME", "lib1" });
    const list_resp = try readResponse(stream, allocator);
    defer allocator.free(list_resp);

    // Should contain only lib1
    try std.testing.expect(std.mem.startsWith(u8, list_resp, "*1\r\n")); // 1 library
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "lib1") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "lib2") == null);
}

test "FUNCTION LIST WITHCODE includes code" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16383, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16383);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    const code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'test' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code });
    _ = try readResponse(stream, allocator);

    // List with code
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LIST", "WITHCODE" });
    const list_resp = try readResponse(stream, allocator);
    defer allocator.free(list_resp);

    // Should contain library_code field
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "library_code") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "redis.register_function") != null);
}

test "FUNCTION LIST without WITHCODE excludes code" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16384, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16384);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    const code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'test' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code });
    _ = try readResponse(stream, allocator);

    // List without code
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LIST" });
    const list_resp = try readResponse(stream, allocator);
    defer allocator.free(list_resp);

    // Should not contain library_code field
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "library_code") == null);
    // But should contain library info
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "library_name") != null);
    try std.testing.expect(std.mem.indexOf(u8, list_resp, "mylib") != null);
}

test "FUNCTION DELETE wrong argument count" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16385, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16385);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "DELETE" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR wrong number of arguments") != null);
}

test "FUNCTION LIST empty when no libraries" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16386, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16386);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LIST" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expectEqualStrings("*0\r\n", resp); // Empty array
}
