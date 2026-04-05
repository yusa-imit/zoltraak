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

test "FCALL_RO executes read-only function successfully" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16387, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16387);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Load a library with a read-only function
    const code = "#!lua name=readonly_lib\nredis.register_function('get_value', function(keys, args) return redis.call('GET', keys[1]) end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code });
    _ = try readResponse(stream, allocator);

    // Set a key to read
    try sendCommand(stream, allocator, &[_][]const u8{ "SET", "mykey", "myvalue" });
    const set_resp = try readResponse(stream, allocator);
    allocator.free(set_resp);

    // Call function with FCALL_RO
    try sendCommand(stream, allocator, &[_][]const u8{ "FCALL_RO", "get_value", "1", "mykey" });
    const fcall_ro_resp = try readResponse(stream, allocator);
    defer allocator.free(fcall_ro_resp);

    // Should return the value
    try std.testing.expect(std.mem.indexOf(u8, fcall_ro_resp, "myvalue") != null);
}

test "FCALL_RO rejects write commands" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16388, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16388);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Load a library with a write function
    const code = "#!lua name=write_lib\nredis.register_function('set_value', function(keys, args) return redis.call('SET', keys[1], args[1]) end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code });
    _ = try readResponse(stream, allocator);

    // Try to call write function with FCALL_RO
    try sendCommand(stream, allocator, &[_][]const u8{ "FCALL_RO", "set_value", "1", "testkey", "testvalue" });
    const fcall_ro_resp = try readResponse(stream, allocator);
    defer allocator.free(fcall_ro_resp);

    // Should reject with error
    try std.testing.expect(std.mem.indexOf(u8, fcall_ro_resp, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, fcall_ro_resp, "read-only") != null);
}

test "FCALL_RO wrong argument count" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16389, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16389);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Missing arguments
    try sendCommand(stream, allocator, &[_][]const u8{ "FCALL_RO", "myfunc" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR wrong number of arguments") != null);
}

test "FCALL_RO nonexistent function" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16390, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16390);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FCALL_RO", "nonexistent", "0" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR Function not found") != null);
}

test "FUNCTION STATS when no function running" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16391, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16391);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "STATS" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);

    // Should return array with running_script=null and engines
    try std.testing.expect(std.mem.indexOf(u8, resp, "running_script") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "engines") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "LUA") != null);
}

test "FUNCTION STATS with wrong argument count" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16392, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16392);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "STATS", "extra_arg" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR wrong number of arguments") != null);
}

test "FUNCTION KILL when no function running" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16393, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16393);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "KILL" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-NOTBUSY") != null);
}

test "FUNCTION KILL with wrong argument count" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16394, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16394);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "KILL", "extra_arg" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR wrong number of arguments") != null);
}

test "FUNCTION HELP returns help text" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16395, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16395);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "HELP" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);

    // Should return array of help strings
    try std.testing.expect(std.mem.startsWith(u8, resp, "*"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION DELETE") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION DUMP") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION FLUSH") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION KILL") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION LIST") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION LOAD") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION RESTORE") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "FUNCTION STATS") != null);
}

test "FUNCTION HELP with wrong argument count" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16396, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16396);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "HELP", "extra_arg" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR wrong number of arguments") != null);
}

test "FUNCTION FLUSH with ASYNC mode" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16397, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16397);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Load a library
    const code = "#!lua name=testlib\nredis.register_function('testfunc', function(keys, args) return 'ok' end)";
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LOAD", code });
    _ = try readResponse(stream, allocator);

    // Flush with ASYNC
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "FLUSH", "ASYNC" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expectEqualStrings("+OK\r\n", resp);

    // Verify functions were deleted
    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "LIST" });
    const list_resp = try readResponse(stream, allocator);
    defer allocator.free(list_resp);
    try std.testing.expectEqualStrings("*0\r\n", list_resp);
}

test "FUNCTION FLUSH with invalid mode" {
    const allocator = std.testing.allocator;

    var server = Server.init(allocator, "127.0.0.1", 16398, null, null, null);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, Server.start, .{&server});
    std.time.sleep(100 * std.time.ns_per_ms);

    defer {
        server.stop();
        server_thread.join();
    }

    const address = try std.net.Address.parseIp("127.0.0.1", 16398);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    try sendCommand(stream, allocator, &[_][]const u8{ "FUNCTION", "FLUSH", "INVALID" });
    const resp = try readResponse(stream, allocator);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-ERR invalid FUNCTION FLUSH mode") != null);
}
