const std = @import("std");
const Server = @import("../src/server.zig").Server;
const Config = @import("../src/storage/config.zig").Config;

/// Helper to execute command and get response
fn execCommand(allocator: std.mem.Allocator, server: *Server, cmd: []const []const u8) ![]const u8 {
    // Build command in RESP format
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();
    try writer.print("*{d}\r\n", .{cmd.len});
    for (cmd) |arg| {
        try writer.print("${d}\r\n{s}\r\n", .{ arg.len, arg });
    }

    const command = try buf.toOwnedSlice();
    defer allocator.free(command);

    const response = try server.handleClient(command, 1);
    return response;
}

test "VREM: basic remove" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vectors
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0", "v3", "5.0", "6.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);
    try std.testing.expect(std.mem.indexOf(u8, add_resp, ":3\r\n") != null);

    // Remove one vector
    const rem_cmd = [_][]const u8{ "VREM", "myvec", "v2" };
    const rem_resp = try execCommand(allocator, &server, &rem_cmd);
    defer allocator.free(rem_resp);
    try std.testing.expect(std.mem.indexOf(u8, rem_resp, ":1\r\n") != null);

    // Verify cardinality
    const card_cmd = [_][]const u8{ "VCARD", "myvec" };
    const card_resp = try execCommand(allocator, &server, &card_cmd);
    defer allocator.free(card_resp);
    try std.testing.expect(std.mem.indexOf(u8, card_resp, ":2\r\n") != null);
}

test "VREM: multiple removes" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vectors
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0", "v3", "5.0", "6.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Remove multiple vectors
    const rem_cmd = [_][]const u8{ "VREM", "myvec", "v1", "v3" };
    const rem_resp = try execCommand(allocator, &server, &rem_cmd);
    defer allocator.free(rem_resp);
    try std.testing.expect(std.mem.indexOf(u8, rem_resp, ":2\r\n") != null);
}

test "VREM: nonexistent vector" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vector
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Try to remove nonexistent vector
    const rem_cmd = [_][]const u8{ "VREM", "myvec", "v2" };
    const rem_resp = try execCommand(allocator, &server, &rem_cmd);
    defer allocator.free(rem_resp);
    try std.testing.expect(std.mem.indexOf(u8, rem_resp, ":0\r\n") != null);
}

test "VREM: nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    const cmd = [_][]const u8{ "VREM", "nonexistent", "v1" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
}

test "VREM: wrong type" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Create a string key
    const set_cmd = [_][]const u8{ "SET", "mykey", "value" };
    const set_resp = try execCommand(allocator, &server, &set_cmd);
    defer allocator.free(set_resp);

    const cmd = [_][]const u8{ "VREM", "mykey", "v1" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-WRONGTYPE") != null);
}

test "VISMEMBER: exists" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vector
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Check membership
    const cmd = [_][]const u8{ "VISMEMBER", "myvec", "v1" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, ":1\r\n") != null);
}

test "VISMEMBER: not exists" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vector
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Check nonexistent member
    const cmd = [_][]const u8{ "VISMEMBER", "myvec", "v2" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
}

test "VISMEMBER: nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    const cmd = [_][]const u8{ "VISMEMBER", "nonexistent", "v1" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
}

test "VISMEMBER: wrong type" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Create a string key
    const set_cmd = [_][]const u8{ "SET", "mykey", "value" };
    const set_resp = try execCommand(allocator, &server, &set_cmd);
    defer allocator.free(set_resp);

    const cmd = [_][]const u8{ "VISMEMBER", "mykey", "v1" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-WRONGTYPE") != null);
}

test "VRANDMEMBER: single random" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vectors
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Get random member
    const cmd = [_][]const u8{ "VRANDMEMBER", "myvec" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return bulk string (either v1 or v2)
    try std.testing.expect(std.mem.indexOf(u8, resp, "$2\r\nv") != null);
}

test "VRANDMEMBER: with positive count" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vectors
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0", "v3", "5.0", "6.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Get 2 random members (distinct)
    const cmd = [_][]const u8{ "VRANDMEMBER", "myvec", "2" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return array of 2 elements
    try std.testing.expect(std.mem.indexOf(u8, resp, "*2\r\n") != null);
}

test "VRANDMEMBER: with negative count" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vector
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Get 3 random members with duplicates allowed
    const cmd = [_][]const u8{ "VRANDMEMBER", "myvec", "-3" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return array of 3 elements (all v1)
    try std.testing.expect(std.mem.indexOf(u8, resp, "*3\r\n") != null);
}

test "VRANDMEMBER: count exceeds size" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add 2 vectors
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Request 5 distinct members (only 2 available)
    const cmd = [_][]const u8{ "VRANDMEMBER", "myvec", "5" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return array of 2 elements (all available)
    try std.testing.expect(std.mem.indexOf(u8, resp, "*2\r\n") != null);
}

test "VRANDMEMBER: count zero" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Add vector
    const add_cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const add_resp = try execCommand(allocator, &server, &add_cmd);
    defer allocator.free(add_resp);

    // Request 0 members
    const cmd = [_][]const u8{ "VRANDMEMBER", "myvec", "0" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return empty array
    try std.testing.expect(std.mem.indexOf(u8, resp, "*0\r\n") != null);
}

test "VRANDMEMBER: nonexistent key without count" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    const cmd = [_][]const u8{ "VRANDMEMBER", "nonexistent" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return null bulk string
    try std.testing.expect(std.mem.indexOf(u8, resp, "$-1\r\n") != null);
}

test "VRANDMEMBER: nonexistent key with count" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    const cmd = [_][]const u8{ "VRANDMEMBER", "nonexistent", "5" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    // Should return empty array
    try std.testing.expect(std.mem.indexOf(u8, resp, "*0\r\n") != null);
}

test "VRANDMEMBER: wrong type" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var server = try Server.init(allocator, &config);
    defer server.deinit();

    // Create a string key
    const set_cmd = [_][]const u8{ "SET", "mykey", "value" };
    const set_resp = try execCommand(allocator, &server, &set_cmd);
    defer allocator.free(set_resp);

    const cmd = [_][]const u8{ "VRANDMEMBER", "mykey" };
    const resp = try execCommand(allocator, &server, &cmd);
    defer allocator.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "-WRONGTYPE") != null);
}
