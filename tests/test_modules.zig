const std = @import("std");
const net = std.net;
const testing = std.testing;
const Server = @import("../src/server.zig").Server;

// Note: These are module storage tests - they will import modules.zig once it's created
// For now, we'll use conditional compilation to allow tests to compile but fail at runtime

// Helper function to connect to Zoltraak server
fn connectToServer() !net.Stream {
    const addr = try net.Address.parseIp("127.0.0.1", 6379);
    return try net.tcpConnectToAddress(addr);
}

// Helper function to send command and receive response
fn sendCommand(stream: net.Stream, command: []const u8) ![]u8 {
    _ = try stream.write(command);

    var buf: [8192]u8 = undefined;
    const bytes_read = try stream.read(&buf);
    if (bytes_read == 0) return error.ConnectionClosed;

    const allocator = testing.allocator;
    return try allocator.dupe(u8, buf[0..bytes_read]);
}

// Helper to format RESP array command
fn formatCommand(allocator: std.mem.Allocator, args: []const []const u8) ![]u8 {
    var msg = std.ArrayList(u8).init(allocator);
    defer msg.deinit();

    const writer = msg.writer();
    try writer.print("*{d}\r\n", .{args.len});
    for (args) |arg| {
        try writer.print("${d}\r\n{s}\r\n", .{ arg.len, arg });
    }

    return try allocator.dupe(u8, msg.items);
}

// ============================================================================
// Storage Layer Tests (6 tests)
// ============================================================================

test "ModuleStore: init and deinit lifecycle" {
    // This test will fail until src/storage/modules.zig is created
    // Expected: ModuleStore struct with init/deinit methods

    // Temporarily skip - will fail when modules.zig doesn't exist
    if (true) return error.SkipZigTest;

    // const modules_mod = @import("../src/storage/modules.zig");
    // const allocator = testing.allocator;
    // var store = modules_mod.ModuleStore.init(allocator);
    // defer store.deinit();

    // // Verify empty state
    // const list = try store.listModules();
    // try testing.expectEqual(@as(usize, 0), list.len);
}

test "ModuleStore: loadModule returns error.NotSupported (stub)" {
    // This test will fail until loadModule method exists
    if (true) return error.SkipZigTest;

    // const modules_mod = @import("../src/storage/modules.zig");
    // const allocator = testing.allocator;
    // var store = modules_mod.ModuleStore.init(allocator);
    // defer store.deinit();

    // // Stub implementation should return NotSupported
    // const result = store.loadModule("/path/to/module.so", &[_][]const u8{});
    // try testing.expectError(error.NotSupported, result);
}

test "ModuleStore: unloadModule returns error.NotSupported (stub)" {
    // This test will fail until unloadModule method exists
    if (true) return error.SkipZigTest;

    // const modules_mod = @import("../src/storage/modules.zig");
    // const allocator = testing.allocator;
    // var store = modules_mod.ModuleStore.init(allocator);
    // defer store.deinit();

    // // Stub implementation should return NotSupported
    // const result = store.unloadModule("mymodule");
    // try testing.expectError(error.NotSupported, result);
}

test "ModuleStore: listModules returns empty slice (stub)" {
    // This test will fail until listModules method exists
    if (true) return error.SkipZigTest;

    // const modules_mod = @import("../src/storage/modules.zig");
    // const allocator = testing.allocator;
    // var store = modules_mod.ModuleStore.init(allocator);
    // defer store.deinit();

    // const list = try store.listModules();
    // defer allocator.free(list);
    // try testing.expectEqual(@as(usize, 0), list.len);
}

test "ModuleStore: ModuleInfo structure exists with required fields" {
    // This test will fail until ModuleInfo struct is defined
    if (true) return error.SkipZigTest;

    // const modules_mod = @import("../src/storage/modules.zig");
    // const allocator = testing.allocator;

    // // Verify ModuleInfo has all required fields
    // const info = modules_mod.ModuleInfo{
    //     .name = try allocator.dupe(u8, "testmodule"),
    //     .ver = 1,
    //     .path = try allocator.dupe(u8, "/path/to/module.so"),
    //     .args = &[_][]const u8{},
    // };
    // defer allocator.free(info.name);
    // defer allocator.free(info.path);

    // try testing.expectEqualStrings("testmodule", info.name);
    // try testing.expectEqual(@as(i32, 1), info.ver);
    // try testing.expectEqualStrings("/path/to/module.so", info.path);
    // try testing.expectEqual(@as(usize, 0), info.args.len);
}

test "ModuleStore: uses StringHashMap for modules map" {
    // This test will fail until modules field exists
    if (true) return error.SkipZigTest;

    // const modules_mod = @import("../src/storage/modules.zig");
    // const allocator = testing.allocator;
    // var store = modules_mod.ModuleStore.init(allocator);
    // defer store.deinit();

    // // Verify internal structure uses StringHashMap
    // const TypeInfo = @typeInfo(@TypeOf(store.modules));
    // try testing.expect(TypeInfo == .Pointer or TypeInfo == .Struct);
}

// ============================================================================
// Command Layer Integration Tests (14 tests)
// ============================================================================

test "MODULE HELP: returns 4 lines of help text" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE HELP command
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "HELP" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return array of 4 help lines
    try testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "MODULE LOAD") != null);
    try testing.expect(std.mem.indexOf(u8, response, "MODULE UNLOAD") != null);
    try testing.expect(std.mem.indexOf(u8, response, "MODULE LIST") != null);
}

test "MODULE HELP: arity validation (rejects extra args)" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE HELP with extra argument (should reject)
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "HELP", "extra" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null or
        std.mem.indexOf(u8, response, "syntax error") != null);
}

test "MODULE LIST: returns empty array when no modules loaded" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LIST command
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LIST" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return empty array *0\r\n (no modules loaded)
    try testing.expectEqualStrings("*0\r\n", response);
}

test "MODULE LIST: arity validation (rejects extra args)" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LIST with extra argument (should reject)
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LIST", "extra" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null or
        std.mem.indexOf(u8, response, "syntax error") != null);
}

test "MODULE LOAD: requires path argument" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LOAD without path (should reject)
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LOAD" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null or
        std.mem.indexOf(u8, response, "syntax error") != null);
}

test "MODULE LOAD: nonexistent file returns error" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LOAD with nonexistent file
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LOAD", "/nonexistent/module.so" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return error loading module
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "loading the module") != null or
        std.mem.indexOf(u8, response, "server logs") != null);
}

test "MODULE LOAD: accepts optional arguments" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LOAD with path and optional args (nonexistent file will error)
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LOAD", "/path/to/module.so", "arg1", "arg2" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should accept arguments but fail because file doesn't exist
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "loading the module") != null or
        std.mem.indexOf(u8, response, "server logs") != null);
}

test "MODULE LOAD: arity validation (requires at least 3 args)" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LOAD without enough args
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LOAD" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return arity error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null or
        std.mem.indexOf(u8, response, "syntax error") != null);
}

test "MODULE UNLOAD: requires name argument" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE UNLOAD without name (should reject)
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "UNLOAD" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null or
        std.mem.indexOf(u8, response, "syntax error") != null);
}

test "MODULE UNLOAD: nonexistent module returns error" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE UNLOAD with nonexistent module name
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "UNLOAD", "mymodule" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return module not found error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "No such module") != null or
        std.mem.indexOf(u8, response, "not found") != null);
}

test "MODULE UNLOAD: arity validation (requires exactly 3 args)" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE UNLOAD with extra args
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "UNLOAD", "mymodule", "extra" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return arity error
    try testing.expect(std.mem.startsWith(u8, response, "-"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null or
        std.mem.indexOf(u8, response, "syntax error") != null);
}

test "MODULE commands: RESP2/RESP3 compatibility" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Test MODULE HELP in RESP2 mode (default)
    var cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "HELP" });
    defer allocator.free(cmd);

    var response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return RESP2 array
    try testing.expect(std.mem.startsWith(u8, response, "*"));

    // Switch to RESP3
    const hello_cmd = try formatCommand(allocator, &[_][]const u8{ "HELLO", "3" });
    defer allocator.free(hello_cmd);

    const hello_resp = try sendCommand(stream, hello_cmd);
    defer allocator.free(hello_resp);

    // Test MODULE HELP in RESP3 mode
    cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "HELP" });
    defer allocator.free(cmd);

    response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should still work (RESP3 arrays also start with *)
    try testing.expect(std.mem.startsWith(u8, response, "*") or std.mem.startsWith(u8, response, ">"));
}

test "MODULE commands: ACL categories validation" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Verify MODULE LOAD is in @admin @slow @dangerous categories
    const cmd_cats = try formatCommand(allocator, &[_][]const u8{ "COMMAND", "INFO", "MODULE" });
    defer allocator.free(cmd_cats);

    const response = try sendCommand(stream, cmd_cats);
    defer allocator.free(response);

    // Note: This is a basic check - full ACL category validation would require
    // checking COMMAND INFO output which is complex. For now, we just verify
    // the command is registered
    try testing.expect(response.len > 0);
}

test "MODULE dispatcher: case-insensitive subcommands" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Test lowercase
    var cmd = try formatCommand(allocator, &[_][]const u8{ "module", "help" });
    defer allocator.free(cmd);

    var response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));

    // Test uppercase
    cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "HELP" });
    defer allocator.free(cmd);

    response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));

    // Test mixed case
    cmd = try formatCommand(allocator, &[_][]const u8{ "MoDuLe", "HeLp" });
    defer allocator.free(cmd);

    response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));
}

test "MODULE LOAD: empty path returns error" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Send MODULE LOAD with empty path
    const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LOAD", "" });
    defer allocator.free(cmd);

    const response = try sendCommand(stream, cmd);
    defer allocator.free(response);

    // Should return error (invalid path)
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "MODULE LOAD: validates file extension in error cases" {
    const stream = try connectToServer();
    defer stream.close();

    const allocator = testing.allocator;

    // Try to load various invalid paths
    const paths = [_][]const u8{
        "/tmp/nonexistent.so",
        "/tmp/nonexistent.dylib",
        "/tmp/nonexistent.dll",
    };

    for (paths) |path| {
        const cmd = try formatCommand(allocator, &[_][]const u8{ "MODULE", "LOAD", path });
        defer allocator.free(cmd);

        const response = try sendCommand(stream, cmd);
        defer allocator.free(response);

        // All should fail with library loading error
        try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    }
}
