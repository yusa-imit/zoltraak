const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const modules_mod = @import("../src/storage/modules.zig");
const parser_mod = @import("../src/protocol/parser.zig");
const RespValue = parser_mod.RespValue;

/// Integration tests for module command registration

// Mock command handler that returns a simple OK response
fn mockCmdHandler(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) anyerror![]const u8 {
    _ = storage;
    _ = args;
    return try allocator.dupe(u8, "+OK\r\n");
}

// Mock command handler that returns args count
fn mockEchoCmdHandler(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) anyerror![]const u8 {
    _ = storage;
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    try buf.writer().print("+ARGS:{d}\r\n", .{args.len});
    return try buf.toOwnedSlice();
}

test "Module command registration: basic registration" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register a command
    try store.registerCommand("testmodule", "TEST.HELLO", mockCmdHandler, "readonly fast", 0, 0, 0);

    // Verify command exists
    const cmd = store.getCommand("TEST.HELLO");
    try testing.expect(cmd != null);

    if (cmd) |c| {
        try testing.expectEqualStrings("TEST.HELLO", c.name);
        try testing.expectEqualStrings("testmodule", c.module_name);
        try testing.expectEqualStrings("readonly fast", c.flags);
        try testing.expectEqual(@as(i32, 0), c.firstkey);
        try testing.expectEqual(@as(i32, 0), c.lastkey);
        try testing.expectEqual(@as(i32, 0), c.keystep);
    }
}

test "Module command registration: multiple commands from same module" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register multiple commands
    try store.registerCommand("mymodule", "MY.GET", mockCmdHandler, "readonly", 1, 1, 1);
    try store.registerCommand("mymodule", "MY.SET", mockCmdHandler, "write", 1, 1, 1);
    try store.registerCommand("mymodule", "MY.DEL", mockCmdHandler, "write", 1, -1, 1);

    // Verify all commands exist
    try testing.expect(store.getCommand("MY.GET") != null);
    try testing.expect(store.getCommand("MY.SET") != null);
    try testing.expect(store.getCommand("MY.DEL") != null);
}

test "Module command registration: duplicate command name rejected" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register first command
    try store.registerCommand("module1", "SHARED.CMD", mockCmdHandler, "readonly", 0, 0, 0);

    // Attempt to register duplicate from different module
    const result = store.registerCommand("module2", "SHARED.CMD", mockCmdHandler, "write", 1, 1, 1);
    try testing.expectError(error.CommandAlreadyExists, result);
}

test "Module command registration: empty command name rejected" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    const result = store.registerCommand("mymodule", "", mockCmdHandler, "readonly", 0, 0, 0);
    try testing.expectError(error.InvalidCommandName, result);
}

test "Module command registration: command with key specs" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register command with key specifications
    // Similar to MGET: firstkey=1, lastkey=-1, keystep=1 (all args after cmd are keys)
    try store.registerCommand("mymodule", "MY.MGET", mockCmdHandler, "readonly", 1, -1, 1);

    const cmd = store.getCommand("MY.MGET");
    try testing.expect(cmd != null);

    if (cmd) |c| {
        try testing.expectEqual(@as(i32, 1), c.firstkey);
        try testing.expectEqual(@as(i32, -1), c.lastkey);
        try testing.expectEqual(@as(i32, 1), c.keystep);
    }
}

test "Module command registration: removeModuleCommands cleanup" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register commands from two modules
    try store.registerCommand("module1", "M1.CMD1", mockCmdHandler, "readonly", 0, 0, 0);
    try store.registerCommand("module1", "M1.CMD2", mockCmdHandler, "write", 1, 1, 1);
    try store.registerCommand("module2", "M2.CMD1", mockCmdHandler, "fast", 0, 0, 0);

    // Remove module1 commands
    store.removeModuleCommands("module1");

    // Verify module1 commands are gone
    try testing.expect(store.getCommand("M1.CMD1") == null);
    try testing.expect(store.getCommand("M1.CMD2") == null);

    // Verify module2 command still exists
    try testing.expect(store.getCommand("M2.CMD1") != null);
}

test "Module command registration: ModuleCtx.createCommand API" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Create module context
    var ctx = modules_mod.ModuleCtx{
        .name = "testmodule",
        .ver = 1,
        .store = &store,
    };

    // Register command via context
    try ctx.createCommand("TEST.PING", mockCmdHandler, "fast readonly", 0, 0, 0);

    // Verify command registered with correct module name
    const cmd = store.getCommand("TEST.PING");
    try testing.expect(cmd != null);

    if (cmd) |c| {
        try testing.expectEqualStrings("testmodule", c.module_name);
        try testing.expectEqualStrings("TEST.PING", c.name);
    }
}

test "Module command registration: command handler invocation" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register command
    try store.registerCommand("testmodule", "TEST.ECHO", mockEchoCmdHandler, "readonly", 0, 0, 0);

    // Get command and invoke handler
    const cmd = store.getCommand("TEST.ECHO");
    try testing.expect(cmd != null);

    if (cmd) |c| {
        // Create mock args
        const args = [_]RespValue{
            RespValue{ .bulk_string = "TEST.ECHO" },
            RespValue{ .bulk_string = "arg1" },
            RespValue{ .bulk_string = "arg2" },
        };

        // Invoke handler
        const response = try c.cmdfunc(allocator, &storage, &args);
        defer allocator.free(response);

        // Verify response
        try testing.expectEqualStrings("+ARGS:3\r\n", response);
    }
}

test "Module command registration: flags preserved" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register with complex flags
    const flags = "write deny-oom random fast";
    try store.registerCommand("mymodule", "MY.HEAVY", mockCmdHandler, flags, 1, 1, 1);

    const cmd = store.getCommand("MY.HEAVY");
    try testing.expect(cmd != null);

    if (cmd) |c| {
        try testing.expectEqualStrings(flags, c.flags);
    }
}

test "Module command registration: case-sensitive command names" {
    const allocator = testing.allocator;
    var store = modules_mod.ModuleStore.init(allocator);
    defer store.deinit();

    // Register with uppercase
    try store.registerCommand("mymodule", "MY.CMD", mockCmdHandler, "readonly", 0, 0, 0);

    // Lookup with exact case works
    try testing.expect(store.getCommand("MY.CMD") != null);

    // Different case should not find (case-sensitive lookup)
    try testing.expect(store.getCommand("my.cmd") == null);
    try testing.expect(store.getCommand("My.Cmd") == null);
}
