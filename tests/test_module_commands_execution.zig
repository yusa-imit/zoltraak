const std = @import("std");
const testing = std.testing;
const server = @import("../src/server.zig");
const parser = @import("../src/protocol/parser.zig");
const RespValue = parser.RespValue;
const Storage = @import("../src/storage/memory.zig").Storage;
const ModuleStore = @import("../src/storage/modules.zig").ModuleStore;

/// Mock module command handler for testing
fn mockCommandHandler(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) anyerror![]const u8 {
    _ = storage; // unused in mock

    // Return a simple response based on args
    var response = std.ArrayList(u8).initCapacity(allocator, 100) catch return error.OutOfMemory;
    defer response.deinit(allocator);

    try response.appendSlice(allocator, "+MOCK RESPONSE:");
    for (args, 0..) |arg, i| {
        if (i > 0) try response.appendSlice(allocator, " ");
        switch (arg) {
            .bulk_string => |s| try response.appendSlice(allocator, s),
            else => try response.appendSlice(allocator, "<arg>"),
        }
    }
    try response.appendSlice(allocator, "\r\n");

    return response.toOwnedSlice(allocator);
}

/// Test module command registration and execution
test "module command execution - basic flow" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    // Register a mock module command
    try storage.module_store.registerCommand(
        "testmod",
        "TESTCMD",
        mockCommandHandler,
        "readonly",
        1, // firstkey
        1, // lastkey
        1, // keystep
    );

    // Verify command is registered
    const cmd = storage.module_store.getCommand("TESTCMD");
    try testing.expect(cmd != null);
    try testing.expectEqualStrings("TESTCMD", cmd.?.name);
    try testing.expectEqualStrings("testmod", cmd.?.module_name);
}

/// Test module command overrides built-in command
test "module command execution - override built-in" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    // Register a module command with same name as built-in (PING)
    try storage.module_store.registerCommand(
        "testmod",
        "PING",
        mockCommandHandler,
        "readonly",
        0, // firstkey
        0, // lastkey
        0, // keystep
    );

    // Verify module command is found instead of built-in
    const cmd = storage.module_store.getCommand("PING");
    try testing.expect(cmd != null);
    try testing.expectEqualStrings("testmod", cmd.?.module_name);
}

/// Test module command execution with arguments
test "module command execution - with arguments" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    // Register command
    try storage.module_store.registerCommand(
        "testmod",
        "MYCMD",
        mockCommandHandler,
        "readonly",
        1,
        2,
        1,
    );

    // Execute command with arguments
    const cmd = storage.module_store.getCommand("MYCMD").?;
    const args = [_]RespValue{
        RespValue{ .bulk_string = "MYCMD" },
        RespValue{ .bulk_string = "arg1" },
        RespValue{ .bulk_string = "arg2" },
    };

    const result = try cmd.cmdfunc(allocator, &storage, &args);
    defer allocator.free(result);

    // Verify response
    try testing.expect(std.mem.indexOf(u8, result, "MOCK RESPONSE") != null);
    try testing.expect(std.mem.indexOf(u8, result, "arg1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "arg2") != null);
}

/// Test case-insensitive command lookup
test "module command execution - case insensitive" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    // Register with uppercase
    try storage.module_store.registerCommand(
        "testmod",
        "MYCMD",
        mockCommandHandler,
        "readonly",
        0,
        0,
        0,
    );

    // Lookup with different cases
    try testing.expect(storage.module_store.getCommand("MYCMD") != null);
    try testing.expect(storage.module_store.getCommand("mycmd") != null);
    try testing.expect(storage.module_store.getCommand("MyCmd") != null);
}

/// Test error when command doesn't exist
test "module command execution - command not found" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    // No commands registered
    const cmd = storage.module_store.getCommand("NONEXISTENT");
    try testing.expect(cmd == null);
}

/// Test module unload removes all commands
test "module command execution - unload removes commands" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var storage = try Storage.init(allocator, 16);
    defer storage.deinit();

    // Register multiple commands from same module
    try storage.module_store.registerCommand("testmod", "CMD1", mockCommandHandler, "readonly", 0, 0, 0);
    try storage.module_store.registerCommand("testmod", "CMD2", mockCommandHandler, "readonly", 0, 0, 0);
    try storage.module_store.registerCommand("othermod", "CMD3", mockCommandHandler, "readonly", 0, 0, 0);

    // Verify all registered
    try testing.expect(storage.module_store.getCommand("CMD1") != null);
    try testing.expect(storage.module_store.getCommand("CMD2") != null);
    try testing.expect(storage.module_store.getCommand("CMD3") != null);

    // Remove testmod commands
    storage.module_store.removeModuleCommands("testmod");

    // Verify testmod commands removed, othermod command remains
    try testing.expect(storage.module_store.getCommand("CMD1") == null);
    try testing.expect(storage.module_store.getCommand("CMD2") == null);
    try testing.expect(storage.module_store.getCommand("CMD3") != null);
}
