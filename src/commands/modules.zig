const std = @import("std");
const storage_mod = @import("../storage/memory.zig");
const modules_storage_mod = @import("../storage/modules.zig");
const writer_mod = @import("../protocol/writer.zig");
const parser_mod = @import("../protocol/parser.zig");

const Storage = storage_mod.Storage;
const Writer = writer_mod.Writer;
const RespValue = parser_mod.RespValue;
const ModuleError = modules_storage_mod.ModuleError;

/// Handle MODULE HELP command
/// Returns array of help strings describing MODULE subcommands
pub fn cmdModuleHelp(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE HELP takes no additional arguments (only MODULE and HELP)
    if (args.len > 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|help' command");
    }

    var w = Writer.init(allocator);
    defer w.deinit();

    // Build help array with 4 lines describing MODULE commands
    var help_lines = try std.ArrayList(RespValue).initCapacity(allocator, 4);
    errdefer {
        for (help_lines.items) |item| {
            if (item == .bulk_string) {
                allocator.free(item.bulk_string);
            }
        }
        help_lines.deinit(allocator);
    }

    const help_text = [_][]const u8{
        "MODULE LOAD <path> [arg ...] -- Load a module",
        "MODULE UNLOAD <name> -- Unload a module",
        "MODULE LIST -- List all loaded modules",
        "MODULE HELP -- Show this help",
    };

    for (help_text) |line| {
        try help_lines.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, line) });
    }

    return try w.writeArray(help_lines.items);
}

/// Handle MODULE LOAD command (stub implementation)
/// Returns error.NotSupported - dynamic library loading not yet implemented
pub fn cmdModuleLoad(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE LOAD requires at least path argument
    // args[0] = "MODULE", args[1] = "LOAD", args[2...] = path and optional args
    if (args.len < 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|load' command");
    }

    // Stub implementation returns NotSupported error
    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeError("ERR Error loading the module. Please check the server logs.");
}

/// Handle MODULE UNLOAD command (stub implementation)
/// Returns error.NotSupported - dynamic library unloading not yet implemented
pub fn cmdModuleUnload(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE UNLOAD requires exactly 3 args: MODULE, UNLOAD, name
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|unload' command");
    }

    // Stub implementation returns NotSupported error
    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeError("ERR Error unloading the module. Please check the server logs.");
}

/// Handle MODULE LIST command
/// Returns empty array (stub implementation - no modules loaded)
pub fn cmdModuleList(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE LIST takes no additional arguments
    if (args.len > 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|list' command");
    }

    var w = Writer.init(allocator);
    defer w.deinit();

    // Return empty array (no modules loaded in stub)
    return try w.writeArray(&[_]RespValue{});
}

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

test "MODULE HELP command exists" {
    // Test will be verified by integration tests
    _ = cmdModuleHelp;
}

test "MODULE LOAD command exists" {
    _ = cmdModuleLoad;
}

test "MODULE UNLOAD command exists" {
    _ = cmdModuleUnload;
}

test "MODULE LIST command exists" {
    _ = cmdModuleList;
}
