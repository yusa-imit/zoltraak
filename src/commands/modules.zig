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

/// Handle MODULE LOAD command
/// Loads a dynamic library module from the specified path
pub fn cmdModuleLoad(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE LOAD requires at least path argument
    // args[0] = "MODULE", args[1] = "LOAD", args[2] = path, args[3...] = optional module args
    if (args.len < 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|load' command");
    }

    const path = args[2];
    const module_args = if (args.len > 3) args[3..] else &[_][]const u8{};

    // Try to load the module
    storage.module_store.loadModule(path, module_args) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();

        const msg = switch (err) {
            error.InvalidPath => "ERR Invalid module path",
            error.AlreadyLoaded => "ERR Module already loaded",
            error.LibraryOpenFailed => "ERR Error loading the module. Please check the server logs.",
            error.SymbolNotFound => "ERR Module does not export RedisModule_OnLoad",
            error.InitFailed => "ERR Module initialization failed",
            else => "ERR Error loading the module",
        };

        return try w.writeError(msg);
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle MODULE UNLOAD command
/// Unloads a previously loaded module by name
pub fn cmdModuleUnload(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE UNLOAD requires exactly 3 args: MODULE, UNLOAD, name
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|unload' command");
    }

    const name = args[2];

    // Try to unload the module
    storage.module_store.unloadModule(name) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();

        const msg = switch (err) {
            error.NotFound => "ERR No such module with that name",
            else => "ERR Error unloading the module",
        };

        return try w.writeError(msg);
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle MODULE LIST command
/// Returns array of loaded modules with their metadata
pub fn cmdModuleList(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // MODULE LIST takes no additional arguments
    if (args.len > 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|list' command");
    }

    const modules = try storage.module_store.listModules();
    defer allocator.free(modules);

    var w = Writer.init(allocator);
    defer w.deinit();

    // Build array of module info
    // Each module is represented as an array: [name, ver]
    var module_list = try std.ArrayList(RespValue).initCapacity(allocator, modules.len);
    errdefer {
        for (module_list.items) |item| {
            if (item == .array) {
                for (item.array) |elem| {
                    if (elem == .bulk_string) {
                        allocator.free(elem.bulk_string);
                    }
                }
                allocator.free(item.array);
            }
        }
        module_list.deinit(allocator);
    }

    for (modules) |module| {
        var fields = try std.ArrayList(RespValue).initCapacity(allocator, 4);
        errdefer {
            for (fields.items) |item| {
                if (item == .bulk_string) {
                    allocator.free(item.bulk_string);
                }
            }
            fields.deinit(allocator);
        }

        // "name"
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "name") });
        // module name value
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, module.name) });
        // "ver"
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "ver") });
        // version value
        try fields.append(allocator, RespValue{ .integer = module.ver });

        const fields_slice = try fields.toOwnedSlice(allocator);
        try module_list.append(allocator, RespValue{ .array = fields_slice });
    }

    return try w.writeArray(module_list.items);
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
