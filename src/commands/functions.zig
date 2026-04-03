const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const RespValue = @import("../protocol/parser.zig").RespValue;
const functions_mod = @import("../storage/functions.zig");
const scripting = @import("../scripting/lua_engine.zig");

/// FUNCTION LOAD [REPLACE] <code>
/// Register a Lua function library
pub fn cmdFunctionLoad(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len < 3) {
        return RespValue{ .err = try allocator.dupe(u8, "ERR wrong number of arguments for 'function load' command") };
    }

    var replace = false;
    var code_arg_idx: usize = 2;

    // Check for REPLACE flag
    if (args.len >= 4 and std.ascii.eqlIgnoreCase(args[2], "REPLACE")) {
        replace = true;
        code_arg_idx = 3;
    }

    const code = args[code_arg_idx];

    // Parse Shebang to extract library name
    const shebang = functions_mod.parseShebang(code) catch {
        return RespValue{ .err = try allocator.dupe(u8, "ERR library code must start with a Shebang statement") };
    };

    // Validate engine
    if (!std.mem.eql(u8, shebang.engine, "lua")) {
        return RespValue{ .err = try allocator.dupe(u8, "ERR unsupported engine (only 'lua' is supported)") };
    }

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if library exists
    if (!replace and storage.functions.getLibrary(shebang.library_name) != null) {
        return RespValue{ .err = try allocator.dupe(u8, "ERR library already exists (use REPLACE to overwrite)") };
    }

    // Create library
    var library = try functions_mod.Library.init(allocator, shebang.library_name, "lua", code);
    errdefer library.deinit();

    // TODO: Execute Lua code to register functions via redis.register_function()
    // For now, this is a stub that creates an empty library
    // Future iterations will:
    // 1. Create FunctionRegistrationContext
    // 2. Execute Lua code in sandbox
    // 3. Collect registered functions
    // 4. Add functions to library

    // Add or replace library
    if (replace) {
        storage.functions.replaceLibrary(library) catch |err| {
            if (err == error.LibraryNotFound) {
                // Library doesn't exist, just add it
                try storage.functions.addLibrary(library);
            } else {
                return err;
            }
        };
    } else {
        storage.functions.addLibrary(library) catch |err| {
            if (err == error.LibraryExists) {
                return RespValue{ .err = try allocator.dupe(u8, "ERR library already exists (use REPLACE to overwrite)") };
            } else if (err == error.FunctionExists) {
                return RespValue{ .err = try allocator.dupe(u8, "ERR function name already exists in another library") };
            }
            return err;
        };
    }

    // Return library name
    return RespValue{ .bulk_string = try allocator.dupe(u8, shebang.library_name) };
}

/// FCALL <function> <numkeys> [key...] [arg...]
/// Call a registered function
pub fn cmdFcall(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    if (args.len < 4) {
        return RespValue{ .err = try allocator.dupe(u8, "ERR wrong number of arguments for 'fcall' command") };
    }

    const function_name = args[2];
    const numkeys_str = args[3];

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return RespValue{ .err = try allocator.dupe(u8, "ERR value is not an integer or out of range") };
    };

    if (args.len < 4 + numkeys) {
        return RespValue{ .err = try allocator.dupe(u8, "ERR Number of keys can't be greater than number of args") };
    }

    // Lock storage
    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Lookup function
    const func_info = storage.functions.getFunction(function_name) orelse {
        return RespValue{ .err = try allocator.dupe(u8, "ERR Function not found") };
    };

    // TODO: Execute function via Lua engine
    // For now, return a stub response
    _ = func_info;

    // Stub response
    return RespValue{ .bulk_string = try allocator.dupe(u8, "STUB: Function execution not yet implemented") };
}

/// FUNCTION FLUSH [ASYNC|SYNC]
/// Delete all function libraries
pub fn cmdFunctionFlush(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    // Optional ASYNC/SYNC argument (ignored for now)
    _ = args;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.functions.flush();

    return RespValue{ .simple_string = try allocator.dupe(u8, "OK") };
}

/// FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]
/// List all function libraries
pub fn cmdFunctionList(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) !RespValue {
    _ = args;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    var libraries = std.ArrayList(RespValue).init(allocator);
    errdefer {
        for (libraries.items) |*item| {
            item.deinitRespValue(allocator);
        }
        libraries.deinit(allocator);
    }

    var lib_iter = storage.functions.libraries.iterator();
    while (lib_iter.next()) |lib_entry| {
        const lib = lib_entry.value_ptr;

        var lib_info = std.ArrayList(RespValue).init(allocator);
        errdefer {
            for (lib_info.items) |*item| {
                item.deinitRespValue(allocator);
            }
            lib_info.deinit(allocator);
        }

        // library_name
        try lib_info.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "library_name") });
        try lib_info.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, lib.name) });

        // engine
        try lib_info.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "engine") });
        try lib_info.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, lib.engine) });

        // functions (array of function info)
        try lib_info.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "functions") });
        var funcs_array = std.ArrayList(RespValue).init(allocator);

        var func_iter = lib.functions.iterator();
        while (func_iter.next()) |func_entry| {
            const func = func_entry.value_ptr;
            var func_info_arr = std.ArrayList(RespValue).init(allocator);

            try func_info_arr.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, func.name) });
            try func_info_arr.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, func.description) });
            try func_info_arr.append(allocator, RespValue{ .integer = @intCast(func.flags) });

            try funcs_array.append(allocator, RespValue{ .array = try func_info_arr.toOwnedSlice(allocator) });
        }

        try lib_info.append(allocator, RespValue{ .array = try funcs_array.toOwnedSlice(allocator) });

        try libraries.append(allocator, RespValue{ .array = try lib_info.toOwnedSlice(allocator) });
    }

    return RespValue{ .array = try libraries.toOwnedSlice(allocator) };
}
