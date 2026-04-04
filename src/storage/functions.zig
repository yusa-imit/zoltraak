const std = @import("std");
const Allocator = std.mem.Allocator;

/// Function metadata stored in a library
pub const FunctionInfo = struct {
    name: []const u8, // Function name (unique across all libraries)
    description: []const u8, // Optional description
    flags: u8, // Reserved for future: no-writes, allow-oom, allow-stale, etc.
    library_name: []const u8, // Library that owns this function
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, library_name: []const u8, description: []const u8) !FunctionInfo {
        return FunctionInfo{
            .name = try allocator.dupe(u8, name),
            .description = try allocator.dupe(u8, description),
            .flags = 0,
            .library_name = try allocator.dupe(u8, library_name),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FunctionInfo) void {
        self.allocator.free(self.name);
        self.allocator.free(self.description);
        self.allocator.free(self.library_name);
    }
};

/// A Redis Function library containing one or more functions
pub const Library = struct {
    name: []const u8, // Library name from Shebang
    engine: []const u8, // "lua" (only supported engine)
    code: []const u8, // Full Lua source code
    functions: std.StringHashMap(FunctionInfo), // function_name → FunctionInfo
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, engine: []const u8, code: []const u8) !Library {
        return Library{
            .name = try allocator.dupe(u8, name),
            .engine = try allocator.dupe(u8, engine),
            .code = try allocator.dupe(u8, code),
            .functions = std.StringHashMap(FunctionInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Library) void {
        var iter = self.functions.iterator();
        while (iter.next()) |entry| {
            var func_info = entry.value_ptr;
            func_info.deinit();
        }
        self.functions.deinit();
        self.allocator.free(self.name);
        self.allocator.free(self.engine);
        self.allocator.free(self.code);
    }

    /// Add a function to this library
    pub fn addFunction(self: *Library, func_info: FunctionInfo) !void {
        try self.functions.put(func_info.name, func_info);
    }
};

/// Storage for all Redis Function libraries
pub const FunctionStore = struct {
    libraries: std.StringHashMap(Library), // library_name → Library
    function_index: std.StringHashMap(*FunctionInfo), // function_name → *FunctionInfo (fast lookup)
    allocator: Allocator,

    pub fn init(allocator: Allocator) FunctionStore {
        return FunctionStore{
            .libraries = std.StringHashMap(Library).init(allocator),
            .function_index = std.StringHashMap(*FunctionInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FunctionStore) void {
        var iter = self.libraries.iterator();
        while (iter.next()) |entry| {
            var lib = entry.value_ptr;
            lib.deinit();
        }
        self.libraries.deinit();
        self.function_index.deinit();
    }

    /// Add a library with all its functions
    /// Returns error.LibraryExists if library already exists (caller should use REPLACE flag)
    pub fn addLibrary(self: *FunctionStore, library: Library) !void {
        if (self.libraries.contains(library.name)) {
            return error.LibraryExists;
        }

        // Check for function name conflicts across all libraries
        var func_iter = library.functions.iterator();
        while (func_iter.next()) |entry| {
            if (self.function_index.contains(entry.key_ptr.*)) {
                return error.FunctionExists;
            }
        }

        // Add library
        try self.libraries.put(library.name, library);

        // Update function index
        const lib_entry = self.libraries.getEntry(library.name).?;
        var lib_ptr = lib_entry.value_ptr;
        var func_iter2 = lib_ptr.functions.iterator();
        while (func_iter2.next()) |entry| {
            try self.function_index.put(entry.key_ptr.*, entry.value_ptr);
        }
    }

    /// Remove a library and all its functions
    /// Returns error.LibraryNotFound if library doesn't exist
    pub fn removeLibrary(self: *FunctionStore, library_name: []const u8) !void {
        const lib_entry = self.libraries.getEntry(library_name) orelse return error.LibraryNotFound;
        var lib = lib_entry.value_ptr;

        // Remove function index entries
        var func_iter = lib.functions.iterator();
        while (func_iter.next()) |entry| {
            _ = self.function_index.remove(entry.key_ptr.*);
        }

        // Remove and deinit library
        var removed_lib = self.libraries.fetchRemove(library_name).?;
        removed_lib.value.deinit();
    }

    /// Get a function by name (O(1) lookup)
    pub fn getFunction(self: *FunctionStore, function_name: []const u8) ?*FunctionInfo {
        return self.function_index.get(function_name);
    }

    /// Get a library by name
    pub fn getLibrary(self: *FunctionStore, library_name: []const u8) ?*Library {
        return self.libraries.getPtr(library_name);
    }

    /// Delete all libraries (FUNCTION FLUSH)
    pub fn flush(self: *FunctionStore) void {
        var iter = self.libraries.iterator();
        while (iter.next()) |entry| {
            var lib = entry.value_ptr;
            lib.deinit();
        }
        self.libraries.clearRetainingCapacity();
        self.function_index.clearRetainingCapacity();
    }

    /// Replace an existing library with a new one
    /// Returns error.LibraryNotFound if library doesn't exist
    pub fn replaceLibrary(self: *FunctionStore, library: Library) !void {
        // Remove old library first
        try self.removeLibrary(library.name);

        // Add new library
        try self.addLibrary(library);
    }

    /// Dump all libraries to a binary payload for backup/migration
    /// Serialization format:
    /// - 8 bytes: Magic header "ZOLFUNC\x01"
    /// - 4 bytes: Number of libraries (u32)
    /// For each library:
    ///   - 4 bytes: Library name length (u32)
    ///   - N bytes: Library name
    ///   - 4 bytes: Engine length (u32)
    ///   - N bytes: Engine
    ///   - 4 bytes: Code length (u32)
    ///   - N bytes: Code
    ///   - 4 bytes: Number of functions (u32)
    ///   For each function:
    ///     - 4 bytes: Function name length (u32)
    ///     - N bytes: Function name
    ///     - 4 bytes: Description length (u32)
    ///     - N bytes: Description
    ///     - 1 byte: Flags
    pub fn dump(self: *FunctionStore, allocator: Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        errdefer buffer.deinit();

        // Write magic header
        try buffer.appendSlice("ZOLFUNC\x01");

        // Write number of libraries
        const lib_count = @as(u32, @intCast(self.libraries.count()));
        try buffer.appendSlice(std.mem.asBytes(&lib_count));

        // Iterate over libraries
        var lib_iter = self.libraries.iterator();
        while (lib_iter.next()) |lib_entry| {
            const lib = lib_entry.value_ptr;

            // Write library name
            const name_len = @as(u32, @intCast(lib.name.len));
            try buffer.appendSlice(std.mem.asBytes(&name_len));
            try buffer.appendSlice(lib.name);

            // Write engine
            const engine_len = @as(u32, @intCast(lib.engine.len));
            try buffer.appendSlice(std.mem.asBytes(&engine_len));
            try buffer.appendSlice(lib.engine);

            // Write code
            const code_len = @as(u32, @intCast(lib.code.len));
            try buffer.appendSlice(std.mem.asBytes(&code_len));
            try buffer.appendSlice(lib.code);

            // Write number of functions
            const func_count = @as(u32, @intCast(lib.functions.count()));
            try buffer.appendSlice(std.mem.asBytes(&func_count));

            // Iterate over functions
            var func_iter = lib.functions.iterator();
            while (func_iter.next()) |func_entry| {
                const func = func_entry.value_ptr;

                // Write function name
                const func_name_len = @as(u32, @intCast(func.name.len));
                try buffer.appendSlice(std.mem.asBytes(&func_name_len));
                try buffer.appendSlice(func.name);

                // Write description
                const desc_len = @as(u32, @intCast(func.description.len));
                try buffer.appendSlice(std.mem.asBytes(&desc_len));
                try buffer.appendSlice(func.description);

                // Write flags
                try buffer.append(func.flags);
            }
        }

        return buffer.toOwnedSlice(allocator);
    }

    /// Restore libraries from a binary payload
    /// Mode:
    ///   - .Flush: Delete all existing libraries before restoring
    ///   - .Append: Add libraries, fail if any already exist
    ///   - .Replace: Replace existing libraries with same name
    pub fn restore(self: *FunctionStore, payload: []const u8, mode: RestoreMode) !void {
        // Validate magic header
        if (payload.len < 8 or !std.mem.eql(u8, payload[0..8], "ZOLFUNC\x01")) {
            return error.InvalidPayload;
        }

        var pos: usize = 8;

        // Read number of libraries
        if (pos + 4 > payload.len) return error.InvalidPayload;
        const lib_count = std.mem.bytesToValue(u32, payload[pos..][0..4]);
        pos += 4;

        // Apply mode
        switch (mode) {
            .Flush => self.flush(),
            .Append, .Replace => {},
        }

        // Parse libraries
        for (0..lib_count) |_| {
            // Read library name
            if (pos + 4 > payload.len) return error.InvalidPayload;
            const name_len = std.mem.bytesToValue(u32, payload[pos..][0..4]);
            pos += 4;
            if (pos + name_len > payload.len) return error.InvalidPayload;
            const lib_name = payload[pos..][0..name_len];
            pos += name_len;

            // Read engine
            if (pos + 4 > payload.len) return error.InvalidPayload;
            const engine_len = std.mem.bytesToValue(u32, payload[pos..][0..4]);
            pos += 4;
            if (pos + engine_len > payload.len) return error.InvalidPayload;
            const engine = payload[pos..][0..engine_len];
            pos += engine_len;

            // Read code
            if (pos + 4 > payload.len) return error.InvalidPayload;
            const code_len = std.mem.bytesToValue(u32, payload[pos..][0..4]);
            pos += 4;
            if (pos + code_len > payload.len) return error.InvalidPayload;
            const code = payload[pos..][0..code_len];
            pos += code_len;

            // Read number of functions
            if (pos + 4 > payload.len) return error.InvalidPayload;
            const func_count = std.mem.bytesToValue(u32, payload[pos..][0..4]);
            pos += 4;

            // Create library
            var library = try Library.init(self.allocator, lib_name, engine, code);
            errdefer library.deinit();

            // Parse functions
            for (0..func_count) |_| {
                // Read function name
                if (pos + 4 > payload.len) return error.InvalidPayload;
                const func_name_len = std.mem.bytesToValue(u32, payload[pos..][0..4]);
                pos += 4;
                if (pos + func_name_len > payload.len) return error.InvalidPayload;
                const func_name = payload[pos..][0..func_name_len];
                pos += func_name_len;

                // Read description
                if (pos + 4 > payload.len) return error.InvalidPayload;
                const desc_len = std.mem.bytesToValue(u32, payload[pos..][0..4]);
                pos += 4;
                if (pos + desc_len > payload.len) return error.InvalidPayload;
                const description = payload[pos..][0..desc_len];
                pos += desc_len;

                // Read flags
                if (pos + 1 > payload.len) return error.InvalidPayload;
                const flags = payload[pos];
                pos += 1;

                // Create FunctionInfo
                var func_info = try FunctionInfo.init(self.allocator, func_name, lib_name, description);
                errdefer func_info.deinit();
                func_info.flags = flags;

                try library.addFunction(func_info);
            }

            // Add library according to mode
            switch (mode) {
                .Flush => try self.addLibrary(library),
                .Append => try self.addLibrary(library),
                .Replace => {
                    // Replace if exists, otherwise add
                    self.replaceLibrary(library) catch |err| {
                        if (err == error.LibraryNotFound) {
                            try self.addLibrary(library);
                        } else {
                            return err;
                        }
                    };
                },
            }
        }
    }

    pub const RestoreMode = enum {
        Flush, // Delete all existing libraries before restoring
        Append, // Add libraries, fail if any already exist
        Replace, // Replace existing libraries with same name
    };
};

/// Parse Shebang line from Lua code
/// Format: #!<engine> name=<library_name>
/// Example: #!lua name=mylib
pub const ShebangInfo = struct {
    engine: []const u8,
    library_name: []const u8,
};

pub fn parseShebang(code: []const u8) !ShebangInfo {
    if (code.len < 3 or !std.mem.startsWith(u8, code, "#!")) {
        return error.InvalidShebang;
    }

    // Find end of first line
    const newline_pos = std.mem.indexOfScalar(u8, code, '\n') orelse code.len;
    const shebang_line = code[2..newline_pos]; // Skip "#!"

    // Find space separator between engine and name
    const space_pos = std.mem.indexOfScalar(u8, shebang_line, ' ') orelse return error.InvalidShebang;
    const engine = std.mem.trim(u8, shebang_line[0..space_pos], " \t\r");
    const name_part = std.mem.trim(u8, shebang_line[space_pos + 1 ..], " \t\r");

    // Parse "name=<library_name>"
    if (!std.mem.startsWith(u8, name_part, "name=")) {
        return error.InvalidShebang;
    }
    const library_name = std.mem.trim(u8, name_part[5..], " \t\r");

    if (library_name.len == 0) {
        return error.InvalidShebang;
    }

    return ShebangInfo{
        .engine = engine,
        .library_name = library_name,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "FunctionInfo init and deinit" {
    const allocator = std.testing.allocator;

    var func = try FunctionInfo.init(allocator, "myfunc", "mylib", "My description");
    defer func.deinit();

    try std.testing.expectEqualStrings("myfunc", func.name);
    try std.testing.expectEqualStrings("mylib", func.library_name);
    try std.testing.expectEqualStrings("My description", func.description);
    try std.testing.expectEqual(@as(u8, 0), func.flags);
}

test "Library init and deinit" {
    const allocator = std.testing.allocator;

    var lib = try Library.init(allocator, "mylib", "lua", "function foo() return 1 end");
    defer lib.deinit();

    try std.testing.expectEqualStrings("mylib", lib.name);
    try std.testing.expectEqualStrings("lua", lib.engine);
    try std.testing.expectEqualStrings("function foo() return 1 end", lib.code);
    try std.testing.expectEqual(@as(usize, 0), lib.functions.count());
}

test "Library addFunction" {
    const allocator = std.testing.allocator;

    var lib = try Library.init(allocator, "mylib", "lua", "-- code --");
    defer lib.deinit();

    const func = try FunctionInfo.init(allocator, "myfunc", "mylib", "");
    try lib.addFunction(func);

    try std.testing.expectEqual(@as(usize, 1), lib.functions.count());
    try std.testing.expect(lib.functions.contains("myfunc"));
}

test "FunctionStore init and deinit" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    try std.testing.expectEqual(@as(usize, 0), store.libraries.count());
    try std.testing.expectEqual(@as(usize, 0), store.function_index.count());
}

test "FunctionStore addLibrary" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib = try Library.init(allocator, "mylib", "lua", "-- code --");
    const func = try FunctionInfo.init(allocator, "myfunc", "mylib", "");
    try lib.addFunction(func);

    try store.addLibrary(lib);

    try std.testing.expectEqual(@as(usize, 1), store.libraries.count());
    try std.testing.expectEqual(@as(usize, 1), store.function_index.count());
}

test "FunctionStore addLibrary duplicate library error" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib1 = try Library.init(allocator, "mylib", "lua", "-- code 1 --");
    const func1 = try FunctionInfo.init(allocator, "func1", "mylib", "");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    var lib2 = try Library.init(allocator, "mylib", "lua", "-- code 2 --");
    const func2 = try FunctionInfo.init(allocator, "func2", "mylib", "");
    try lib2.addFunction(func2);

    const result = store.addLibrary(lib2);
    try std.testing.expectError(error.LibraryExists, result);
    lib2.deinit(); // Clean up since addLibrary failed
}

test "FunctionStore addLibrary duplicate function error" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib1 = try Library.init(allocator, "lib1", "lua", "-- code 1 --");
    const func1 = try FunctionInfo.init(allocator, "samefunc", "lib1", "");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    var lib2 = try Library.init(allocator, "lib2", "lua", "-- code 2 --");
    const func2 = try FunctionInfo.init(allocator, "samefunc", "lib2", "");
    try lib2.addFunction(func2);

    const result = store.addLibrary(lib2);
    try std.testing.expectError(error.FunctionExists, result);
    lib2.deinit(); // Clean up
}

test "FunctionStore getFunction" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib = try Library.init(allocator, "mylib", "lua", "-- code --");
    const func = try FunctionInfo.init(allocator, "myfunc", "mylib", "Test desc");
    try lib.addFunction(func);
    try store.addLibrary(lib);

    const retrieved = store.getFunction("myfunc");
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqualStrings("myfunc", retrieved.?.name);
    try std.testing.expectEqualStrings("Test desc", retrieved.?.description);

    const not_found = store.getFunction("nonexistent");
    try std.testing.expect(not_found == null);
}

test "FunctionStore removeLibrary" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib = try Library.init(allocator, "mylib", "lua", "-- code --");
    const func = try FunctionInfo.init(allocator, "myfunc", "mylib", "");
    try lib.addFunction(func);
    try store.addLibrary(lib);

    try store.removeLibrary("mylib");
    try std.testing.expectEqual(@as(usize, 0), store.libraries.count());
    try std.testing.expectEqual(@as(usize, 0), store.function_index.count());
}

test "FunctionStore removeLibrary not found error" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    const result = store.removeLibrary("nonexistent");
    try std.testing.expectError(error.LibraryNotFound, result);
}

test "FunctionStore flush" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib1 = try Library.init(allocator, "lib1", "lua", "-- code 1 --");
    const func1 = try FunctionInfo.init(allocator, "func1", "lib1", "");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    var lib2 = try Library.init(allocator, "lib2", "lua", "-- code 2 --");
    const func2 = try FunctionInfo.init(allocator, "func2", "lib2", "");
    try lib2.addFunction(func2);
    try store.addLibrary(lib2);

    try std.testing.expectEqual(@as(usize, 2), store.libraries.count());
    try std.testing.expectEqual(@as(usize, 2), store.function_index.count());

    store.flush();
    try std.testing.expectEqual(@as(usize, 0), store.libraries.count());
    try std.testing.expectEqual(@as(usize, 0), store.function_index.count());
}

test "FunctionStore replaceLibrary" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    var lib1 = try Library.init(allocator, "mylib", "lua", "-- code 1 --");
    const func1 = try FunctionInfo.init(allocator, "func1", "mylib", "");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    var lib2 = try Library.init(allocator, "mylib", "lua", "-- code 2 --");
    const func2 = try FunctionInfo.init(allocator, "func2", "mylib", "");
    try lib2.addFunction(func2);
    try store.replaceLibrary(lib2);

    try std.testing.expectEqual(@as(usize, 1), store.libraries.count());
    try std.testing.expectEqual(@as(usize, 1), store.function_index.count());

    // Old function should be gone
    try std.testing.expect(store.getFunction("func1") == null);
    // New function should be present
    try std.testing.expect(store.getFunction("func2") != null);
}

test "parseShebang valid" {
    const code = "#!lua name=mylib\nfunction foo() return 1 end";
    const info = try parseShebang(code);
    try std.testing.expectEqualStrings("lua", info.engine);
    try std.testing.expectEqualStrings("mylib", info.library_name);
}

test "parseShebang with whitespace" {
    const code = "#!lua  name=mylib  \nfunction foo() return 1 end";
    const info = try parseShebang(code);
    try std.testing.expectEqualStrings("lua", info.engine);
    try std.testing.expectEqualStrings("mylib", info.library_name);
}

test "parseShebang missing prefix" {
    const code = "lua name=mylib\nfunction foo() return 1 end";
    const result = parseShebang(code);
    try std.testing.expectError(error.InvalidShebang, result);
}

test "parseShebang missing name parameter" {
    const code = "#!lua\nfunction foo() return 1 end";
    const result = parseShebang(code);
    try std.testing.expectError(error.InvalidShebang, result);
}

test "parseShebang empty library name" {
    const code = "#!lua name=\nfunction foo() return 1 end";
    const result = parseShebang(code);
    try std.testing.expectError(error.InvalidShebang, result);
}

test "parseShebang wrong format" {
    const code = "#!lua lib=mylib\nfunction foo() return 1 end";
    const result = parseShebang(code);
    try std.testing.expectError(error.InvalidShebang, result);
}

test "parseShebang no newline" {
    const code = "#!lua name=mylib";
    const info = try parseShebang(code);
    try std.testing.expectEqualStrings("lua", info.engine);
    try std.testing.expectEqualStrings("mylib", info.library_name);
}

/// Context for tracking functions being registered during FUNCTION LOAD
/// Used by redis.register_function() Lua C callback to collect function metadata
pub const FunctionRegistrationContext = struct {
    library_name: []const u8, // Library name being loaded
    functions: std.StringHashMap(FunctionMetadata), // function_name → metadata
    allocator: Allocator,

    pub const FunctionMetadata = struct {
        name: []const u8,
        description: []const u8,
        flags: u8, // Reserved for future: no-writes, allow-oom, etc.
    };

    pub fn init(allocator: Allocator, library_name: []const u8) FunctionRegistrationContext {
        return FunctionRegistrationContext{
            .library_name = library_name,
            .functions = std.StringHashMap(FunctionMetadata).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FunctionRegistrationContext) void {
        var iter = self.functions.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
            self.allocator.free(entry.value_ptr.description);
        }
        self.functions.deinit();
    }

    /// Register a function (called from redis.register_function() Lua callback)
    /// Returns error.FunctionExists if function already registered in this context
    pub fn registerFunction(self: *FunctionRegistrationContext, name: []const u8, description: []const u8, flags: u8) !void {
        if (self.functions.contains(name)) {
            return error.FunctionExists;
        }

        const metadata = FunctionMetadata{
            .name = try self.allocator.dupe(u8, name),
            .description = try self.allocator.dupe(u8, description),
            .flags = flags,
        };

        try self.functions.put(metadata.name, metadata);
    }

    /// Transfer all registered functions to a Library
    /// Caller owns the Library and must deinit it
    pub fn transferToLibrary(self: *FunctionRegistrationContext, library: *Library) !void {
        var iter = self.functions.iterator();
        while (iter.next()) |entry| {
            const metadata = entry.value_ptr;
            const func_info = try FunctionInfo.init(
                self.allocator,
                metadata.name,
                self.library_name,
                metadata.description,
            );
            try library.addFunction(func_info);
        }
    }
};

// ============================================================================
// FunctionRegistrationContext Tests
// ============================================================================

test "FunctionRegistrationContext init and deinit" {
    const allocator = std.testing.allocator;

    var ctx = FunctionRegistrationContext.init(allocator, "mylib");
    defer ctx.deinit();

    try std.testing.expectEqualStrings("mylib", ctx.library_name);
    try std.testing.expectEqual(@as(usize, 0), ctx.functions.count());
}

test "FunctionRegistrationContext registerFunction" {
    const allocator = std.testing.allocator;

    var ctx = FunctionRegistrationContext.init(allocator, "mylib");
    defer ctx.deinit();

    try ctx.registerFunction("func1", "Description 1", 0);
    try ctx.registerFunction("func2", "Description 2", 0);

    try std.testing.expectEqual(@as(usize, 2), ctx.functions.count());
    try std.testing.expect(ctx.functions.contains("func1"));
    try std.testing.expect(ctx.functions.contains("func2"));
}

test "FunctionRegistrationContext registerFunction duplicate error" {
    const allocator = std.testing.allocator;

    var ctx = FunctionRegistrationContext.init(allocator, "mylib");
    defer ctx.deinit();

    try ctx.registerFunction("func1", "Description 1", 0);

    const result = ctx.registerFunction("func1", "Different description", 0);
    try std.testing.expectError(error.FunctionExists, result);
}

test "FunctionRegistrationContext transferToLibrary" {
    const allocator = std.testing.allocator;

    var ctx = FunctionRegistrationContext.init(allocator, "mylib");
    defer ctx.deinit();

    try ctx.registerFunction("func1", "Desc 1", 0);
    try ctx.registerFunction("func2", "Desc 2", 0);

    var lib = try Library.init(allocator, "mylib", "lua", "-- code --");
    defer lib.deinit();

    try ctx.transferToLibrary(&lib);

    try std.testing.expectEqual(@as(usize, 2), lib.functions.count());
    try std.testing.expect(lib.functions.contains("func1"));
    try std.testing.expect(lib.functions.contains("func2"));
}

// ============================================================================
// FunctionStore Dump/Restore Tests
// ============================================================================

test "FunctionStore dump empty store" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    const payload = try store.dump(allocator);
    defer allocator.free(payload);

    // Should contain magic + 0 libraries
    try std.testing.expectEqual(@as(usize, 12), payload.len); // 8 bytes magic + 4 bytes count
    try std.testing.expectEqualSlices(u8, "ZOLFUNC\x01", payload[0..8]);
}

test "FunctionStore dump and restore single library" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Add a library with functions
    var lib = try Library.init(allocator, "mylib", "lua", "function foo() return 1 end");
    const func1 = try FunctionInfo.init(allocator, "func1", "mylib", "First function");
    const func2 = try FunctionInfo.init(allocator, "func2", "mylib", "Second function");
    try lib.addFunction(func1);
    try lib.addFunction(func2);
    try store.addLibrary(lib);

    // Dump
    const payload = try store.dump(allocator);
    defer allocator.free(payload);

    // Restore to new store
    var store2 = FunctionStore.init(allocator);
    defer store2.deinit();

    try store2.restore(payload, .Append);

    // Verify restored library
    try std.testing.expectEqual(@as(usize, 1), store2.libraries.count());
    try std.testing.expectEqual(@as(usize, 2), store2.function_index.count());

    const restored_lib = store2.getLibrary("mylib");
    try std.testing.expect(restored_lib != null);
    try std.testing.expectEqualStrings("lua", restored_lib.?.engine);
    try std.testing.expectEqualStrings("function foo() return 1 end", restored_lib.?.code);

    const restored_func1 = store2.getFunction("func1");
    try std.testing.expect(restored_func1 != null);
    try std.testing.expectEqualStrings("First function", restored_func1.?.description);

    const restored_func2 = store2.getFunction("func2");
    try std.testing.expect(restored_func2 != null);
    try std.testing.expectEqualStrings("Second function", restored_func2.?.description);
}

test "FunctionStore dump and restore multiple libraries" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Add first library
    var lib1 = try Library.init(allocator, "lib1", "lua", "-- code 1 --");
    const func1 = try FunctionInfo.init(allocator, "func1", "lib1", "Desc 1");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    // Add second library
    var lib2 = try Library.init(allocator, "lib2", "lua", "-- code 2 --");
    const func2 = try FunctionInfo.init(allocator, "func2", "lib2", "Desc 2");
    try lib2.addFunction(func2);
    try store.addLibrary(lib2);

    // Dump
    const payload = try store.dump(allocator);
    defer allocator.free(payload);

    // Restore to new store
    var store2 = FunctionStore.init(allocator);
    defer store2.deinit();

    try store2.restore(payload, .Append);

    // Verify both libraries restored
    try std.testing.expectEqual(@as(usize, 2), store2.libraries.count());
    try std.testing.expectEqual(@as(usize, 2), store2.function_index.count());
    try std.testing.expect(store2.getLibrary("lib1") != null);
    try std.testing.expect(store2.getLibrary("lib2") != null);
    try std.testing.expect(store2.getFunction("func1") != null);
    try std.testing.expect(store2.getFunction("func2") != null);
}

test "FunctionStore restore with FLUSH mode" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Add a library
    var lib1 = try Library.init(allocator, "lib1", "lua", "-- code 1 --");
    const func1 = try FunctionInfo.init(allocator, "func1", "lib1", "");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    const payload = try store.dump(allocator);
    defer allocator.free(payload);

    // Add another library to the store
    var lib2 = try Library.init(allocator, "lib2", "lua", "-- code 2 --");
    const func2 = try FunctionInfo.init(allocator, "func2", "lib2", "");
    try lib2.addFunction(func2);
    try store.addLibrary(lib2);

    try std.testing.expectEqual(@as(usize, 2), store.libraries.count());

    // Restore with FLUSH should remove lib2 and restore lib1
    try store.restore(payload, .Flush);

    try std.testing.expectEqual(@as(usize, 1), store.libraries.count());
    try std.testing.expect(store.getLibrary("lib1") != null);
    try std.testing.expect(store.getLibrary("lib2") == null);
}

test "FunctionStore restore with APPEND mode duplicate error" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Add a library
    var lib = try Library.init(allocator, "lib1", "lua", "-- code --");
    const func = try FunctionInfo.init(allocator, "func1", "lib1", "");
    try lib.addFunction(func);
    try store.addLibrary(lib);

    const payload = try store.dump(allocator);
    defer allocator.free(payload);

    // Attempt to restore with APPEND should fail (library already exists)
    const result = store.restore(payload, .Append);
    try std.testing.expectError(error.LibraryExists, result);
}

test "FunctionStore restore with REPLACE mode" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Add first version of library
    var lib1 = try Library.init(allocator, "mylib", "lua", "-- old code --");
    const func1 = try FunctionInfo.init(allocator, "func1", "mylib", "Old desc");
    try lib1.addFunction(func1);
    try store.addLibrary(lib1);

    // Create second version payload
    var store2 = FunctionStore.init(allocator);
    defer store2.deinit();
    var lib2 = try Library.init(allocator, "mylib", "lua", "-- new code --");
    const func2 = try FunctionInfo.init(allocator, "func2", "mylib", "New desc");
    try lib2.addFunction(func2);
    try store2.addLibrary(lib2);

    const payload = try store2.dump(allocator);
    defer allocator.free(payload);

    // Restore with REPLACE should replace old library
    try store.restore(payload, .Replace);

    try std.testing.expectEqual(@as(usize, 1), store.libraries.count());
    const restored = store.getLibrary("mylib");
    try std.testing.expect(restored != null);
    try std.testing.expectEqualStrings("-- new code --", restored.?.code);

    // Old function should be gone
    try std.testing.expect(store.getFunction("func1") == null);
    // New function should be present
    try std.testing.expect(store.getFunction("func2") != null);
}

test "FunctionStore restore invalid payload" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Invalid magic
    const invalid_magic = "INVALID\x01";
    const result1 = store.restore(invalid_magic, .Append);
    try std.testing.expectError(error.InvalidPayload, result1);

    // Too short
    const too_short = "ZOL";
    const result2 = store.restore(too_short, .Append);
    try std.testing.expectError(error.InvalidPayload, result2);
}

test "FunctionStore restore truncated payload" {
    const allocator = std.testing.allocator;

    var store = FunctionStore.init(allocator);
    defer store.deinit();

    // Add a library and dump
    var lib = try Library.init(allocator, "mylib", "lua", "-- code --");
    const func = try FunctionInfo.init(allocator, "func1", "mylib", "");
    try lib.addFunction(func);
    try store.addLibrary(lib);

    const full_payload = try store.dump(allocator);
    defer allocator.free(full_payload);

    // Create truncated payload (remove last 10 bytes)
    const truncated = full_payload[0 .. full_payload.len - 10];

    var store2 = FunctionStore.init(allocator);
    defer store2.deinit();

    const result = store2.restore(truncated, .Append);
    try std.testing.expectError(error.InvalidPayload, result);
}
