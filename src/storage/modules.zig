const std = @import("std");

/// Module initialization context passed to RedisModule_OnLoad
pub const ModuleCtx = struct {
    name: []const u8,
    ver: i32,
};

/// Function signature for module OnLoad function
/// Redis module ABI: int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
pub const OnLoadFn = *const fn (ctx: *ModuleCtx, argv: [*]const [*:0]const u8, argc: c_int) c_int;

/// Function signature for module OnUnload function (optional)
/// Redis module ABI: int RedisModule_OnUnload(RedisModuleCtx *ctx)
pub const OnUnloadFn = *const fn (ctx: *ModuleCtx) c_int;

/// Information about a loaded module
pub const ModuleInfo = struct {
    name: []const u8,
    ver: i32,
    path: []const u8,
    args: [][]const u8,
    /// Dynamic library handle (null if module is not loaded from library)
    lib: ?std.DynLib,

    /// Deallocate ModuleInfo structure and all owned strings
    pub fn deinit(self: *ModuleInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.path);
        for (self.args) |arg| {
            allocator.free(arg);
        }
        allocator.free(self.args);
        // Close dynamic library if loaded
        if (self.lib) |*lib| {
            lib.close();
        }
    }
};

/// Error types for module operations
pub const ModuleError = error{
    /// Dynamic library loading not supported in this implementation
    NotSupported,
    /// Module already loaded
    AlreadyLoaded,
    /// Module not found
    NotFound,
    /// Invalid module path
    InvalidPath,
    /// Failed to open dynamic library
    LibraryOpenFailed,
    /// Required symbol not found in module
    SymbolNotFound,
    /// Module initialization failed
    InitFailed,
    /// Module unload failed
    UnloadFailed,
};

/// Storage for dynamically loaded modules
pub const ModuleStore = struct {
    allocator: std.mem.Allocator,
    modules: std.StringHashMap(ModuleInfo),

    /// Initialize a new ModuleStore
    ///
    /// Arguments:
    ///   - allocator: Memory allocator for module storage
    ///
    /// Returns a new ModuleStore instance
    pub fn init(allocator: std.mem.Allocator) ModuleStore {
        return ModuleStore{
            .allocator = allocator,
            .modules = std.StringHashMap(ModuleInfo).init(allocator),
        };
    }

    /// Deallocate ModuleStore and all contained modules
    pub fn deinit(self: *ModuleStore) void {
        var it = self.modules.keyIterator();
        while (it.next()) |key| {
            self.allocator.free(key.*);
        }

        var val_it = self.modules.valueIterator();
        while (val_it.next()) |value| {
            value.deinit(self.allocator);
        }

        self.modules.deinit();
    }

    /// Load a module from the specified path
    ///
    /// Opens a dynamic library (.so/.dylib/.dll), calls RedisModule_OnLoad,
    /// and registers the module in the module store.
    ///
    /// Arguments:
    ///   - path: Path to the module binary (.so/.dylib file)
    ///   - args: Optional arguments to pass to module on load
    ///
    /// Returns:
    ///   - void on success
    ///   - ModuleError if loading fails
    pub fn loadModule(
        self: *ModuleStore,
        path: []const u8,
        args: []const []const u8,
    ) !void {
        // Validate path is not empty
        if (path.len == 0) {
            return ModuleError.InvalidPath;
        }

        // Extract module name from path (filename without extension)
        const name = extractModuleName(path);

        // Check if module already loaded
        if (self.modules.contains(name)) {
            return ModuleError.AlreadyLoaded;
        }

        // Open the dynamic library
        var lib = std.DynLib.open(path) catch {
            return ModuleError.LibraryOpenFailed;
        };
        errdefer lib.close();

        // Look up the OnLoad function
        const onload_fn = lib.lookup(OnLoadFn, "RedisModule_OnLoad") orelse {
            return ModuleError.SymbolNotFound;
        };

        // Create module context for OnLoad call
        var ctx = ModuleCtx{
            .name = name,
            .ver = 1, // Module API version
        };

        // Convert args to C-compatible format (null-terminated strings)
        var c_args = try self.allocator.alloc([*:0]const u8, args.len);
        defer self.allocator.free(c_args);

        for (args, 0..) |arg, i| {
            const c_str = try self.allocator.dupeZ(u8, arg);
            defer self.allocator.free(c_str);
            c_args[i] = c_str.ptr;
        }

        // Call module OnLoad function
        const result = onload_fn(&ctx, c_args.ptr, @intCast(args.len));
        if (result != 0) {
            return ModuleError.InitFailed;
        }

        // Create owned copies of all strings
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        const owned_args = try self.allocator.alloc([]const u8, args.len);
        errdefer self.allocator.free(owned_args);

        for (args, 0..) |arg, i| {
            owned_args[i] = try self.allocator.dupe(u8, arg);
        }
        errdefer {
            for (owned_args, 0..) |arg, i| {
                if (i < args.len) self.allocator.free(arg);
            }
        }

        // Store module info
        const module_info = ModuleInfo{
            .name = owned_name,
            .ver = ctx.ver,
            .path = owned_path,
            .args = owned_args,
            .lib = lib,
        };

        try self.modules.put(owned_name, module_info);
    }

    /// Extract module name from path
    /// Returns the filename without directory and extension
    fn extractModuleName(path: []const u8) []const u8 {
        // Find last path separator
        var start: usize = 0;
        if (std.mem.lastIndexOfScalar(u8, path, '/')) |idx| {
            start = idx + 1;
        } else if (std.mem.lastIndexOfScalar(u8, path, '\\')) |idx| {
            start = idx + 1;
        }

        // Find extension
        var end: usize = path.len;
        if (std.mem.lastIndexOfScalar(u8, path[start..], '.')) |idx| {
            end = start + idx;
        }

        return path[start..end];
    }

    /// Unload a module by name
    ///
    /// Calls RedisModule_OnUnload if present, closes the dynamic library,
    /// and removes the module from the module store.
    ///
    /// Arguments:
    ///   - name: Module name to unload
    ///
    /// Returns:
    ///   - void on success
    ///   - ModuleError.NotFound if module is not loaded
    pub fn unloadModule(self: *ModuleStore, name: []const u8) !void {
        // Get module entry
        const entry = self.modules.fetchRemove(name) orelse {
            return ModuleError.NotFound;
        };

        var module_info = entry.value;
        defer module_info.deinit(self.allocator);

        // Free the key string (owned by HashMap)
        self.allocator.free(entry.key);

        // Call OnUnload if the module has a dynamic library
        if (module_info.lib) |*lib| {
            // Look up optional OnUnload function
            if (lib.lookup(OnUnloadFn, "RedisModule_OnUnload")) |onunload_fn| {
                var ctx = ModuleCtx{
                    .name = module_info.name,
                    .ver = module_info.ver,
                };

                // Call module OnUnload function (ignore result - best effort cleanup)
                _ = onunload_fn(&ctx);
            }
            // Library will be closed by module_info.deinit()
        }
    }

    /// List all loaded modules
    ///
    /// Returns: Slice of ModuleInfo for all loaded modules (owned by caller)
    ///          Empty slice if no modules loaded
    pub fn listModules(self: *ModuleStore) ![]const ModuleInfo {
        var modules = try std.ArrayList(ModuleInfo).initCapacity(self.allocator, self.modules.count());
        errdefer modules.deinit(self.allocator);

        var it = self.modules.valueIterator();
        while (it.next()) |value| {
            try modules.append(self.allocator, value.*);
        }

        return try modules.toOwnedSlice(self.allocator);
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

test "ModuleStore: init and deinit lifecycle" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Verify empty state
    const list = try store.listModules();
    defer allocator.free(list);
    try testing.expectEqual(@as(usize, 0), list.len);
}

test "ModuleStore: loadModule with invalid path" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Empty path should return InvalidPath
    const result = store.loadModule("", &[_][]const u8{});
    try testing.expectError(error.InvalidPath, result);
}

test "ModuleStore: loadModule with nonexistent file" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Nonexistent file should return LibraryOpenFailed
    const result = store.loadModule("/nonexistent/module.so", &[_][]const u8{});
    try testing.expectError(error.LibraryOpenFailed, result);
}

test "ModuleStore: unloadModule with nonexistent module" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Unloading nonexistent module should return NotFound
    const result = store.unloadModule("mymodule");
    try testing.expectError(error.NotFound, result);
}

test "ModuleStore: extractModuleName from various paths" {
    // Unix path with extension
    try testing.expectEqualStrings("mymodule", ModuleStore.extractModuleName("/path/to/mymodule.so"));

    // Windows path with extension
    try testing.expectEqualStrings("mymodule", ModuleStore.extractModuleName("C:\\path\\to\\mymodule.dll"));

    // Filename only with extension
    try testing.expectEqualStrings("mymodule", ModuleStore.extractModuleName("mymodule.so"));

    // Filename without extension
    try testing.expectEqualStrings("mymodule", ModuleStore.extractModuleName("mymodule"));

    // Complex path
    try testing.expectEqualStrings("my.module", ModuleStore.extractModuleName("/usr/lib/redis/my.module.so"));
}

test "ModuleStore: listModules returns empty slice" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    const list = try store.listModules();
    defer allocator.free(list);
    try testing.expectEqual(@as(usize, 0), list.len);
}

test "ModuleStore: ModuleInfo structure with fields" {
    const allocator = testing.allocator;

    const name = try allocator.dupe(u8, "testmodule");
    const path = try allocator.dupe(u8, "/path/to/module.so");
    const args = try allocator.alloc([]const u8, 0);

    var info = ModuleInfo{
        .name = name,
        .ver = 1,
        .path = path,
        .args = args,
        .lib = null, // No dynamic library in this test
    };
    defer info.deinit(allocator);

    try testing.expectEqualStrings("testmodule", info.name);
    try testing.expectEqual(@as(i32, 1), info.ver);
    try testing.expectEqualStrings("/path/to/module.so", info.path);
    try testing.expectEqual(@as(usize, 0), info.args.len);
    try testing.expect(info.lib == null);
}

test "ModuleStore: uses StringHashMap for modules" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Verify modules field exists and is a struct (StringHashMap is a struct)
    const TypeInfo = @typeInfo(@TypeOf(store.modules));
    try testing.expect(TypeInfo == .@"struct");
}
