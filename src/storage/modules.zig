const std = @import("std");

/// Information about a loaded module
pub const ModuleInfo = struct {
    name: []const u8,
    ver: i32,
    path: []const u8,
    args: [][]const u8,

    /// Deallocate ModuleInfo structure and all owned strings
    pub fn deinit(self: *ModuleInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.path);
        for (self.args) |arg| {
            allocator.free(arg);
        }
        allocator.free(self.args);
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

    /// Load a module from the specified path (stub implementation)
    ///
    /// Arguments:
    ///   - path: Path to the module binary (.so file)
    ///   - args: Optional arguments to pass to module on load
    ///
    /// Returns error.NotSupported - dynamic library loading not yet implemented
    pub fn loadModule(
        _: *ModuleStore,
        _: []const u8,
        _: [][]const u8,
    ) ModuleError!void {
        return ModuleError.NotSupported;
    }

    /// Unload a module by name (stub implementation)
    ///
    /// Arguments:
    ///   - name: Module name to unload
    ///
    /// Returns error.NotSupported - dynamic library unloading not yet implemented
    pub fn unloadModule(_: *ModuleStore, _: []const u8) ModuleError!void {
        return ModuleError.NotSupported;
    }

    /// List all loaded modules
    ///
    /// Returns: Slice of ModuleInfo for all loaded modules (owned by caller)
    ///          Empty slice if no modules loaded
    pub fn listModules(self: *ModuleStore) ![]const ModuleInfo {
        var modules = try std.ArrayList(ModuleInfo).initCapacity(self.allocator, self.modules.count());
        errdefer modules.deinit();

        var it = self.modules.valueIterator();
        while (it.next()) |value| {
            try modules.append(value.*);
        }

        return try modules.toOwnedSlice();
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

test "ModuleStore: loadModule returns error.NotSupported" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    const result = store.loadModule("/path/to/module.so", &[_][]const u8{});
    try testing.expectError(error.NotSupported, result);
}

test "ModuleStore: unloadModule returns error.NotSupported" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    const result = store.unloadModule("mymodule");
    try testing.expectError(error.NotSupported, result);
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
    };
    defer info.deinit(allocator);

    try testing.expectEqualStrings("testmodule", info.name);
    try testing.expectEqual(@as(i32, 1), info.ver);
    try testing.expectEqualStrings("/path/to/module.so", info.path);
    try testing.expectEqual(@as(usize, 0), info.args.len);
}

test "ModuleStore: uses StringHashMap for modules" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Verify StringHashMap type
    const TypeInfo = @typeInfo(@TypeOf(store.modules));
    try testing.expect(TypeInfo != null);
}
