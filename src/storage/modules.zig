const std = @import("std");
const RespValue = @import("../protocol/parser.zig").RespValue;
const Storage = @import("memory.zig").Storage;

/// Module initialization context passed to RedisModule_OnLoad
pub const ModuleCtx = struct {
    name: []const u8,
    ver: i32,
    store: *ModuleStore,

    /// Register a new command from a module
    /// Redis module ABI: int RedisModule_CreateCommand(RedisModuleCtx *ctx, const char *name, RedisModuleCmdFunc cmdfunc, const char *strflags, int firstkey, int lastkey, int keystep)
    pub fn createCommand(
        self: *ModuleCtx,
        name: []const u8,
        cmdfunc: ModuleCmdFunc,
        flags: []const u8,
        firstkey: i32,
        lastkey: i32,
        keystep: i32,
    ) !void {
        return self.store.registerCommand(self.name, name, cmdfunc, flags, firstkey, lastkey, keystep);
    }
};

/// Function signature for module OnLoad function
/// Redis module ABI: int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
pub const OnLoadFn = *const fn (ctx: *ModuleCtx, argv: [*]const [*:0]const u8, argc: c_int) c_int;

/// Function signature for module OnUnload function (optional)
/// Redis module ABI: int RedisModule_OnUnload(RedisModuleCtx *ctx)
pub const OnUnloadFn = *const fn (ctx: *ModuleCtx) c_int;

/// Function signature for module command handler
/// Redis module ABI: int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
///
/// We use a simpler signature internally - modules convert their C ABI to this
pub const ModuleCmdFunc = *const fn (allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) anyerror![]const u8;

/// Information about a registered module command
pub const ModuleCommand = struct {
    name: []const u8,
    module_name: []const u8,
    cmdfunc: ModuleCmdFunc,
    flags: []const u8,
    firstkey: i32,
    lastkey: i32,
    keystep: i32,

    pub fn deinit(self: *ModuleCommand, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.module_name);
        allocator.free(self.flags);
    }
};

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
    /// Command already registered
    CommandAlreadyExists,
    /// Invalid command name
    InvalidCommandName,
};

/// Storage for dynamically loaded modules
pub const ModuleStore = struct {
    allocator: std.mem.Allocator,
    modules: std.StringHashMap(ModuleInfo),
    commands: std.StringHashMap(ModuleCommand),

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
            .commands = std.StringHashMap(ModuleCommand).init(allocator),
        };
    }

    /// Deallocate ModuleStore and all contained modules
    pub fn deinit(self: *ModuleStore) void {
        // Free module keys
        var it = self.modules.keyIterator();
        while (it.next()) |key| {
            self.allocator.free(key.*);
        }

        // Free module values
        var val_it = self.modules.valueIterator();
        while (val_it.next()) |value| {
            value.deinit(self.allocator);
        }

        self.modules.deinit();

        // Free command keys
        var cmd_key_it = self.commands.keyIterator();
        while (cmd_key_it.next()) |key| {
            self.allocator.free(key.*);
        }

        // Free command values
        var cmd_val_it = self.commands.valueIterator();
        while (cmd_val_it.next()) |value| {
            value.deinit(self.allocator);
        }

        self.commands.deinit();
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
            .store = self,
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
    /// removes all registered commands, and removes the module from the module store.
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
                    .store = self,
                };

                // Call module OnUnload function (ignore result - best effort cleanup)
                _ = onunload_fn(&ctx);
            }
            // Library will be closed by module_info.deinit()
        }

        // Remove all commands registered by this module
        self.removeModuleCommands(module_info.name);
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

    /// Register a new command from a module
    ///
    /// Called by modules via RedisModule_CreateCommand during OnLoad.
    ///
    /// Arguments:
    ///   - module_name: Name of the module registering the command
    ///   - cmd_name: Name of the command to register
    ///   - cmdfunc: Function pointer to command handler
    ///   - flags: Command flags (e.g., "write", "readonly", "fast")
    ///   - firstkey: First key argument position (0 if no keys)
    ///   - lastkey: Last key argument position (-1 for all remaining)
    ///   - keystep: Step between key arguments (1 for consecutive keys)
    ///
    /// Returns:
    ///   - void on success
    ///   - ModuleError.CommandAlreadyExists if command name already registered
    ///   - ModuleError.InvalidCommandName if command name is empty
    pub fn registerCommand(
        self: *ModuleStore,
        module_name: []const u8,
        cmd_name: []const u8,
        cmdfunc: ModuleCmdFunc,
        flags: []const u8,
        firstkey: i32,
        lastkey: i32,
        keystep: i32,
    ) !void {
        // Validate command name
        if (cmd_name.len == 0) {
            return ModuleError.InvalidCommandName;
        }

        // Check if command already exists
        if (self.commands.contains(cmd_name)) {
            return ModuleError.CommandAlreadyExists;
        }

        // Create owned copies of strings
        const owned_name = try self.allocator.dupe(u8, cmd_name);
        errdefer self.allocator.free(owned_name);

        const owned_module_name = try self.allocator.dupe(u8, module_name);
        errdefer self.allocator.free(owned_module_name);

        const owned_flags = try self.allocator.dupe(u8, flags);
        errdefer self.allocator.free(owned_flags);

        // Create command info
        const cmd = ModuleCommand{
            .name = owned_name,
            .module_name = owned_module_name,
            .cmdfunc = cmdfunc,
            .flags = owned_flags,
            .firstkey = firstkey,
            .lastkey = lastkey,
            .keystep = keystep,
        };

        // Store command
        try self.commands.put(owned_name, cmd);
    }

    /// Get a registered command by name
    ///
    /// Arguments:
    ///   - name: Command name to look up
    ///
    /// Returns:
    ///   - Pointer to ModuleCommand if found
    ///   - null if command not registered
    pub fn getCommand(self: *ModuleStore, name: []const u8) ?*const ModuleCommand {
        return self.commands.getPtr(name);
    }

    /// Remove all commands registered by a module
    ///
    /// Called during module unload to clean up all commands.
    ///
    /// Arguments:
    ///   - module_name: Name of the module whose commands to remove
    pub fn removeModuleCommands(self: *ModuleStore, module_name: []const u8) void {
        // Collect command names to remove (can't modify HashMap while iterating)
        var to_remove = std.ArrayList([]const u8).init(self.allocator);
        defer to_remove.deinit();

        var it = self.commands.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.module_name, module_name)) {
                to_remove.append(entry.key_ptr.*) catch continue;
            }
        }

        // Remove collected commands
        for (to_remove.items) |cmd_name| {
            if (self.commands.fetchRemove(cmd_name)) |entry| {
                self.allocator.free(entry.key);
                entry.value.deinit(self.allocator);
            }
        }
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

// Mock command handler for testing
fn mockCommandHandler(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) anyerror![]const u8 {
    _ = storage;
    _ = args;
    return try allocator.dupe(u8, "+OK\r\n");
}

test "ModuleStore: registerCommand with valid command" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register a command
    try store.registerCommand("mymodule", "MYCOMMAND", mockCommandHandler, "write", 1, 1, 1);

    // Verify command is registered
    const cmd = store.getCommand("MYCOMMAND");
    try testing.expect(cmd != null);
    if (cmd) |c| {
        try testing.expectEqualStrings("MYCOMMAND", c.name);
        try testing.expectEqualStrings("mymodule", c.module_name);
        try testing.expectEqualStrings("write", c.flags);
        try testing.expectEqual(@as(i32, 1), c.firstkey);
        try testing.expectEqual(@as(i32, 1), c.lastkey);
        try testing.expectEqual(@as(i32, 1), c.keystep);
    }
}

test "ModuleStore: registerCommand with empty name" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Empty command name should fail
    const result = store.registerCommand("mymodule", "", mockCommandHandler, "write", 0, 0, 0);
    try testing.expectError(error.InvalidCommandName, result);
}

test "ModuleStore: registerCommand with duplicate command" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register command
    try store.registerCommand("mymodule", "MYCOMMAND", mockCommandHandler, "write", 1, 1, 1);

    // Duplicate registration should fail
    const result = store.registerCommand("other", "MYCOMMAND", mockCommandHandler, "readonly", 0, 0, 0);
    try testing.expectError(error.CommandAlreadyExists, result);
}

test "ModuleStore: getCommand returns null for nonexistent command" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Lookup nonexistent command
    const cmd = store.getCommand("NONEXISTENT");
    try testing.expect(cmd == null);
}

test "ModuleStore: removeModuleCommands removes all module commands" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register multiple commands from same module
    try store.registerCommand("mymodule", "CMD1", mockCommandHandler, "write", 1, 1, 1);
    try store.registerCommand("mymodule", "CMD2", mockCommandHandler, "readonly", 0, 0, 0);
    try store.registerCommand("other", "CMD3", mockCommandHandler, "fast", 0, 0, 0);

    // Remove mymodule commands
    store.removeModuleCommands("mymodule");

    // Verify mymodule commands are removed
    try testing.expect(store.getCommand("CMD1") == null);
    try testing.expect(store.getCommand("CMD2") == null);

    // Verify other module command still exists
    try testing.expect(store.getCommand("CMD3") != null);
}

test "ModuleStore: ModuleCtx createCommand method" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Create context
    var ctx = ModuleCtx{
        .name = "testmodule",
        .ver = 1,
        .store = &store,
    };

    // Register command via context method
    try ctx.createCommand("TESTCMD", mockCommandHandler, "write", 1, -1, 1);

    // Verify command is registered
    const cmd = store.getCommand("TESTCMD");
    try testing.expect(cmd != null);
    if (cmd) |c| {
        try testing.expectEqualStrings("testmodule", c.module_name);
    }
}
