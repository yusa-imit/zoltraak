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

    /// Register a new data type from a module
    /// Redis module ABI: RedisModuleType *RedisModule_CreateDataType(RedisModuleCtx *ctx, const char *name, int encver, RedisModuleTypeMethods *typemethods)
    pub fn createDataType(
        self: *ModuleCtx,
        name: []const u8,
        encver: u32,
        rdb_save: ?RdbSaveFn,
        rdb_load: ?RdbLoadFn,
        aof_rewrite: ?AofRewriteFn,
        mem_usage: ?MemUsageFn,
        free_fn: FreeFn,
    ) !void {
        return self.store.registerDataType(
            self.name,
            name,
            encver,
            rdb_save,
            rdb_load,
            aof_rewrite,
            mem_usage,
            free_fn,
        );
    }

    /// Block a client from a module command
    /// Redis module ABI: RedisModuleBlockedClient *RedisModule_BlockClient(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_callback, RedisModuleCmdFunc timeout_callback, void (*free_privdata)(void*), long long timeout_ms)
    pub fn blockClient(
        self: *ModuleCtx,
        timeout_ms: i64,
        privdata: ?*anyopaque,
        timeout_fn: ?BlockedClientTimeoutFn,
        reply_cb: ?BlockedClientReplyCb,
        disconnect_cb: ?BlockedClientDisconnectCb,
        free_privdata: ?FreeFn,
    ) !u64 {
        return self.store.blocking.blockClient(
            self.name,
            timeout_ms,
            privdata,
            timeout_fn,
            reply_cb,
            disconnect_cb,
            free_privdata,
        );
    }

    /// Unblock a previously blocked client
    /// Redis module ABI: int RedisModule_UnblockClient(RedisModuleBlockedClient *bc, void *privdata)
    pub fn unblockClient(
        self: *ModuleCtx,
        client_id: u64,
        storage: *Storage,
    ) !?[]const u8 {
        return self.store.blocking.unblockClient(client_id, storage);
    }

    /// Register a hook for event notifications
    /// Redis module ABI: int RedisModule_SubscribeToServerEvent(RedisModuleCtx *ctx, RedisModuleEvent event, RedisModuleEventCallback callback)
    pub fn registerHook(
        self: *ModuleCtx,
        hook_type: HookType,
        callback: HookCallback,
        context: ?*anyopaque,
    ) !void {
        return self.store.registerHook(hook_type, self.name, callback, context);
    }

    /// Create a timer that fires after specified period
    /// Redis module ABI: RedisModuleTimerID RedisModule_CreateTimer(RedisModuleCtx *ctx, mstime_t period, RedisModuleTimerProc callback, void *data)
    pub fn createTimer(
        self: *ModuleCtx,
        period_ms: i64,
        data: ?*anyopaque,
        callback: TimerCallback,
        free_data: ?FreeFn,
    ) !u64 {
        return self.store.timers.createTimer(self.name, period_ms, data, callback, free_data);
    }

    /// Stop a previously created timer
    /// Redis module ABI: int RedisModule_StopTimer(RedisModuleCtx *ctx, RedisModuleTimerID id, void **data)
    pub fn stopTimer(
        self: *ModuleCtx,
        timer_id: u64,
    ) !void {
        return self.store.timers.stopTimer(timer_id);
    }

    /// Get a thread-safe context for background thread command execution
    /// Redis module ABI: RedisModuleCtx *RedisModule_GetThreadSafeContext(RedisModuleBlockedClient *bc)
    pub fn getThreadSafeContext(
        self: *ModuleCtx,
        allocator: std.mem.Allocator,
    ) !*ThreadSafeContext {
        return ThreadSafeContext.init(allocator, &self.store.global_lock);
    }

    /// Get a thread-safe context bound to a blocked client
    /// Replies will be accumulated and delivered when the client is unblocked
    pub fn getThreadSafeContextBound(
        self: *ModuleCtx,
        allocator: std.mem.Allocator,
        client_id: u64,
    ) !*ThreadSafeContext {
        return ThreadSafeContext.initBound(allocator, &self.store.global_lock, client_id);
    }

    /// Free a thread-safe context
    /// Redis module ABI: void RedisModule_FreeThreadSafeContext(RedisModuleCtx *ctx)
    pub fn freeThreadSafeContext(
        self: *ModuleCtx,
        allocator: std.mem.Allocator,
        ctx: *ThreadSafeContext,
    ) !void {
        _ = self; // unused
        return ctx.deinit(allocator);
    }

    /// Lock a thread-safe context to access Redis dataset
    /// Redis module ABI: void RedisModule_ThreadSafeContextLock(RedisModuleCtx *ctx)
    pub fn lockThreadSafeContext(
        self: *ModuleCtx,
        ctx: *ThreadSafeContext,
    ) void {
        _ = self; // unused
        ctx.lockContext();
    }

    /// Unlock a thread-safe context
    /// Redis module ABI: void RedisModule_ThreadSafeContextUnlock(RedisModuleCtx *ctx)
    pub fn unlockThreadSafeContext(
        self: *ModuleCtx,
        ctx: *ThreadSafeContext,
    ) void {
        _ = self; // unused
        ctx.unlockContext();
    }

    /// Try to lock a thread-safe context without blocking
    /// Redis module ABI: int RedisModule_ThreadSafeContextTryLock(RedisModuleCtx *ctx)
    pub fn tryLockThreadSafeContext(
        self: *ModuleCtx,
        ctx: *ThreadSafeContext,
    ) bool {
        _ = self; // unused
        return ctx.tryLockContext();
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

/// Function signature for RDB save callback
/// Redis module ABI: void (*rdb_save)(RedisModuleIO *io, void *value)
pub const RdbSaveFn = *const fn (data: *anyopaque, writer: std.io.AnyWriter) anyerror!void;

/// Function signature for RDB load callback
/// Redis module ABI: void *(*rdb_load)(RedisModuleIO *io, int encver)
pub const RdbLoadFn = *const fn (allocator: std.mem.Allocator, reader: std.io.AnyReader, encver: u32) anyerror!*anyopaque;

/// Function signature for AOF rewrite callback
/// Redis module ABI: void (*aof_rewrite)(RedisModuleIO *io, RedisModuleString *key, void *value)
pub const AofRewriteFn = *const fn (key: []const u8, data: *anyopaque, writer: std.io.AnyWriter) anyerror!void;

/// Function signature for memory usage callback
/// Redis module ABI: size_t (*mem_usage)(const void *value)
pub const MemUsageFn = *const fn (data: *const anyopaque) usize;

/// Function signature for free callback
/// Redis module ABI: void (*free)(void *value)
pub const FreeFn = *const fn (allocator: std.mem.Allocator, data: *anyopaque) void;

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

/// Information about a registered module data type
/// Redis module ABI: RedisModuleType
pub const ModuleDataType = struct {
    name: []const u8,
    module_name: []const u8,
    encver: u32,
    rdb_save: ?RdbSaveFn,
    rdb_load: ?RdbLoadFn,
    aof_rewrite: ?AofRewriteFn,
    mem_usage: ?MemUsageFn,
    free: FreeFn,

    /// Deallocate ModuleDataType structure and owned strings
    pub fn deinit(self: *ModuleDataType, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.module_name);
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

/// Types of events that can trigger module hooks
pub const HookType = enum {
    /// Keyspace notifications (key added/modified/deleted)
    keyspace_notification,
    /// Command filter (before/after command execution)
    command_filter,
    /// Client state change (connect/disconnect)
    client_state_change,
};

/// Information about a key for hook callbacks
pub const KeyInfo = struct {
    key: []const u8,
    db: i32,
};

/// Function signature for module hook callback
/// Redis module ABI: void (*hook)(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data)
///
/// Simplified signature for internal use:
/// - ctx: opaque context pointer (module-specific data)
/// - event_type: numeric event type identifier
/// - event: event name string
/// - key: key information for the event
pub const HookCallback = *const fn (ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void;

/// A registered module hook
pub const ModuleHook = struct {
    module_name: []const u8,
    hook_type: HookType,
    callback: HookCallback,
    context: ?*anyopaque,

    /// Deallocate ModuleHook structure and owned strings
    pub fn deinit(self: *ModuleHook, allocator: std.mem.Allocator) void {
        allocator.free(self.module_name);
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
    /// Data type already registered
    DataTypeAlreadyExists,
    /// Invalid data type name
    InvalidDataTypeName,
    /// Data type not found
    DataTypeNotFound,
    /// Timer not found
    TimerNotFound,
};

/// Storage for dynamically loaded modules
pub const ModuleStore = struct {
    allocator: std.mem.Allocator,
    modules: std.StringHashMap(ModuleInfo),
    commands: std.StringHashMap(ModuleCommand),
    data_types: std.StringHashMap(ModuleDataType),
    blocking: ModuleBlockingStore,
    hooks: std.ArrayList(ModuleHook),
    timers: ModuleTimerStore,
    global_lock: GlobalLock,

    /// Initialize a new ModuleStore
    ///
    /// Arguments:
    ///   - allocator: Memory allocator for module storage
    ///
    /// Returns a new ModuleStore instance
    pub fn init(allocator: std.mem.Allocator) !ModuleStore {
        return ModuleStore{
            .allocator = allocator,
            .modules = std.StringHashMap(ModuleInfo).init(allocator),
            .commands = std.StringHashMap(ModuleCommand).init(allocator),
            .data_types = std.StringHashMap(ModuleDataType).init(allocator),
            .blocking = ModuleBlockingStore.init(allocator),
            .hooks = std.ArrayList(ModuleHook).init(allocator),
            .timers = ModuleTimerStore.init(allocator),
            .global_lock = GlobalLock.init(),
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

        // Free data type keys
        var dt_key_it = self.data_types.keyIterator();
        while (dt_key_it.next()) |key| {
            self.allocator.free(key.*);
        }

        // Free data type values
        var dt_val_it = self.data_types.valueIterator();
        while (dt_val_it.next()) |value| {
            value.deinit(self.allocator);
        }

        self.data_types.deinit();

        // Free hooks
        for (self.hooks.items) |*hook| {
            hook.deinit(self.allocator);
        }
        self.hooks.deinit(self.allocator);

        // Clean up blocking store
        self.blocking.deinit();

        // Clean up timer store
        self.timers.deinit();
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

        // Remove all data types registered by this module
        self.removeModuleDataTypes(module_info.name);

        // Remove all hooks registered by this module
        self.removeModuleHooks(module_info.name) catch {};

        // Remove all timers registered by this module
        self.timers.removeModuleTimers(module_info.name);
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
        var to_remove = std.ArrayList([]const u8).initCapacity(self.allocator, 0) catch return;
        defer to_remove.deinit(self.allocator);

        var it = self.commands.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.module_name, module_name)) {
                to_remove.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        // Remove collected commands
        for (to_remove.items) |cmd_name| {
            if (self.commands.fetchRemove(cmd_name)) |entry| {
                self.allocator.free(entry.key);
                var cmd = entry.value;
                cmd.deinit(self.allocator);
            }
        }
    }

    /// Register a new data type for a module
    ///
    /// Arguments:
    ///   - module_name: Name of the module registering the data type
    ///   - name: Name of the data type
    ///   - encver: Encoding version for RDB serialization
    ///   - rdb_save: Optional RDB save callback
    ///   - rdb_load: Optional RDB load callback
    ///   - aof_rewrite: Optional AOF rewrite callback
    ///   - mem_usage: Optional memory usage callback
    ///   - free_fn: Required free callback
    ///
    /// Returns:
    ///   - void on success
    ///   - ModuleError.InvalidDataTypeName if name is empty
    ///   - ModuleError.DataTypeAlreadyExists if data type already registered
    pub fn registerDataType(
        self: *ModuleStore,
        module_name: []const u8,
        name: []const u8,
        encver: u32,
        rdb_save: ?RdbSaveFn,
        rdb_load: ?RdbLoadFn,
        aof_rewrite: ?AofRewriteFn,
        mem_usage: ?MemUsageFn,
        free_fn: FreeFn,
    ) !void {
        // Validate name is not empty
        if (name.len == 0) {
            return ModuleError.InvalidDataTypeName;
        }

        // Check if data type already exists
        if (self.data_types.contains(name)) {
            return ModuleError.DataTypeAlreadyExists;
        }

        // Allocate owned strings
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        const owned_module_name = try self.allocator.dupe(u8, module_name);
        errdefer self.allocator.free(owned_module_name);

        // Create data type
        const data_type = ModuleDataType{
            .name = owned_name,
            .module_name = owned_module_name,
            .encver = encver,
            .rdb_save = rdb_save,
            .rdb_load = rdb_load,
            .aof_rewrite = aof_rewrite,
            .mem_usage = mem_usage,
            .free = free_fn,
        };

        // Store in HashMap
        try self.data_types.put(owned_name, data_type);
    }

    /// Get a registered data type by name
    ///
    /// Arguments:
    ///   - name: Name of the data type to retrieve
    ///
    /// Returns:
    ///   - Pointer to ModuleDataType if found
    ///   - null if not found
    pub fn getDataType(self: *ModuleStore, name: []const u8) ?*ModuleDataType {
        return self.data_types.getPtr(name);
    }

    /// Remove all data types registered by a module
    ///
    /// Called when a module is unloaded to clean up all its data types.
    ///
    /// Arguments:
    ///   - module_name: Name of the module whose data types to remove
    pub fn removeModuleDataTypes(self: *ModuleStore, module_name: []const u8) void {
        // Collect data type names to remove (can't modify HashMap while iterating)
        var to_remove = std.ArrayList([]const u8).initCapacity(self.allocator, 0) catch return;
        defer to_remove.deinit(self.allocator);

        var it = self.data_types.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.module_name, module_name)) {
                to_remove.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        // Remove collected data types
        for (to_remove.items) |dt_name| {
            if (self.data_types.fetchRemove(dt_name)) |entry| {
                self.allocator.free(entry.key);
                var dt = entry.value;
                dt.deinit(self.allocator);
            }
        }
    }

    // ========================================================================
    // Hook Management API
    // ========================================================================

    /// Register a module hook for event notifications
    ///
    /// Arguments:
    ///   - hook_type: Type of event to hook (keyspace, command filter, client state)
    ///   - module_name: Name of the module registering the hook
    ///   - callback: Function to call when event occurs
    ///   - context: Optional context pointer passed to callback
    ///
    /// Returns:
    ///   - void on success
    ///   - error.OutOfMemory if allocation fails
    pub fn registerHook(
        self: *ModuleStore,
        hook_type: HookType,
        module_name: []const u8,
        callback: HookCallback,
        context: ?*anyopaque,
    ) !void {
        const module_name_copy = try self.allocator.dupe(u8, module_name);
        errdefer self.allocator.free(module_name_copy);

        const hook = ModuleHook{
            .module_name = module_name_copy,
            .hook_type = hook_type,
            .callback = callback,
            .context = context,
        };

        try self.hooks.append(self.allocator, hook);
    }

    /// Get all hooks registered for a specific hook type
    ///
    /// Arguments:
    ///   - hook_type: Type of hook to retrieve
    ///
    /// Returns:
    ///   - Slice of ModuleHook matching the specified type
    pub fn getHooks(self: *ModuleStore, hook_type: HookType) []const ModuleHook {
        var result = std.ArrayList(ModuleHook).initCapacity(self.allocator, 0) catch return &[_]ModuleHook{};
        defer result.deinit(self.allocator);

        for (self.hooks.items) |hook| {
            if (hook.hook_type == hook_type) {
                result.append(self.allocator, hook) catch continue;
            }
        }

        return result.toOwnedSlice(self.allocator) catch &[_]ModuleHook{};
    }

    /// Trigger keyspace notification event to all registered hooks
    ///
    /// Arguments:
    ///   - event_type: Numeric event type identifier
    ///   - event: Event name (e.g., "set", "del", "expire")
    ///   - key: Key information for the event
    ///
    /// Returns:
    ///   - void on success
    pub fn triggerKeyspaceNotification(
        self: *ModuleStore,
        event_type: i32,
        event: []const u8,
        key: *const KeyInfo,
    ) !void {
        for (self.hooks.items) |hook| {
            if (hook.hook_type == .keyspace_notification) {
                hook.callback(hook.context, event_type, event, key);
            }
        }
    }

    /// Trigger command filter event to all registered hooks
    ///
    /// Arguments:
    ///   - key: Key information (command name in key field)
    ///
    /// Returns:
    ///   - void on success
    pub fn triggerCommandFilter(self: *ModuleStore, key: *const KeyInfo) !void {
        for (self.hooks.items) |hook| {
            if (hook.hook_type == .command_filter) {
                hook.callback(hook.context, 0, "", key);
            }
        }
    }

    /// Remove all hooks registered by a specific module
    ///
    /// This is called automatically when a module is unloaded to clean up
    /// all hooks that the module registered.
    ///
    /// Arguments:
    ///   - module_name: Name of the module whose hooks should be removed
    ///
    /// Returns:
    ///   - void on success
    ///   - error.OutOfMemory if temporary allocation fails
    pub fn removeModuleHooks(self: *ModuleStore, module_name: []const u8) !void {
        var i: usize = 0;
        while (i < self.hooks.items.len) {
            if (std.mem.eql(u8, self.hooks.items[i].module_name, module_name)) {
                var hook = self.hooks.swapRemove(i);
                hook.deinit(self.allocator);
            } else {
                i += 1;
            }
        }
    }
};

// ============================================================================
// Module Timer API
// ============================================================================

/// Callback function invoked when a timer fires
/// Redis module ABI: void (*RedisModuleTimerProc)(RedisModuleCtx *ctx, void *data)
pub const TimerCallback = *const fn (allocator: std.mem.Allocator, data: ?*anyopaque) void;

/// Represents a timer registered by a module
/// Timers can be one-shot or periodic based on the period_ms value
pub const TimerInfo = struct {
    /// Unique timer identifier
    timer_id: u64,
    /// Module that owns this timer
    module_name: []const u8,
    /// Period in milliseconds (0 = one-shot)
    period_ms: i64,
    /// Next fire time (milliseconds since epoch)
    next_fire_time: i64,
    /// Module-specific private data
    data: ?*anyopaque,
    /// Callback function to invoke
    callback: TimerCallback,
    /// Free function for data
    free_data: ?FreeFn,

    allocator: std.mem.Allocator,

    /// Deallocate TimerInfo and free private data
    pub fn deinit(self: *TimerInfo) void {
        self.allocator.free(self.module_name);
        // Free data if free function provided
        if (self.free_data) |free_fn| {
            if (self.data) |d| {
                free_fn(self.allocator, d);
            }
        }
    }
};

/// Storage for module timers
pub const ModuleTimerStore = struct {
    allocator: std.mem.Allocator,
    /// Map of timer_id -> TimerInfo
    timers: std.AutoHashMap(u64, TimerInfo),
    /// Next timer ID for registration
    next_timer_id: u64,

    /// Initialize a new ModuleTimerStore
    pub fn init(allocator: std.mem.Allocator) ModuleTimerStore {
        return .{
            .allocator = allocator,
            .timers = std.AutoHashMap(u64, TimerInfo).init(allocator),
            .next_timer_id = 1,
        };
    }

    /// Deallocate all timers
    pub fn deinit(self: *ModuleTimerStore) void {
        var iter = self.timers.iterator();
        while (iter.next()) |entry| {
            var timer = entry.value_ptr;
            timer.deinit();
        }
        self.timers.deinit();
    }

    /// Create a new timer
    ///
    /// Arguments:
    ///   - module_name: Name of the module creating the timer
    ///   - period_ms: Period in milliseconds (0 = one-shot timer)
    ///   - data: Module-specific private data (can be null)
    ///   - callback: Function to invoke when timer fires
    ///   - free_data: Function to free data when timer is deleted
    ///
    /// Returns:
    ///   - Timer ID on success
    ///   - error.OutOfMemory if allocation fails
    pub fn createTimer(
        self: *ModuleTimerStore,
        module_name: []const u8,
        period_ms: i64,
        data: ?*anyopaque,
        callback: TimerCallback,
        free_data: ?FreeFn,
    ) !u64 {
        const timer_id = self.next_timer_id;
        self.next_timer_id += 1;

        const now_ms = std.time.milliTimestamp();
        const owned_name = try self.allocator.dupe(u8, module_name);
        errdefer self.allocator.free(owned_name);

        const timer = TimerInfo{
            .timer_id = timer_id,
            .module_name = owned_name,
            .period_ms = period_ms,
            .next_fire_time = now_ms + period_ms,
            .data = data,
            .callback = callback,
            .free_data = free_data,
            .allocator = self.allocator,
        };

        try self.timers.put(timer_id, timer);
        return timer_id;
    }

    /// Stop and remove a timer
    ///
    /// Arguments:
    ///   - timer_id: ID of the timer to stop
    ///
    /// Returns:
    ///   - error.TimerNotFound if timer doesn't exist
    pub fn stopTimer(self: *ModuleTimerStore, timer_id: u64) !void {
        if (self.timers.fetchRemove(timer_id)) |kv| {
            var timer = kv.value;
            timer.deinit();
        } else {
            return error.TimerNotFound;
        }
    }

    /// Process all timers and invoke callbacks for those that have fired
    ///
    /// This should be called periodically (e.g., every 10ms) by the server's
    /// main event loop.
    ///
    /// Returns:
    ///   - Number of timers that fired
    pub fn processTimers(self: *ModuleTimerStore) usize {
        const now_ms = std.time.milliTimestamp();
        var fired_count: usize = 0;
        var to_remove = std.ArrayList(u64).init(self.allocator);
        defer to_remove.deinit();

        var iter = self.timers.iterator();
        while (iter.next()) |entry| {
            const timer = entry.value_ptr;
            if (now_ms >= timer.next_fire_time) {
                // Invoke callback
                timer.callback(self.allocator, timer.data);
                fired_count += 1;

                // One-shot timer (period_ms == 0) should be removed
                if (timer.period_ms == 0) {
                    to_remove.append(timer.timer_id) catch {};
                } else {
                    // Periodic timer: reschedule
                    timer.next_fire_time = now_ms + timer.period_ms;
                }
            }
        }

        // Remove one-shot timers that have fired
        for (to_remove.items) |timer_id| {
            _ = self.stopTimer(timer_id) catch {};
        }

        return fired_count;
    }

    /// Remove all timers registered by a specific module
    ///
    /// Arguments:
    ///   - module_name: Name of the module whose timers should be removed
    pub fn removeModuleTimers(self: *ModuleTimerStore, module_name: []const u8) void {
        var to_remove = std.ArrayList(u64).init(self.allocator);
        defer to_remove.deinit();

        var iter = self.timers.iterator();
        while (iter.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.module_name, module_name)) {
                to_remove.append(entry.value_ptr.timer_id) catch {};
            }
        }

        for (to_remove.items) |timer_id| {
            _ = self.stopTimer(timer_id) catch {};
        }
    }

    /// Get count of active timers
    pub fn timerCount(self: *ModuleTimerStore) usize {
        return self.timers.count();
    }

    /// Check if a timer exists
    pub fn hasTimer(self: *ModuleTimerStore, timer_id: u64) bool {
        return self.timers.contains(timer_id);
    }
};

// ============================================================================
// Module Blocking Context API
// ============================================================================

/// Callback function invoked when a blocked client is unblocked
/// Redis module ABI: void (*RedisModuleBlockedClientTimedOut)(RedisModuleCtx *ctx, void *privdata)
pub const BlockedClientTimeoutFn = *const fn (allocator: std.mem.Allocator, privdata: ?*anyopaque) anyerror!void;

/// Callback function invoked to prepare response for unblocked client
/// Redis module ABI: int (*RedisModuleBlockedClientReplyCb)(RedisModuleCtx *ctx, void **argv, int argc, void *privdata)
pub const BlockedClientReplyCb = *const fn (allocator: std.mem.Allocator, storage: *Storage, privdata: ?*anyopaque) anyerror![]const u8;

/// Callback function invoked when a blocked client disconnects before unblocking
/// Redis module ABI: void (*RedisModuleBlockedClientDisconnectCb)(RedisModuleCtx *ctx, void *privdata)
pub const BlockedClientDisconnectCb = *const fn (allocator: std.mem.Allocator, privdata: ?*anyopaque) void;

/// Represents a client blocked by a module command
/// Tracks client ID, timeout, callbacks, and module-specific private data
pub const ModuleBlockedClient = struct {
    /// Unique client identifier
    client_id: u64,
    /// Module that blocked this client
    module_name: []const u8,
    /// Timeout in milliseconds (0 = infinite)
    timeout_ms: i64,
    /// Start time (milliseconds since epoch)
    start_time: i64,
    /// Module-specific private data
    privdata: ?*anyopaque,
    /// Callback for timeout
    timeout_fn: ?BlockedClientTimeoutFn,
    /// Callback for preparing reply
    reply_cb: ?BlockedClientReplyCb,
    /// Callback for disconnect
    disconnect_cb: ?BlockedClientDisconnectCb,
    /// Free function for privdata
    free_privdata: ?FreeFn,

    allocator: std.mem.Allocator,

    /// Deallocate ModuleBlockedClient and free private data
    pub fn deinit(self: *ModuleBlockedClient) void {
        self.allocator.free(self.module_name);
        // Free privdata if free function provided
        if (self.free_privdata) |free_fn| {
            if (self.privdata) |data| {
                free_fn(self.allocator, data);
            }
        }
    }
};

/// Storage for module-blocked clients
///
/// THREAD SAFETY: This implementation assumes single-threaded access.
/// If multi-threaded command processing is added, protect blocked_clients
/// with a Mutex and use atomic operations for next_client_id.
pub const ModuleBlockingStore = struct {
    allocator: std.mem.Allocator,
    /// Map of client_id -> ModuleBlockedClient
    blocked_clients: std.AutoHashMap(u64, ModuleBlockedClient),
    /// Next client ID for blocking operations
    next_client_id: u64,

    /// Initialize a new ModuleBlockingStore
    pub fn init(allocator: std.mem.Allocator) ModuleBlockingStore {
        return ModuleBlockingStore{
            .allocator = allocator,
            .blocked_clients = std.AutoHashMap(u64, ModuleBlockedClient).init(allocator),
            .next_client_id = 1,
        };
    }

    /// Deallocate ModuleBlockingStore and all blocked clients
    pub fn deinit(self: *ModuleBlockingStore) void {
        var it = self.blocked_clients.valueIterator();
        while (it.next()) |client| {
            var c = client.*;
            c.deinit();
        }
        self.blocked_clients.deinit();
    }

    /// Block a client with the specified module context
    ///
    /// Arguments:
    ///   - module_name: Name of the module blocking the client
    ///   - timeout_ms: Timeout in milliseconds (0 = infinite)
    ///   - privdata: Module-specific private data
    ///               OWNERSHIP: Transferred immediately to ModuleBlockingStore.
    ///               Freed via free_privdata callback on unblock/timeout/disconnect.
    ///               DO NOT access privdata after this call returns.
    ///   - timeout_fn: Optional timeout callback
    ///   - reply_cb: Optional reply callback
    ///   - disconnect_cb: Optional disconnect callback
    ///   - free_privdata: Function to free privdata (REQUIRED if privdata is non-null)
    ///
    /// Returns:
    ///   - client_id: Unique identifier for this blocked client
    pub fn blockClient(
        self: *ModuleBlockingStore,
        module_name: []const u8,
        timeout_ms: i64,
        privdata: ?*anyopaque,
        timeout_fn: ?BlockedClientTimeoutFn,
        reply_cb: ?BlockedClientReplyCb,
        disconnect_cb: ?BlockedClientDisconnectCb,
        free_privdata: ?FreeFn,
    ) !u64 {
        // Validate that if privdata is provided, free_privdata must also be provided
        if (privdata != null and free_privdata == null) {
            std.log.warn("blockClient: privdata provided without free_privdata - potential memory leak", .{});
        }
        const client_id = self.next_client_id;
        self.next_client_id += 1;

        const owned_module_name = try self.allocator.dupe(u8, module_name);
        errdefer self.allocator.free(owned_module_name);

        const now = std.time.milliTimestamp();

        const blocked_client = ModuleBlockedClient{
            .client_id = client_id,
            .module_name = owned_module_name,
            .timeout_ms = timeout_ms,
            .start_time = now,
            .privdata = privdata,
            .timeout_fn = timeout_fn,
            .reply_cb = reply_cb,
            .disconnect_cb = disconnect_cb,
            .free_privdata = free_privdata,
            .allocator = self.allocator,
        };

        try self.blocked_clients.put(client_id, blocked_client);
        return client_id;
    }

    /// Unblock a client with the specified client ID
    ///
    /// Calls the reply callback to generate response, then removes client from blocked list.
    ///
    /// Arguments:
    ///   - client_id: Unique identifier returned by blockClient
    ///   - storage: Storage instance for reply callback
    ///
    /// Returns:
    ///   - Response string to send to client (owned by caller)
    ///   - null if client not found or no reply callback
    pub fn unblockClient(
        self: *ModuleBlockingStore,
        client_id: u64,
        storage: *Storage,
    ) !?[]const u8 {
        const entry = self.blocked_clients.fetchRemove(client_id) orelse return null;
        var client = entry.value;
        defer client.deinit();

        // Call reply callback if provided
        if (client.reply_cb) |reply_fn| {
            return try reply_fn(self.allocator, storage, client.privdata);
        }

        return null;
    }

    /// Trigger timeout for all expired blocked clients
    ///
    /// Iterates through all blocked clients, calls timeout callbacks for expired ones,
    /// and removes them from the blocked list.
    ///
    /// Returns:
    ///   - Count of clients timed out
    pub fn processTimeouts(self: *ModuleBlockingStore) !usize {
        const now = std.time.milliTimestamp();
        var timed_out_count: usize = 0;

        // Collect client IDs to timeout (can't modify HashMap while iterating)
        var to_timeout = try std.ArrayList(u64).initCapacity(self.allocator, 0);
        defer to_timeout.deinit(self.allocator);

        var it = self.blocked_clients.iterator();
        while (it.next()) |entry| {
            const client = entry.value_ptr;
            // Skip clients with infinite timeout (timeout_ms == 0)
            if (client.timeout_ms > 0) {
                const elapsed = now - client.start_time;
                if (elapsed >= client.timeout_ms) {
                    try to_timeout.append(self.allocator, client.client_id);
                }
            }
        }

        // Process timeouts
        for (to_timeout.items) |client_id| {
            if (self.blocked_clients.fetchRemove(client_id)) |entry| {
                var client = entry.value;
                defer client.deinit();

                // Call timeout callback if provided
                if (client.timeout_fn) |timeout_fn| {
                    timeout_fn(self.allocator, client.privdata) catch |err| {
                        std.log.err("Module blocking timeout callback failed: {}", .{err});
                    };
                }
                timed_out_count += 1;
            }
        }

        return timed_out_count;
    }

    /// Handle client disconnect before unblock
    ///
    /// Calls disconnect callback and removes client from blocked list.
    ///
    /// Arguments:
    ///   - client_id: Unique identifier of disconnected client
    pub fn handleDisconnect(self: *ModuleBlockingStore, client_id: u64) void {
        const entry = self.blocked_clients.fetchRemove(client_id) orelse return;
        var client = entry.value;
        defer client.deinit();

        // Call disconnect callback if provided
        if (client.disconnect_cb) |disconnect_fn| {
            disconnect_fn(self.allocator, client.privdata);
        }
    }

    /// Get count of currently blocked clients
    pub fn blockedCount(self: *ModuleBlockingStore) usize {
        return self.blocked_clients.count();
    }

    /// Check if a client is currently blocked
    pub fn isBlocked(self: *ModuleBlockingStore, client_id: u64) bool {
        return self.blocked_clients.contains(client_id);
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

// ============================================================================
// Module Blocking Tests
// ============================================================================

test "ModuleBlockingStore: init and deinit" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
}

test "ModuleBlockingStore: block and unblock client" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    // Block a client
    const client_id = try blocking.blockClient(
        "testmodule",
        5000, // 5 second timeout
        null, // no privdata
        null, // no timeout callback
        null, // no reply callback
        null, // no disconnect callback
        null, // no free function
    );

    try testing.expect(client_id > 0);
    try testing.expectEqual(@as(usize, 1), blocking.blockedCount());
    try testing.expect(blocking.isBlocked(client_id));

    // Unblock the client
    var mock_storage: Storage = undefined;
    const response = try blocking.unblockClient(client_id, &mock_storage);
    try testing.expect(response == null); // No reply callback

    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
    try testing.expect(!blocking.isBlocked(client_id));
}

test "ModuleBlockingStore: multiple clients" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    // Block 3 clients
    const id1 = try blocking.blockClient("mod1", 1000, null, null, null, null, null);
    const id2 = try blocking.blockClient("mod2", 2000, null, null, null, null, null);
    const id3 = try blocking.blockClient("mod3", 0, null, null, null, null, null); // Infinite timeout

    try testing.expectEqual(@as(usize, 3), blocking.blockedCount());
    try testing.expect(id1 != id2);
    try testing.expect(id2 != id3);

    // Unblock one
    var mock_storage: Storage = undefined;
    _ = try blocking.unblockClient(id2, &mock_storage);
    try testing.expectEqual(@as(usize, 2), blocking.blockedCount());
    try testing.expect(!blocking.isBlocked(id2));
}

fn testTimeoutCallback(allocator: std.mem.Allocator, privdata: ?*anyopaque) !void {
    _ = allocator;
    if (privdata) |data| {
        const counter = @as(*usize, @ptrCast(@alignCast(data)));
        counter.* += 1;
    }
}

test "ModuleBlockingStore: timeout callback" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    var timeout_counter: usize = 0;

    // Block client with very short timeout (1ms)
    _ = try blocking.blockClient(
        "testmodule",
        1, // 1ms timeout
        @ptrCast(&timeout_counter),
        testTimeoutCallback,
        null,
        null,
        null, // Don't free stack variable
    );

    try testing.expectEqual(@as(usize, 1), blocking.blockedCount());

    // Wait for timeout
    std.time.sleep(5 * std.time.ns_per_ms);

    // Process timeouts
    const timed_out = try blocking.processTimeouts();
    try testing.expectEqual(@as(usize, 1), timed_out);
    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
    try testing.expectEqual(@as(usize, 1), timeout_counter);
}

test "ModuleBlockingStore: infinite timeout not triggered" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    var timeout_counter: usize = 0;

    // Block client with infinite timeout (0)
    const client_id = try blocking.blockClient(
        "testmodule",
        0, // Infinite timeout
        @ptrCast(&timeout_counter),
        testTimeoutCallback,
        null,
        null,
        null,
    );

    // Wait a bit
    std.time.sleep(10 * std.time.ns_per_ms);

    // Process timeouts - should not timeout
    const timed_out = try blocking.processTimeouts();
    try testing.expectEqual(@as(usize, 0), timed_out);
    try testing.expectEqual(@as(usize, 1), blocking.blockedCount());
    try testing.expect(blocking.isBlocked(client_id));
    try testing.expectEqual(@as(usize, 0), timeout_counter);
}

fn testDisconnectCallback(allocator: std.mem.Allocator, privdata: ?*anyopaque) void {
    _ = allocator;
    if (privdata) |data| {
        const counter = @as(*usize, @ptrCast(@alignCast(data)));
        counter.* += 1;
    }
}

test "ModuleBlockingStore: disconnect callback" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    var disconnect_counter: usize = 0;

    const client_id = try blocking.blockClient(
        "testmodule",
        5000,
        @ptrCast(&disconnect_counter),
        null,
        null,
        testDisconnectCallback,
        null,
    );

    try testing.expectEqual(@as(usize, 1), blocking.blockedCount());

    // Simulate disconnect
    blocking.handleDisconnect(client_id);

    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
    try testing.expectEqual(@as(usize, 1), disconnect_counter);
}

fn testReplyCallback(allocator: std.mem.Allocator, storage: *Storage, privdata: ?*anyopaque) ![]const u8 {
    _ = storage;
    if (privdata) |data| {
        const msg = @as(*const []const u8, @ptrCast(@alignCast(data)));
        return try allocator.dupe(u8, msg.*);
    }
    return try allocator.dupe(u8, "+OK\r\n");
}

test "ModuleBlockingStore: reply callback" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    const reply_msg = "+CUSTOM_REPLY\r\n";

    const client_id = try blocking.blockClient(
        "testmodule",
        5000,
        @ptrCast(@constCast(&reply_msg)),
        null,
        testReplyCallback,
        null,
        null,
    );

    var mock_storage: Storage = undefined;
    const response = try blocking.unblockClient(client_id, &mock_storage);

    try testing.expect(response != null);
    if (response) |resp| {
        defer allocator.free(resp);
        try testing.expectEqualStrings(reply_msg, resp);
    }

    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
}

fn testFreePrivdata(allocator: std.mem.Allocator, data: *anyopaque) void {
    const counter = @as(*usize, @ptrCast(@alignCast(data)));
    allocator.destroy(counter);
}

test "ModuleBlockingStore: privdata cleanup on unblock" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    // Allocate privdata on heap
    const privdata = try allocator.create(usize);
    privdata.* = 42;

    const client_id = try blocking.blockClient(
        "testmodule",
        5000,
        privdata,
        null,
        null,
        null,
        testFreePrivdata,
    );

    var mock_storage: Storage = undefined;
    _ = try blocking.unblockClient(client_id, &mock_storage);

    // Privdata should be freed by free_privdata callback during deinit
    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
}

test "ModuleBlockingStore: multiple timeouts" {
    const allocator = testing.allocator;
    var blocking = ModuleBlockingStore.init(allocator);
    defer blocking.deinit();

    var timeout_counter: usize = 0;

    // Block 3 clients with short timeouts
    _ = try blocking.blockClient("mod1", 1, @ptrCast(&timeout_counter), testTimeoutCallback, null, null, null);
    _ = try blocking.blockClient("mod2", 1, @ptrCast(&timeout_counter), testTimeoutCallback, null, null, null);
    _ = try blocking.blockClient("mod3", 1, @ptrCast(&timeout_counter), testTimeoutCallback, null, null, null);

    try testing.expectEqual(@as(usize, 3), blocking.blockedCount());

    // Wait for all to timeout
    std.time.sleep(10 * std.time.ns_per_ms);

    const timed_out = try blocking.processTimeouts();
    try testing.expectEqual(@as(usize, 3), timed_out);
    try testing.expectEqual(@as(usize, 0), blocking.blockedCount());
    try testing.expectEqual(@as(usize, 3), timeout_counter);
}

/// Global lock (GIL) for Redis main thread access
/// Only one thread can hold the lock at a time, ensuring single-threaded semantics
pub const GlobalLock = struct {
    mutex: std.Thread.Mutex,
    owner_thread: ?std.Thread.Id,
    waiter_count: usize,

    pub fn init() GlobalLock {
        return .{
            .mutex = .{},
            .owner_thread = null,
            .waiter_count = 0,
        };
    }

    /// Acquire the global lock (blocking)
    /// Returns the thread ID of the caller for validation
    pub fn lock(self: *GlobalLock) std.Thread.Id {
        const thread_id = std.Thread.getCurrentId();

        // Track waiter before blocking
        @atomicStore(usize, &self.waiter_count, self.waiter_count + 1, .monotonic);

        self.mutex.lock();

        // Decrement waiter after acquiring
        @atomicStore(usize, &self.waiter_count, self.waiter_count - 1, .monotonic);

        self.owner_thread = thread_id;
        return thread_id;
    }

    /// Release the global lock
    pub fn unlock(self: *GlobalLock) void {
        self.owner_thread = null;
        self.mutex.unlock();
    }

    /// Try to acquire the lock without blocking
    /// Returns the thread ID on success, null if already locked
    pub fn tryLock(self: *GlobalLock) ?std.Thread.Id {
        if (self.mutex.tryLock()) {
            const thread_id = std.Thread.getCurrentId();
            self.owner_thread = thread_id;
            return thread_id;
        }
        return null;
    }

    /// Check if the lock is held by the current thread
    pub fn isOwnedByCurrentThread(self: *const GlobalLock) bool {
        const current = std.Thread.getCurrentId();
        return if (self.owner_thread) |owner| owner == current else false;
    }

    /// Get number of threads waiting to acquire the lock
    pub fn getWaiterCount(self: *const GlobalLock) usize {
        return @atomicLoad(usize, &self.waiter_count, .monotonic);
    }
};

/// Thread-safe context for background thread command execution
/// Redis module ABI: RedisModuleCtx with thread-safe semantics
pub const ThreadSafeContext = struct {
    global_lock: *GlobalLock,
    creator_thread: std.Thread.Id,
    locked: bool,
    client_id: ?u64, // If bound to a blocked client
    reply_buffer: ?std.ArrayList(u8), // Accumulated reply for bound client

    /// Create a thread-safe context (detached, not bound to client)
    /// Redis module ABI: RedisModuleCtx *RedisModule_GetThreadSafeContext(RedisModuleBlockedClient *bc)
    pub fn init(allocator: std.mem.Allocator, global_lock: *GlobalLock) !*ThreadSafeContext {
        const ctx = try allocator.create(ThreadSafeContext);
        ctx.* = .{
            .global_lock = global_lock,
            .creator_thread = std.Thread.getCurrentId(),
            .locked = false,
            .client_id = null,
            .reply_buffer = null,
        };
        return ctx;
    }

    /// Create a thread-safe context bound to a blocked client
    /// This variant accumulates replies in a buffer for delivery to the client
    pub fn initBound(allocator: std.mem.Allocator, global_lock: *GlobalLock, client_id: u64) !*ThreadSafeContext {
        const ctx = try allocator.create(ThreadSafeContext);
        ctx.* = .{
            .global_lock = global_lock,
            .creator_thread = std.Thread.getCurrentId(),
            .locked = false,
            .client_id = client_id,
            .reply_buffer = std.ArrayList(u8).init(allocator),
        };
        return ctx;
    }

    /// Free a thread-safe context
    /// Redis module ABI: void RedisModule_FreeThreadSafeContext(RedisModuleCtx *ctx)
    /// MUST be called from the same thread that created the context
    /// MUST not be called while context is locked
    pub fn deinit(self: *ThreadSafeContext, allocator: std.mem.Allocator) !void {
        // Validate caller thread
        const current_thread = std.Thread.getCurrentId();
        if (current_thread != self.creator_thread) {
            return error.WrongThread;
        }

        // Cannot free while locked
        if (self.locked) {
            return error.ContextLocked;
        }

        if (self.reply_buffer) |*buf| {
            buf.deinit();
        }

        allocator.destroy(self);
    }

    /// Acquire the global lock
    /// Redis module ABI: void RedisModule_ThreadSafeContextLock(RedisModuleCtx *ctx)
    pub fn lockContext(self: *ThreadSafeContext) void {
        _ = self.global_lock.lock();
        self.locked = true;
    }

    /// Release the global lock
    /// Redis module ABI: void RedisModule_ThreadSafeContextUnlock(RedisModuleCtx *ctx)
    pub fn unlockContext(self: *ThreadSafeContext) void {
        self.locked = false;
        self.global_lock.unlock();
    }

    /// Try to acquire the global lock without blocking
    /// Redis module ABI: int RedisModule_ThreadSafeContextTryLock(RedisModuleCtx *ctx)
    /// Returns true if lock acquired, false otherwise
    pub fn tryLockContext(self: *ThreadSafeContext) bool {
        if (self.global_lock.tryLock()) |_| {
            self.locked = true;
            return true;
        }
        return false;
    }

    /// Check if context is currently locked
    pub fn isLocked(self: *const ThreadSafeContext) bool {
        return self.locked;
    }

    /// Execute a command (requires lock to be held)
    /// This validates that the global lock is held before allowing command execution
    pub fn callCommand(
        self: *ThreadSafeContext,
        allocator: std.mem.Allocator,
        storage: *Storage,
        cmd: ModuleCmdFunc,
        args: []const RespValue,
    ) ![]const u8 {
        // CRITICAL: Must hold global lock to access Redis dataset
        if (!self.locked) {
            return error.ContextNotLocked;
        }

        // Execute command (this accesses the Redis dataset)
        const response = try cmd(allocator, storage, args);

        // If bound to a client, accumulate reply in buffer
        if (self.reply_buffer) |*buf| {
            try buf.appendSlice(response);
            allocator.free(response);
            // Return empty slice - actual reply will be sent when client is unblocked
            return try allocator.dupe(u8, "");
        }

        return response;
    }

    /// Get accumulated reply (for bound contexts)
    /// Returns null if not bound or no reply accumulated
    pub fn getReply(self: *ThreadSafeContext, allocator: std.mem.Allocator) !?[]const u8 {
        if (self.reply_buffer) |*buf| {
            if (buf.items.len > 0) {
                return try allocator.dupe(u8, buf.items);
            }
        }
        return null;
    }
};

test "ModuleCtx: blockClient and unblockClient" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    var ctx = ModuleCtx{
        .name = "testmodule",
        .ver = 1,
        .store = &store,
    };

    // Block via context
    const client_id = try ctx.blockClient(
        5000,
        null,
        null,
        null,
        null,
        null,
    );

    try testing.expect(client_id > 0);
    try testing.expect(store.blocking.isBlocked(client_id));

    // Unblock via context
    var mock_storage: Storage = undefined;
    const response = try ctx.unblockClient(client_id, &mock_storage);
    try testing.expect(response == null);
    try testing.expect(!store.blocking.isBlocked(client_id));
}

test "GlobalLock: single-threaded lock/unlock" {
    var lock = GlobalLock.init();

    // Initially no owner
    try testing.expect(lock.owner_thread == null);
    try testing.expect(!lock.isOwnedByCurrentThread());

    // Acquire lock
    const thread_id = lock.lock();
    try testing.expect(lock.owner_thread.? == thread_id);
    try testing.expect(lock.isOwnedByCurrentThread());

    // Release lock
    lock.unlock();
    try testing.expect(lock.owner_thread == null);
    try testing.expect(!lock.isOwnedByCurrentThread());
}

test "GlobalLock: tryLock success and failure" {
    var lock = GlobalLock.init();

    // First tryLock succeeds
    const thread_id_1 = lock.tryLock();
    try testing.expect(thread_id_1 != null);
    try testing.expect(lock.isOwnedByCurrentThread());

    // Second tryLock fails (already locked)
    const thread_id_2 = lock.tryLock();
    try testing.expect(thread_id_2 == null);

    // Release and try again
    lock.unlock();
    const thread_id_3 = lock.tryLock();
    try testing.expect(thread_id_3 != null);

    lock.unlock();
}

test "ThreadSafeContext: create and free detached context" {
    const allocator = testing.allocator;
    var lock = GlobalLock.init();

    const ctx = try ThreadSafeContext.init(allocator, &lock);
    try testing.expect(ctx.creator_thread == std.Thread.getCurrentId());
    try testing.expect(!ctx.locked);
    try testing.expect(ctx.client_id == null);
    try testing.expect(ctx.reply_buffer == null);

    try ctx.deinit(allocator);
}

test "ThreadSafeContext: create and free bound context" {
    const allocator = testing.allocator;
    var lock = GlobalLock.init();

    const client_id: u64 = 12345;
    const ctx = try ThreadSafeContext.initBound(allocator, &lock, client_id);
    try testing.expect(ctx.client_id.? == client_id);
    try testing.expect(ctx.reply_buffer != null);

    try ctx.deinit(allocator);
}

test "ThreadSafeContext: cannot free while locked" {
    const allocator = testing.allocator;
    var lock = GlobalLock.init();

    const ctx = try ThreadSafeContext.init(allocator, &lock);
    ctx.lockContext();

    // Should error
    try testing.expectError(error.ContextLocked, ctx.deinit(allocator));

    // Unlock and free successfully
    ctx.unlockContext();
    try ctx.deinit(allocator);
}

test "ThreadSafeContext: lock and unlock" {
    const allocator = testing.allocator;
    var lock = GlobalLock.init();

    const ctx = try ThreadSafeContext.init(allocator, &lock);
    defer ctx.deinit(allocator) catch unreachable;

    try testing.expect(!ctx.isLocked());
    try testing.expect(!lock.isOwnedByCurrentThread());

    ctx.lockContext();
    try testing.expect(ctx.isLocked());
    try testing.expect(lock.isOwnedByCurrentThread());

    ctx.unlockContext();
    try testing.expect(!ctx.isLocked());
    try testing.expect(!lock.isOwnedByCurrentThread());
}

test "ThreadSafeContext: tryLock" {
    const allocator = testing.allocator;
    var lock = GlobalLock.init();

    const ctx = try ThreadSafeContext.init(allocator, &lock);
    defer ctx.deinit(allocator) catch unreachable;

    // First tryLock succeeds
    try testing.expect(ctx.tryLockContext());
    try testing.expect(ctx.isLocked());

    // Create another context (simulating different thread context)
    const ctx2 = try ThreadSafeContext.init(allocator, &lock);
    defer ctx2.deinit(allocator) catch unreachable;

    // Second tryLock fails (lock already held)
    try testing.expect(!ctx2.tryLockContext());
    try testing.expect(!ctx2.isLocked());

    // Release first lock
    ctx.unlockContext();

    // Now second context can acquire
    try testing.expect(ctx2.tryLockContext());
    try testing.expect(ctx2.isLocked());

    ctx2.unlockContext();
}

test "ModuleCtx: thread-safe context lifecycle" {
    const allocator = testing.allocator;
    var store = try ModuleStore.init(allocator);
    defer store.deinit();

    var ctx = ModuleCtx{
        .name = "testmodule",
        .ver = 1,
        .store = &store,
    };

    // Create thread-safe context
    const ts_ctx = try ctx.getThreadSafeContext(allocator);

    try testing.expect(!ts_ctx.isLocked());
    try testing.expect(ts_ctx.client_id == null);

    // Lock and unlock
    ctx.lockThreadSafeContext(ts_ctx);
    try testing.expect(ts_ctx.isLocked());

    ctx.unlockThreadSafeContext(ts_ctx);
    try testing.expect(!ts_ctx.isLocked());

    // Free context
    try ctx.freeThreadSafeContext(allocator, ts_ctx);
}

test "ModuleCtx: bound thread-safe context with reply accumulation" {
    const allocator = testing.allocator;
    var store = try ModuleStore.init(allocator);
    defer store.deinit();

    var ctx = ModuleCtx{
        .name = "testmodule",
        .ver = 1,
        .store = &store,
    };

    const client_id: u64 = 99999;

    // Create bound context
    const ts_ctx = try ctx.getThreadSafeContextBound(allocator, client_id);
    defer ctx.freeThreadSafeContext(allocator, ts_ctx) catch unreachable;

    try testing.expect(ts_ctx.client_id.? == client_id);
    try testing.expect(ts_ctx.reply_buffer != null);

    // Simulate reply accumulation
    if (ts_ctx.reply_buffer) |*buf| {
        try buf.appendSlice("+OK\r\n");
        try buf.appendSlice(":12345\r\n");
    }

    const reply = try ts_ctx.getReply(allocator);
    try testing.expect(reply != null);
    defer if (reply) |r| allocator.free(r);

    try testing.expectEqualStrings("+OK\r\n:12345\r\n", reply.?);
}

// Multi-threaded test helper
const ThreadTestContext = struct {
    lock: *GlobalLock,
    counter: *usize,
    iterations: usize,
};

fn threadWorker(context: *ThreadTestContext) void {
    var i: usize = 0;
    while (i < context.iterations) : (i += 1) {
        const thread_id = context.lock.lock();
        _ = thread_id;

        // Critical section - increment shared counter
        context.counter.* += 1;

        context.lock.unlock();

        // Small delay to increase contention
        std.time.sleep(1 * std.time.ns_per_us);
    }
}

test "GlobalLock: multi-threaded contention" {
    var lock = GlobalLock.init();

    var counter: usize = 0;
    const num_threads = 10;
    const iterations_per_thread = 100;

    var threads: [num_threads]std.Thread = undefined;
    var contexts: [num_threads]ThreadTestContext = undefined;

    // Spawn worker threads
    for (0..num_threads) |i| {
        contexts[i] = .{
            .lock = &lock,
            .counter = &counter,
            .iterations = iterations_per_thread,
        };
        threads[i] = try std.Thread.spawn(.{}, threadWorker, .{&contexts[i]});
    }

    // Wait for all threads
    for (threads) |thread| {
        thread.join();
    }

    // Verify counter is correct (all increments were atomic)
    const expected = num_threads * iterations_per_thread;
    try testing.expectEqual(expected, counter);
}
