# Iteration 254 — Phase 17 Modules API Foundation

**Target**: MODULE LOAD, MODULE UNLOAD, MODULE LIST, MODULE HELP commands
**Status**: Specification phase
**Redis versions**: 4.0.0+ (core commands), 5.0.0+ (MODULE HELP), 8.0+ (extended MODULE LIST format)

---

## 1. Overview

Redis Modules API allows loading custom dynamic libraries (.so/.dylib/.dll) at runtime to extend Redis with:
- Custom commands
- Custom data types
- Background tasks
- Hooks and notifications

This iteration implements **Phase 17.1: Foundation commands** for module lifecycle management.

---

## 2. Command Specifications

### 2.1 MODULE LOAD

**Syntax**:
```
MODULE LOAD path [arg [arg ...]]
```

**Parameters**:
- `path` (string, required): Absolute path to dynamic library file (.so on Linux, .dylib on macOS, .dll on Windows)
- `arg` (string, optional, multiple): Arguments passed unmodified to module's `RedisModule_OnLoad()` function

**Return Type**:
- **Success**: Simple string reply `+OK\r\n`
- **Error**: Error string with specific error type

**Error Conditions**:

| Error | Message | Trigger |
|-------|---------|---------|
| ERR | `ERR Error loading the extension. Please check the server logs.` | Generic load failure (invalid path, not a shared library, missing symbols) |
| ERR | `ERR Module already loaded` | Module with same name already registered |
| ERR | `ERR Module initialization failed` | Module's `RedisModule_OnLoad()` returned error |
| ERR | `ERR API version mismatch` | Module requires unsupported API version |
| ERR | `ERR Error loading module: <specific error>` | Dynamic library loader error (file not found, permission denied, unsupported architecture) |

**Time Complexity**: O(1) plus module initialization cost

**ACL Categories**: `@admin`, `@slow`, `@dangerous`

**Command Flags**: `admin`, `noscript`, `no_async_loading`

**Examples**:
```redis
MODULE LOAD /path/to/mymodule.so
+OK

MODULE LOAD /path/to/mymodule.so arg1 arg2 arg3
+OK

MODULE LOAD /nonexistent.so
-ERR Error loading module: /nonexistent.so: cannot open shared object file: No such file or directory

MODULE LOAD /path/to/mymodule.so
-ERR Module already loaded
```

**Behavior Notes**:
1. Path must be absolute (no relative paths like `./module.so`)
2. Arguments are passed as `char **argv, int argc` to module's `RedisModule_OnLoad()`
3. Module name is determined by what the module registers via `RedisModule_Init()`, not by filename
4. Good practice: filename should match registered module name (e.g., `mymodule.so` registers as "mymodule")
5. Modules loaded at startup via `loadmodule` config directive use same mechanism

**Redis.conf Alternative**:
```conf
loadmodule /path/to/module1.so arg1 arg2
loadmodule /path/to/module2.so
```

**Specification References**:
- [MODULE LOAD Command](https://redis.io/docs/latest/commands/module-load/)
- [Redis Modules API](https://redis.io/docs/latest/develop/reference/modules/)

---

### 2.2 MODULE UNLOAD

**Syntax**:
```
MODULE UNLOAD name
```

**Parameters**:
- `name` (string, required): Module's registered name (as reported by `MODULE LIST`), **not** the filename

**Return Type**:
- **Success**: Simple string reply `+OK\r\n`
- **Error**: Error string with specific error type

**Error Conditions**:

| Error | Message | Trigger |
|-------|---------|---------|
| ERR | `ERR No such module with that name` | Module name not found in loaded modules |
| ERR | `ERR Error unloading module: the module exports one or more module-side data types, can't unload` | Module registered custom data types that are in use |
| ERR | `ERR Module unload failed` | Module's `RedisModule_OnUnload()` returned `REDISMODULE_ERR` |
| ERR | `ERR the module has keys in use` | Module has active keys using its data types (variant of data type error) |

**Time Complexity**: O(1) plus module cleanup cost

**ACL Categories**: `@admin`, `@slow`, `@dangerous`

**Command Flags**: `admin`, `noscript`

**Examples**:
```redis
MODULE UNLOAD mymodule
+OK

MODULE UNLOAD nonexistent
-ERR No such module with that name

MODULE UNLOAD redisbloom
-ERR Error unloading module: the module exports one or more module-side data types, can't unload
```

**Behavior Notes**:
1. **CRITICAL LIMITATION**: Modules that register custom data types **cannot** be unloaded if any keys of that type exist
2. Module name is case-sensitive (matches what module registered via `RedisModule_Init()`)
3. Module can prevent unloading by returning `REDISMODULE_ERR` from its `RedisModule_OnUnload()` callback
4. After unload, all commands registered by module are removed from command table
5. If module has background threads/timers, it must clean them up in `RedisModule_OnUnload()`

**Specification References**:
- [MODULE UNLOAD Command](https://redis.io/docs/latest/commands/module-unload/)
- [Redis Modules API - Unloading](https://redis.io/docs/latest/develop/reference/modules/)

---

### 2.3 MODULE LIST

**Syntax**:
```
MODULE LIST
```

**Parameters**: None (arity = 2)

**Return Type**:

**RESP2**: Array reply - list of loaded modules, each element is a flat array of alternating property names (bulk strings) and values:
```
*2
*8
$4
name
$8
mymodule
$3
ver
:12345
$4
path
$/path/to/mymodule.so
$4
args
*2
$4
arg1
$4
arg2
*8
$4
name
...
```

**RESP3**: Array reply - list of loaded modules, each element is a **map reply** of property names → values:
```
*2
%4
+name
$8
mymodule
+ver
:12345
+path
$/path/to/mymodule.so
+args
*2
$4
arg1
$4
arg2
%4
...
```

**Response Structure**:

Each module entry contains **4 fields** (as of Redis 8.0, extended from 2 fields in Redis 4.0):

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `name` | Bulk string | Module's registered name | `"mymodule"` |
| `ver` | Integer | Module version (from `RedisModule_Init()`) | `12345` |
| `path` | Bulk string | Absolute path where module was loaded from | `"/usr/lib/redis/modules/mymodule.so"` |
| `args` | Array of bulk strings | Arguments passed to module at load time | `["arg1", "arg2"]` or `[]` (empty) |

**Time Complexity**: O(N) where N is the number of loaded modules

**ACL Categories**: `@admin`, `@slow`, `@dangerous`

**Command Flags**: `admin`, `noscript`

**Examples**:

**RESP2 format** (redis-cli output):
```redis
MODULE LIST
1) 1) "name"
   2) "mymodule"
   3) "ver"
   4) (integer) 12345
   5) "path"
   6) "/usr/lib/redis/modules/mymodule.so"
   7) "args"
   8) 1) "arg1"
      2) "arg2"
2) 1) "name"
   2) "redisbloom"
   3) "ver"
   4) (integer) 20403
   5) "path"
   6) "/opt/redis/modules/redisbloom.so"
   7) "args"
   8) (empty array)
```

**RESP3 format**:
```redis
1# 1=> "name"
      "mymodule"
   2=> "ver"
      (integer) 12345
   3=> "path"
      "/usr/lib/redis/modules/mymodule.so"
   4=> "args"
      1) "arg1"
      2) "arg2"
2# 1=> "name"
      "redisbloom"
   ...
```

**Behavior Notes**:
1. **Format evolution**: Redis 4.0-7.x returned only `name` and `ver` (2 fields), Redis 8.0+ added `path` and `args` (4 fields total)
2. Empty module list returns empty array `*0\r\n`
3. `args` field is an array (can be empty `*0\r\n` if no args were passed)
4. Module version is an integer reported by module via `RedisModule_Init(ctx, "name", version)` second parameter
5. Path is the exact path string passed to `MODULE LOAD` (or `loadmodule` config directive)
6. Order of modules in list is **insertion order** (not alphabetical)

**Specification References**:
- [MODULE LIST Command](https://redis.io/docs/latest/commands/module-list/)
- [Redis PR #4848 - Extended MODULE LIST format](https://github.com/redis/redis/pull/4848/files)

---

### 2.4 MODULE HELP

**Syntax**:
```
MODULE HELP
```

**Parameters**: None (arity = 2)

**Return Type**: Array reply - list of help text lines (bulk strings)

**Response Format**:
```
*6
$80
MODULE LOAD <path> [arg ...] - Load a module at runtime with optional arguments
$35
MODULE UNLOAD <name> - Unload a module
$33
MODULE LIST - List all loaded modules
$34
MODULE HELP - Print this help message
```

**Expected Output** (redis-cli):
```redis
MODULE HELP
1) "MODULE LOAD <path> [arg ...] - Load a module at runtime with optional arguments"
2) "MODULE UNLOAD <name> - Unload a module"
3) "MODULE LIST - List all loaded modules"
4) "MODULE HELP - Print this help message"
```

**Time Complexity**: O(1)

**ACL Categories**: `@slow`

**Command Flags**: `loading`, `stale`

**Availability**: Redis 5.0.0+

**Behavior Notes**:
1. Static help text (not dynamically generated from loaded modules)
2. Should list all MODULE subcommands (LOAD, UNLOAD, LIST, HELP, and in future: LOADEX)
3. Returns array of bulk strings (one string per help line)
4. Each line follows format: `<COMMAND> <args> - <description>`
5. **No error conditions** - always succeeds

**Specification References**:
- [MODULE HELP Command](https://redis.io/docs/latest/commands/module-help/)

---

## 3. Storage Layer Requirements

### 3.1 Data Structures

**File**: `src/storage/modules.zig` (new file)

```zig
const std = @import("std");
const Allocator = std.mem.Allocator;

/// Represents a loaded module instance
pub const ModuleInfo = struct {
    /// Module's registered name (as passed to RedisModule_Init)
    name: []const u8,

    /// Module version (integer from RedisModule_Init)
    ver: i64,

    /// Absolute path to the loaded .so/.dylib/.dll file
    path: []const u8,

    /// Arguments passed to module at load time (can be empty)
    args: [][]const u8,

    /// Dynamic library handle (platform-specific)
    handle: ?std.DynLib,

    /// Allocator for memory cleanup
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, ver: i64, path: []const u8, args: [][]const u8) !ModuleInfo;
    pub fn deinit(self: *ModuleInfo) void;
};

/// Module store tracking all loaded modules
pub const ModuleStore = struct {
    /// Map of module name → ModuleInfo
    modules: std.StringHashMap(ModuleInfo),

    /// Allocator for store operations
    allocator: Allocator,

    pub fn init(allocator: Allocator) ModuleStore;
    pub fn deinit(self: *ModuleStore) void;

    /// Load a module from dynamic library
    pub fn loadModule(self: *ModuleStore, path: []const u8, args: [][]const u8) !void;

    /// Unload a module by name
    pub fn unloadModule(self: *ModuleStore, name: []const u8) !void;

    /// Get all loaded modules (caller owns returned slice)
    pub fn listModules(self: *ModuleStore, allocator: Allocator) ![]ModuleInfo;

    /// Check if module with given name is loaded
    pub fn hasModule(self: *ModuleStore, name: []const u8) bool;

    /// Get module info by name (returns null if not found)
    pub fn getModule(self: *ModuleStore, name: []const u8) ?*ModuleInfo;
};
```

### 3.2 Error Types

```zig
pub const ModuleError = error{
    /// Dynamic library file not found
    ModuleNotFound,

    /// Module with same name already loaded
    ModuleAlreadyLoaded,

    /// Module name not in loaded modules map
    NoSuchModule,

    /// Module initialization failed (OnLoad returned error)
    InitializationFailed,

    /// Module exports custom data types, cannot unload
    HasDataTypes,

    /// Module's OnUnload callback returned error
    UnloadFailed,

    /// API version mismatch
    ApiVersionMismatch,

    /// Invalid dynamic library format
    InvalidLibrary,

    /// Permission denied loading library
    PermissionDenied,
};
```

### 3.3 Integration with Storage

**File**: `src/storage/memory.zig` (additions)

```zig
const modules_mod = @import("modules.zig");

pub const Storage = struct {
    // ... existing fields ...

    /// Module store for MODULE LOAD/UNLOAD/LIST
    module_store: modules_mod.ModuleStore,

    pub fn init(allocator: Allocator) !Storage {
        // ... existing init code ...

        var module_store = modules_mod.ModuleStore.init(allocator);

        return Storage{
            // ... existing fields ...
            .module_store = module_store,
        };
    }

    pub fn deinit(self: *Storage) void {
        // ... existing cleanup ...
        self.module_store.deinit();
    }
};
```

---

## 4. Command Layer Requirements

### 4.1 Command Handlers

**File**: `src/commands/modules.zig` (new file)

```zig
const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Writer = @import("../protocol/writer.zig").Writer;
const RespValue = @import("../protocol/writer.zig").RespValue;

/// MODULE LOAD path [arg ...]
pub fn cmdModuleLoad(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) ![]const u8 {
    // Validation: require at least path argument
    if (args.len < 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|load' command");
    }

    const path = args[0];
    const module_args = if (args.len > 1) args[1..] else &[_][]const u8{};

    // Attempt to load module
    storage.module_store.loadModule(path, module_args) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();

        const error_msg = switch (err) {
            error.ModuleNotFound => "ERR Error loading module: file not found",
            error.ModuleAlreadyLoaded => "ERR Module already loaded",
            error.InitializationFailed => "ERR Module initialization failed",
            error.ApiVersionMismatch => "ERR API version mismatch",
            error.InvalidLibrary => "ERR Error loading the extension. Please check the server logs.",
            error.PermissionDenied => "ERR Error loading module: permission denied",
            else => "ERR Error loading the extension. Please check the server logs.",
        };

        return try w.writeError(error_msg);
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// MODULE UNLOAD name
pub fn cmdModuleUnload(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) ![]const u8 {
    // Validation: require module name
    if (args.len != 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'module|unload' command");
    }

    const name = args[0];

    // Attempt to unload module
    storage.module_store.unloadModule(name) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();

        const error_msg = switch (err) {
            error.NoSuchModule => "ERR No such module with that name",
            error.HasDataTypes => "ERR Error unloading module: the module exports one or more module-side data types, can't unload",
            error.UnloadFailed => "ERR Module unload failed",
            else => "ERR Error unloading module",
        };

        return try w.writeError(error_msg);
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// MODULE LIST
pub fn cmdModuleList(
    allocator: std.mem.Allocator,
    storage: *Storage,
    protocol_version: u8, // 2 or 3 for RESP2/RESP3
) ![]const u8 {
    const modules = try storage.module_store.listModules(allocator);
    defer allocator.free(modules);

    var w = Writer.init(allocator);
    defer w.deinit();

    if (protocol_version == 3) {
        // RESP3: Array of Maps
        try w.writeArrayLen(modules.len);

        for (modules) |module| {
            // Each module is a map with 4 fields
            try w.writeMapLen(4);

            // name
            try w.writeSimpleString("name");
            try w.writeBulkString(module.name);

            // ver
            try w.writeSimpleString("ver");
            try w.writeInteger(module.ver);

            // path
            try w.writeSimpleString("path");
            try w.writeBulkString(module.path);

            // args
            try w.writeSimpleString("args");
            try w.writeArrayLen(module.args.len);
            for (module.args) |arg| {
                try w.writeBulkString(arg);
            }
        }
    } else {
        // RESP2: Array of flat arrays (alternating key-value pairs)
        try w.writeArrayLen(modules.len);

        for (modules) |module| {
            // Each module is array of 8 elements (4 key-value pairs)
            try w.writeArrayLen(8);

            try w.writeBulkString("name");
            try w.writeBulkString(module.name);

            try w.writeBulkString("ver");
            try w.writeInteger(module.ver);

            try w.writeBulkString("path");
            try w.writeBulkString(module.path);

            try w.writeBulkString("args");
            try w.writeArrayLen(module.args.len);
            for (module.args) |arg| {
                try w.writeBulkString(arg);
            }
        }
    }

    return try w.toOwnedSlice();
}

/// MODULE HELP
pub fn cmdModuleHelp(allocator: std.mem.Allocator) ![]const u8 {
    const help_lines = [_][]const u8{
        "MODULE LOAD <path> [arg ...] - Load a module at runtime with optional arguments",
        "MODULE UNLOAD <name> - Unload a module",
        "MODULE LIST - List all loaded modules",
        "MODULE HELP - Print this help message",
    };

    var w = Writer.init(allocator);
    defer w.deinit();

    try w.writeArrayLen(help_lines.len);
    for (help_lines) |line| {
        try w.writeBulkString(line);
    }

    return try w.toOwnedSlice();
}
```

### 4.2 Command Dispatcher Integration

**File**: `src/commands/strings.zig` (additions)

In the main command dispatcher loop, add MODULE routing (similar to CLUSTER/SENTINEL/FUNCTION):

```zig
// MODULE commands
else if (std.mem.eql(u8, cmd_upper, "MODULE")) {
    if (array.len < 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        break :blk try w.writeError("ERR wrong number of arguments for 'module' command");
    }

    const subcmd_raw = try extractBulkString(array[1]);
    var subcmd_upper: [32]u8 = undefined;
    const subcmd = std.ascii.upperString(&subcmd_upper, subcmd_raw);

    if (std.mem.eql(u8, subcmd, "LOAD")) {
        const args_load = try extractBulkStrings(allocator, array[2..]);
        defer allocator.free(args_load);
        break :blk try modules_mod.cmdModuleLoad(allocator, storage, args_load);
    }
    else if (std.mem.eql(u8, subcmd, "UNLOAD")) {
        const args_unload = try extractBulkStrings(allocator, array[2..]);
        defer allocator.free(args_unload);
        break :blk try modules_mod.cmdModuleUnload(allocator, storage, args_unload);
    }
    else if (std.mem.eql(u8, subcmd, "LIST")) {
        // MODULE LIST takes no arguments
        if (array.len != 2) {
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeError("ERR wrong number of arguments for 'module|list' command");
        }
        break :blk try modules_mod.cmdModuleList(allocator, storage, client_ctx.protocol_version);
    }
    else if (std.mem.eql(u8, subcmd, "HELP")) {
        if (array.len != 2) {
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeError("ERR wrong number of arguments for 'module|help' command");
        }
        break :blk try modules_mod.cmdModuleHelp(allocator);
    }
    else {
        var w = Writer.init(allocator);
        defer w.deinit();
        break :blk try w.writeError("ERR unknown MODULE subcommand");
    }
}
```

Add MODULE to the skip_redirect_cmds list:
```zig
const skip_redirect_cmds = [_][]const u8{
    "PING", "INFO", "AUTH", "HELLO", "CLUSTER", "SENTINEL", "ASKING", "MIGRATE",
    "READONLY", "READWRITE", "MODULE", // <-- ADD THIS
    "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
    // ...
};
```

---

## 5. Stub Implementation Strategy

For **Iteration 254**, we implement MODULE commands as **stubs** (similar to initial CLUSTER/SENTINEL implementation):

### 5.1 Stub Behavior

| Command | Stub Implementation |
|---------|---------------------|
| MODULE LOAD | Returns `-ERR Module loading not yet supported` |
| MODULE UNLOAD | Returns `-ERR Module unloading not yet supported` |
| MODULE LIST | Returns empty array `*0\r\n` (no modules loaded) |
| MODULE HELP | Returns full help array (real implementation) |

### 5.2 Stub Storage Layer

```zig
// src/storage/modules.zig (stub version)
pub const ModuleStore = struct {
    allocator: Allocator,

    pub fn init(allocator: Allocator) ModuleStore {
        return ModuleStore{ .allocator = allocator };
    }

    pub fn deinit(self: *ModuleStore) void {
        _ = self;
    }

    pub fn loadModule(self: *ModuleStore, path: []const u8, args: [][]const u8) !void {
        _ = self;
        _ = path;
        _ = args;
        return error.NotSupported;
    }

    pub fn unloadModule(self: *ModuleStore, name: []const u8) !void {
        _ = self;
        _ = name;
        return error.NotSupported;
    }

    pub fn listModules(self: *ModuleStore, allocator: Allocator) ![]ModuleInfo {
        _ = self;
        // Return empty slice (no modules loaded in stub)
        return try allocator.alloc(ModuleInfo, 0);
    }
};

pub const ModuleError = error{
    NotSupported,
    NoSuchModule,
};
```

### 5.3 Stub Command Layer

```zig
// src/commands/modules.zig (stub version)
pub fn cmdModuleLoad(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) ![]const u8 {
    _ = storage;
    _ = args;

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeError("ERR Module loading not yet supported");
}

pub fn cmdModuleUnload(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: [][]const u8,
) ![]const u8 {
    _ = storage;
    _ = args;

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeError("ERR Module unloading not yet supported");
}

pub fn cmdModuleList(
    allocator: std.mem.Allocator,
    storage: *Storage,
    protocol_version: u8,
) ![]const u8 {
    _ = storage;
    _ = protocol_version;

    var w = Writer.init(allocator);
    defer w.deinit();

    // Return empty array (no modules loaded)
    try w.writeArrayLen(0);
    return try w.toOwnedSlice();
}

pub fn cmdModuleHelp(allocator: std.mem.Allocator) ![]const u8 {
    // HELP is fully functional (not a stub)
    const help_lines = [_][]const u8{
        "MODULE LOAD <path> [arg ...] - Load a module at runtime with optional arguments",
        "MODULE UNLOAD <name> - Unload a module",
        "MODULE LIST - List all loaded modules",
        "MODULE HELP - Print this help message",
    };

    var w = Writer.init(allocator);
    defer w.deinit();

    try w.writeArrayLen(help_lines.len);
    for (help_lines) |line| {
        try w.writeBulkString(line);
    }

    return try w.toOwnedSlice();
}
```

### 5.4 Why Stub?

Dynamic library loading requires:
1. **Platform-specific dynamic loader** (`dlopen` on Unix, `LoadLibrary` on Windows)
2. **Module API function table** (RedisModule_Init, RedisModule_CreateCommand, etc.)
3. **Symbol resolution** for ~100+ API functions
4. **Custom data type registration** and serialization
5. **Thread-safe contexts** for background operations

This is **8-10 iterations** worth of work. For Iteration 254, we:
- ✅ Implement command routing and argument parsing
- ✅ Implement MODULE HELP (full)
- ✅ Implement MODULE LIST (stub - returns empty)
- ✅ Implement MODULE LOAD/UNLOAD (stub - returns error)
- ✅ Add integration tests for command syntax
- ⏭️ Defer real dynamic loading to future iterations

---

## 6. Test Requirements

### 6.1 Unit Tests (Storage Layer)

**File**: `tests/test_modules.zig`

```zig
const std = @import("std");
const testing = std.testing;
const modules_mod = @import("../src/storage/modules.zig");

test "ModuleStore: init and deinit" {
    var store = modules_mod.ModuleStore.init(testing.allocator);
    defer store.deinit();

    // Should initialize empty
    const list = try store.listModules(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqual(@as(usize, 0), list.len);
}

test "ModuleStore: loadModule returns NotSupported (stub)" {
    var store = modules_mod.ModuleStore.init(testing.allocator);
    defer store.deinit();

    const result = store.loadModule("/fake/path.so", &[_][]const u8{});
    try testing.expectError(error.NotSupported, result);
}

test "ModuleStore: unloadModule returns NotSupported (stub)" {
    var store = modules_mod.ModuleStore.init(testing.allocator);
    defer store.deinit();

    const result = store.unloadModule("fakename");
    try testing.expectError(error.NotSupported, result);
}

test "ModuleStore: listModules returns empty array" {
    var store = modules_mod.ModuleStore.init(testing.allocator);
    defer store.deinit();

    const list = try store.listModules(testing.allocator);
    defer testing.allocator.free(list);

    try testing.expectEqual(@as(usize, 0), list.len);
}
```

### 6.2 Integration Tests (Command Layer)

**File**: `tests/test_modules_integration.zig`

```zig
const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const modules_cmd = @import("../src/commands/modules.zig");

test "MODULE HELP: returns help text array" {
    const result = try modules_cmd.cmdModuleHelp(testing.allocator);
    defer testing.allocator.free(result);

    // Should be RESP array with 4 elements
    try testing.expect(std.mem.startsWith(u8, result, "*4\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "MODULE LOAD") != null);
    try testing.expect(std.mem.indexOf(u8, result, "MODULE UNLOAD") != null);
    try testing.expect(std.mem.indexOf(u8, result, "MODULE LIST") != null);
}

test "MODULE LIST: returns empty array (stub)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const result = try modules_cmd.cmdModuleList(testing.allocator, &storage, 2);
    defer testing.allocator.free(result);

    // Should return empty RESP array
    try testing.expectEqualStrings("*0\r\n", result);
}

test "MODULE LIST: RESP3 format returns empty array" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const result = try modules_cmd.cmdModuleList(testing.allocator, &storage, 3);
    defer testing.allocator.free(result);

    // Should return empty RESP3 array
    try testing.expectEqualStrings("*0\r\n", result);
}

test "MODULE LOAD: returns error (stub)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const args = [_][]const u8{"/path/to/module.so"};
    const result = try modules_cmd.cmdModuleLoad(testing.allocator, &storage, &args);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try testing.expect(std.mem.indexOf(u8, result, "not yet supported") != null);
}

test "MODULE LOAD: validates arguments" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    // No arguments
    const result = try modules_cmd.cmdModuleLoad(testing.allocator, &storage, &[_][]const u8{});
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-ERR wrong number of arguments") != null);
}

test "MODULE LOAD: accepts path with arguments" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "/path/to/module.so", "arg1", "arg2" };
    const result = try modules_cmd.cmdModuleLoad(testing.allocator, &storage, &args);
    defer testing.allocator.free(result);

    // Should still return "not supported" but accept the syntax
    try testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

test "MODULE UNLOAD: returns error (stub)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const args = [_][]const u8{"mymodule"};
    const result = try modules_cmd.cmdModuleUnload(testing.allocator, &storage, &args);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try testing.expect(std.mem.indexOf(u8, result, "not yet supported") != null);
}

test "MODULE UNLOAD: validates arguments" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    // No arguments
    const result = try modules_cmd.cmdModuleUnload(testing.allocator, &storage, &[_][]const u8{});
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-ERR wrong number of arguments") != null);
}

test "MODULE UNLOAD: rejects multiple arguments" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "module1", "module2" };
    const result = try modules_cmd.cmdModuleUnload(testing.allocator, &storage, &args);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-ERR wrong number of arguments") != null);
}
```

### 6.3 E2E Tests (redis-cli via bash)

**File**: `tests/integration_test.sh` (additions)

```bash
echo "Testing MODULE commands..."

# MODULE HELP should return help array
RESP=$(redis-cli -p 6379 MODULE HELP)
echo "$RESP" | grep -q "MODULE LOAD" || fail "MODULE HELP missing LOAD"
echo "$RESP" | grep -q "MODULE UNLOAD" || fail "MODULE HELP missing UNLOAD"
echo "$RESP" | grep -q "MODULE LIST" || fail "MODULE HELP missing LIST"

# MODULE LIST should return empty array (stub)
RESP=$(redis-cli -p 6379 MODULE LIST)
[ "$RESP" = "(empty array)" ] || fail "MODULE LIST should return empty array"

# MODULE LOAD should return error (stub)
RESP=$(redis-cli -p 6379 MODULE LOAD /fake/path.so 2>&1)
echo "$RESP" | grep -q "ERR" || fail "MODULE LOAD should return error"

# MODULE UNLOAD should return error (stub)
RESP=$(redis-cli -p 6379 MODULE UNLOAD fakename 2>&1)
echo "$RESP" | grep -q "ERR" || fail "MODULE UNLOAD should return error"

# MODULE with no subcommand should error
RESP=$(redis-cli -p 6379 MODULE 2>&1)
echo "$RESP" | grep -q "ERR wrong number of arguments" || fail "MODULE arity check failed"

# MODULE with unknown subcommand should error
RESP=$(redis-cli -p 6379 MODULE UNKNOWN 2>&1)
echo "$RESP" | grep -q "ERR unknown MODULE subcommand" || fail "MODULE unknown subcommand check failed"

echo "MODULE commands: OK"
```

---

## 7. Build Integration

**File**: `build.zig` (add test step)

```zig
// Add modules unit tests
const test_modules = b.addTest(.{
    .root_source_file = b.path("tests/test_modules.zig"),
    .target = target,
    .optimize = optimize,
});
test_modules.root_module.addImport("sailor", sailor);
test_step.step.dependOn(&b.addRunArtifact(test_modules).step);

// Add modules integration tests
const test_modules_integration = b.addTest(.{
    .root_source_file = b.path("tests/test_modules_integration.zig"),
    .target = target,
    .optimize = optimize,
});
test_modules_integration.root_module.addImport("sailor", sailor);
test_step.step.dependOn(&b.addRunArtifact(test_modules_integration).step);
```

---

## 8. Phase Completion Tracking

**File**: `docs/milestones.md` (update after iteration)

```markdown
### Phase 17 — Modules API (5% complete)

| Iteration | Command | Status |
|-----------|---------|--------|
| 254 | Foundation (MODULE LOAD/UNLOAD/LIST/HELP stubs, ModuleStore) | Done ✅ |
| 255+ | Dynamic library loading (std.DynLib, symbol resolution) | Planned |
| 256+ | Module API function table (RedisModule_Init, CreateCommand) | Planned |
| ... | Custom data types, blocking commands, hooks | Planned |

**Commands implemented (stubs)**: MODULE LOAD, MODULE UNLOAD, MODULE LIST, MODULE HELP
```

---

## 9. Future Iterations

### 9.1 Real Dynamic Loading (Iteration 255+)

```zig
// src/storage/modules.zig (future real implementation)
pub fn loadModule(self: *ModuleStore, path: []const u8, args: [][]const u8) !void {
    // 1. Check if module already loaded
    // 2. Open dynamic library with std.DynLib
    var lib = try std.DynLib.open(path);
    errdefer lib.close();

    // 3. Resolve RedisModule_OnLoad symbol
    const onload_fn = lib.lookup(RedisModuleOnLoadFn, "RedisModule_OnLoad") orelse
        return error.InvalidLibrary;

    // 4. Call OnLoad with module context
    const ctx = try createModuleContext(allocator);
    defer destroyModuleContext(ctx);

    const result = onload_fn(ctx, args.ptr, @intCast(args.len));
    if (result != REDISMODULE_OK) {
        return error.InitializationFailed;
    }

    // 5. Extract module name and version from context
    const module_info = try ModuleInfo.init(
        allocator,
        ctx.module_name,
        ctx.module_version,
        path,
        args,
    );
    module_info.handle = lib;

    // 6. Add to modules map
    try self.modules.put(module_info.name, module_info);
}
```

### 9.2 Module API Function Table (Iteration 256+)

Implement ~100 RedisModule_* functions:
- RedisModule_Init
- RedisModule_CreateCommand
- RedisModule_RegisterDataType
- RedisModule_Call
- RedisModule_ReplyWithString
- ... (see redismodule.h for full list)

### 9.3 Custom Data Types (Iteration 257+)

Support module-defined types with:
- RDB serialization callbacks
- AOF rewrite callbacks
- Memory usage callbacks
- Free callbacks

---

## 10. Compatibility Matrix

| Feature | Redis 4.0 | Redis 5.0 | Redis 7.0 | Redis 8.0 | Zoltraak v0.1 | Zoltraak v1.0 |
|---------|-----------|-----------|-----------|-----------|---------------|---------------|
| MODULE LOAD | ✅ | ✅ | ✅ | ✅ | ⚠️ Stub | 🎯 Full |
| MODULE UNLOAD | ✅ | ✅ | ✅ | ✅ | ⚠️ Stub | 🎯 Full |
| MODULE LIST (2 fields) | ✅ | ✅ | ✅ | ✅ | ⚠️ Stub | 🎯 Full |
| MODULE LIST (4 fields) | ❌ | ❌ | ❌ | ✅ | ⚠️ Stub | 🎯 Full |
| MODULE HELP | ❌ | ✅ | ✅ | ✅ | ✅ Full | ✅ Full |
| MODULE LOADEX | ❌ | ❌ | ✅ | ✅ | ❌ | 🎯 Planned |

Legend:
- ✅ = Fully supported
- ⚠️ = Stub implementation (command exists, returns error/empty)
- 🎯 = Target for v1.0
- ❌ = Not supported

---

## 11. References

### Official Redis Documentation
- [MODULE LOAD](https://redis.io/docs/latest/commands/module-load/)
- [MODULE UNLOAD](https://redis.io/docs/latest/commands/module-unload/)
- [MODULE LIST](https://redis.io/docs/latest/commands/module-list/)
- [MODULE HELP](https://redis.io/docs/latest/commands/module-help/)
- [Redis Modules API](https://redis.io/docs/latest/develop/reference/modules/)
- [Modules API Reference](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/)

### Source Code
- [Redis PR #4848 - Extended MODULE LIST](https://github.com/redis/redis/pull/4848/files)
- [Redis modules.c source](https://github.com/redis/redis/blob/unstable/src/module.c)
- [redismodule.h header](https://github.com/redis/redis/blob/unstable/src/redismodule.h)

### Community Resources
- [Redis Modules SDK](https://github.com/RedisLabsModules/RedisModulesSDK)

---

## 12. Success Criteria

**Iteration 254 is complete when**:

✅ **Storage Layer**:
- [ ] `src/storage/modules.zig` created with ModuleStore struct
- [ ] ModuleStore.init/deinit implemented
- [ ] Stub methods for loadModule/unloadModule/listModules
- [ ] ModuleStore integrated into Storage struct

✅ **Command Layer**:
- [ ] `src/commands/modules.zig` created
- [ ] cmdModuleLoad stub (returns error)
- [ ] cmdModuleUnload stub (returns error)
- [ ] cmdModuleList stub (returns empty array)
- [ ] cmdModuleHelp full implementation (returns help text)

✅ **Dispatcher Integration**:
- [ ] MODULE routing added to strings.zig
- [ ] Subcommand dispatcher for LOAD/UNLOAD/LIST/HELP
- [ ] MODULE added to skip_redirect_cmds

✅ **Tests**:
- [ ] 4 unit tests in test_modules.zig
- [ ] 9 integration tests in test_modules_integration.zig
- [ ] E2E tests in integration_test.sh
- [ ] All tests pass with `zig build test`

✅ **Quality**:
- [ ] Zero memory leaks (std.testing.allocator)
- [ ] All error paths have proper cleanup
- [ ] Doc comments on all public functions
- [ ] RESP2 and RESP3 compatibility

✅ **Documentation**:
- [ ] Phase 17 status updated in docs/milestones.md
- [ ] README.md updated with MODULE commands
- [ ] CLAUDE.md updated if new patterns emerged

---

**End of Specification — Ready for Test-First Implementation**
