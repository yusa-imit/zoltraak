# Redis Functions API — Phase 11.1 Foundation Specification
## Iteration 163: FUNCTION LOAD + redis.register_function()

**Author**: redis-spec-analyzer
**Date**: 2026-04-04
**Target**: Zoltraak v0.2.0 — Phase 11 (Redis Functions)
**Redis Version**: 7.0.0+
**Depends on**: Phase 2 (Lua Scripting) ✅ — Already complete

---

## Executive Summary

Redis Functions (introduced in Redis 7.0) provide a **library-based scripting model** that improves upon EVAL/EVALSHA:

| Feature | EVAL/EVALSHA | Redis Functions |
|---------|--------------|-----------------|
| **Persistence** | Not persistent | Persisted in RDB/AOF |
| **Organization** | Ad-hoc scripts | Named libraries with multiple functions |
| **Registration** | Load on every execution | Load once, call by name |
| **Management** | Manual SHA1 tracking | Built-in lifecycle commands |
| **Reusability** | Requires client-side caching | Server-side reusable by name |
| **Metadata** | None | Name, description, flags per function |

**Phase 11.1 Goal**: Implement the **foundation layer** for Redis Functions, enabling library registration and basic function execution. This iteration delivers:

1. **Storage**: `FunctionStore` for persistent library and function metadata
2. **Command**: `FUNCTION LOAD` with REPLACE flag
3. **Lua API**: `redis.register_function()` bridge
4. **Command**: `FCALL` for calling registered functions
5. **Integration**: RDB/AOF persistence hooks

---

## 1. Architecture Overview

### 1.1 Component Stack

```
┌─────────────────────────────────────────────────────┐
│  FUNCTION LOAD / FCALL commands                     │
│  (src/commands/functions.zig)                       │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  FunctionStore (src/storage/functions.zig)          │
│  - Library storage (name → Library)                 │
│  - Function registry (name → FunctionInfo)          │
│  - Duplicate detection                              │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  LuaEngine extension (src/scripting/lua_engine.zig) │
│  - redis.register_function() Lua C bridge           │
│  - Function invocation context                      │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  Persistence (src/storage/persistence.zig + aof.zig)│
│  - RDB: Save/load libraries                         │
│  - AOF: Log FUNCTION LOAD/DELETE/FLUSH              │
└─────────────────────────────────────────────────────┘
```

### 1.2 Data Flow: FUNCTION LOAD

```
1. Client: FUNCTION LOAD [REPLACE] "#!lua name=mylib\nredis.register_function(...)"
2. cmdFunctionLoad() parses Shebang → extract library name
3. Check duplicate library (error if exists without REPLACE)
4. Create FunctionRegistrationContext (tracks functions being registered)
5. LuaEngine.loadLibrary():
   - Compile Lua code
   - Execute in sandbox → calls redis.register_function()
6. redis.register_function() bridge:
   - Validate function name
   - Check duplicate across all libraries (error)
   - Add to FunctionRegistrationContext
7. After execution, FunctionStore.addLibrary():
   - Atomically store library + all registered functions
   - If REPLACE: remove old library first
8. AOF: Write "FUNCTION LOAD [REPLACE] <code>"
9. Return: Bulk string reply with library name
```

### 1.3 Data Flow: FCALL

```
1. Client: FCALL myfunction 1 mykey arg1 arg2
2. cmdFcall() parses: function_name, numkeys, keys[], args[]
3. FunctionStore.getFunction(function_name) → FunctionInfo | null
4. Error if function not found
5. LuaEngine.callFunction():
   - Load library code from FunctionInfo.library
   - Set KEYS and ARGV tables
   - Call function
   - Return result
6. Convert Lua result → RESP response
```

---

## 2. Storage Layer: FunctionStore

### 2.1 Data Structures

**File**: `src/storage/functions.zig`

```zig
const std = @import("std");
const Allocator = std.mem.Allocator;

/// A Redis function library (collection of functions)
pub const Library = struct {
    /// Library name (e.g., "mylib")
    name: []const u8,
    /// Engine name (currently only "lua")
    engine: []const u8,
    /// Full library source code (includes Shebang + all function definitions)
    code: []const u8,
    /// Functions defined in this library
    functions: std.StringHashMap(FunctionInfo),
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, engine: []const u8, code: []const u8) !Library {
        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);
        const engine_copy = try allocator.dupe(u8, engine);
        errdefer allocator.free(engine_copy);
        const code_copy = try allocator.dupe(u8, code);
        errdefer allocator.free(code_copy);

        return Library{
            .name = name_copy,
            .engine = engine_copy,
            .code = code_copy,
            .functions = std.StringHashMap(FunctionInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Library) void {
        self.allocator.free(self.name);
        self.allocator.free(self.engine);
        self.allocator.free(self.code);

        var it = self.functions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var info = entry.value_ptr;
            info.deinit(self.allocator);
        }
        self.functions.deinit();
    }
};

/// Metadata for a single function within a library
pub const FunctionInfo = struct {
    /// Function name (e.g., "myfunc")
    name: []const u8,
    /// Optional description
    description: ?[]const u8,
    /// Function flags (currently unused, reserved for future: no-writes, allow-oom, etc.)
    flags: []const []const u8,
    /// Parent library name (pointer to library.name for efficiency)
    library_name: []const u8,

    pub fn deinit(self: *FunctionInfo, allocator: Allocator) void {
        if (self.description) |desc| {
            allocator.free(desc);
        }
        for (self.flags) |flag| {
            allocator.free(flag);
        }
        allocator.free(self.flags);
        // Note: name and library_name are owned by Library, not freed here
    }
};

/// Global function registry and library storage
pub const FunctionStore = struct {
    /// Map: library_name → Library
    libraries: std.StringHashMap(Library),
    /// Map: function_name → *FunctionInfo (for fast lookup by function name)
    /// Note: FunctionInfo is owned by Library, this is just a pointer
    function_index: std.StringHashMap(*FunctionInfo),
    allocator: Allocator,

    pub fn init(allocator: Allocator) FunctionStore {
        return .{
            .libraries = std.StringHashMap(Library).init(allocator),
            .function_index = std.StringHashMap(*FunctionInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FunctionStore) void {
        var it = self.libraries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var lib = entry.value_ptr;
            lib.deinit();
        }
        self.libraries.deinit();
        self.function_index.deinit();
    }

    /// Add a new library (called after successful Lua compilation)
    /// If replace=true, removes existing library with same name first
    pub fn addLibrary(self: *FunctionStore, library: Library, replace: bool) !void {
        // Check if library already exists
        if (self.libraries.get(library.name)) |existing_lib| {
            if (!replace) {
                return error.LibraryAlreadyExists;
            }
            // Remove old library
            try self.removeLibrary(library.name);
        }

        // Check for function name conflicts with other libraries
        var it = library.functions.iterator();
        while (it.next()) |entry| {
            const func_name = entry.key_ptr.*;
            if (self.function_index.contains(func_name)) {
                // Function exists in another library
                return error.FunctionAlreadyExists;
            }
        }

        // Store library
        const name_copy = try self.allocator.dupe(u8, library.name);
        errdefer self.allocator.free(name_copy);

        try self.libraries.put(name_copy, library);

        // Index all functions
        var lib_ptr = self.libraries.getPtr(name_copy).?;
        var func_it = lib_ptr.functions.iterator();
        while (func_it.next()) |entry| {
            const func_name_copy = try self.allocator.dupe(u8, entry.key_ptr.*);
            errdefer self.allocator.free(func_name_copy);
            try self.function_index.put(func_name_copy, entry.value_ptr);
        }
    }

    /// Remove a library by name
    pub fn removeLibrary(self: *FunctionStore, name: []const u8) !void {
        var lib = self.libraries.get(name) orelse return error.LibraryNotFound;

        // Remove functions from index
        var it = lib.functions.iterator();
        while (it.next()) |entry| {
            const func_name = entry.key_ptr.*;
            if (self.function_index.fetchRemove(func_name)) |kv| {
                self.allocator.free(kv.key);
            }
        }

        // Remove library
        if (self.libraries.fetchRemove(name)) |kv| {
            self.allocator.free(kv.key);
            var removed_lib = kv.value;
            removed_lib.deinit();
        }
    }

    /// Get function info by name (returns null if not found)
    pub fn getFunction(self: *FunctionStore, name: []const u8) ?*FunctionInfo {
        return self.function_index.get(name);
    }

    /// Get library by name
    pub fn getLibrary(self: *FunctionStore, name: []const u8) ?*Library {
        return self.libraries.getPtr(name);
    }

    /// Flush all libraries and functions
    pub fn flush(self: *FunctionStore) void {
        var it = self.libraries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var lib = entry.value_ptr;
            lib.deinit();
        }
        self.libraries.clearRetainingCapacity();

        var func_it = self.function_index.iterator();
        while (func_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.function_index.clearRetainingCapacity();
    }
};
```

### 2.2 Key Design Decisions

1. **Dual indexing**: `libraries` for library-level operations, `function_index` for fast O(1) function lookup by name
2. **Ownership**: Library owns FunctionInfo, function_index stores pointers (avoids duplication)
3. **Atomicity**: `addLibrary()` is atomic — either succeeds completely or fails without partial state
4. **Duplicate detection**: Cross-library function name uniqueness enforced at registration time
5. **Memory safety**: All string fields are owned copies, proper deinit() cleanup

---

## 3. FUNCTION LOAD Command

### 3.1 Syntax

```
FUNCTION LOAD [REPLACE] function-code
```

### 3.2 Shebang Format

The first line of `function-code` **must** be a Shebang that specifies:

```
#!<engine> name=<library_name>
```

**Example**:
```lua
#!lua name=mylib
redis.register_function('myfunc', function(keys, args)
    return 'Hello from ' .. (keys[1] or 'anonymous')
end)
```

### 3.3 Parsing Logic

**File**: `src/commands/functions.zig`

```zig
/// Parse Shebang line: "#!lua name=mylib"
/// Returns: (engine, library_name)
fn parseShebang(code: []const u8) !struct { engine: []const u8, library_name: []const u8 } {
    // Find first newline
    const newline_idx = std.mem.indexOfScalar(u8, code, '\n') orelse return error.InvalidShebang;
    const first_line = code[0..newline_idx];

    // Check "#!" prefix
    if (!std.mem.startsWith(u8, first_line, "#!")) {
        return error.InvalidShebang;
    }

    const after_shebang = first_line[2..];

    // Find space separator between engine and attributes
    const space_idx = std.mem.indexOfScalar(u8, after_shebang, ' ') orelse return error.InvalidShebang;
    const engine = std.mem.trim(u8, after_shebang[0..space_idx], &std.ascii.whitespace);

    // Currently only "lua" engine is supported
    if (!std.mem.eql(u8, engine, "lua")) {
        return error.UnsupportedEngine;
    }

    const attributes = std.mem.trim(u8, after_shebang[space_idx + 1 ..], &std.ascii.whitespace);

    // Parse "name=<library_name>"
    if (!std.mem.startsWith(u8, attributes, "name=")) {
        return error.MissingLibraryName;
    }

    const library_name = attributes[5..]; // Skip "name="
    if (library_name.len == 0) {
        return error.EmptyLibraryName;
    }

    return .{ .engine = engine, .library_name = library_name };
}
```

### 3.4 Error Conditions

| Error | Condition | Response |
|-------|-----------|----------|
| Syntax error | Wrong number of arguments | `ERR wrong number of arguments for 'function\|load' command` |
| Invalid Shebang | Missing "#!" or malformed | `ERR library code must start with a Shebang statement` |
| Unsupported engine | Engine is not "lua" | `ERR unsupported engine (only 'lua' is supported)` |
| Missing library name | Shebang missing `name=` | `ERR missing library name in Shebang` |
| Empty library name | `name=` with no value | `ERR library name cannot be empty` |
| Library exists | Library name already exists without REPLACE | `ERR library already exists (use REPLACE to overwrite)` |
| Function exists | Function name exists in another library | `ERR function name already exists in another library` |
| Compilation error | Lua syntax error | `ERR <lua error message>` |
| No functions | Library doesn't register any functions | `ERR library must declare at least one function` |

### 3.5 Return Value

**Success**: Bulk string reply containing the library name

```
$5
mylib
```

---

## 4. Lua API: redis.register_function()

### 4.1 Syntax

**Lua function signature**:

```lua
redis.register_function(name, callback)
redis.register_function({
    function_name = 'myfunc',
    callback = function(keys, args) ... end,
    description = 'My function description',
    flags = {} -- Reserved for future (no-writes, allow-oom, etc.)
})
```

### 4.2 Validation Rules

1. **Function name**: Must be non-empty string
2. **Callback**: Must be a Lua function
3. **Duplicate check**: Within library (error if duplicate)
4. **Cross-library check**: Done in FunctionStore.addLibrary() (error if exists in another library)
5. **At least one function**: Library must register at least one function

---

## 5. FCALL Command

### 5.1 Syntax

```
FCALL function numkeys [key [key ...]] [arg [arg ...]]
```

### 5.2 Key Differences from EVAL

| Feature | EVAL | FCALL |
|---------|------|-------|
| **Script source** | Inline Lua code | Pre-registered function name |
| **Key specification** | Optional | **Mandatory** (must explicitly list all keys) |
| **Compilation** | Every execution | Once at FUNCTION LOAD time |
| **Persistence** | Not persisted | Persisted in RDB/AOF |
| **Error on missing keys** | No | Yes (ERR Function not found) |

### 5.3 Return Value

**RESP2/RESP3**: Depends on function's Lua return value (same conversion rules as EVAL)

---

## 6. Integration Points

### 6.1 Storage Struct Extension

**File**: `src/storage/memory.zig`

```zig
pub const Storage = struct {
    // ... existing fields ...
    function_store: FunctionStore,

    pub fn init(allocator: Allocator, port: u16, host: []const u8) !Storage {
        // ... existing init code ...
        .function_store = FunctionStore.init(allocator),
    }

    pub fn deinit(self: *Storage) void {
        // ... existing deinit code ...
        self.function_store.deinit();
    }
};
```

### 6.2 Command Routing

**File**: `src/server.zig` (existing command dispatcher)

Add to command routing table:

```zig
if (std.ascii.eqlIgnoreCase(command, "FUNCTION")) {
    if (args.len < 1) {
        return try writer_mod.Writer.init(arena_allocator).writeError("ERR wrong number of arguments");
    }
    const subcommand = args[0];
    const subargs = args[1..];

    if (std.ascii.eqlIgnoreCase(subcommand, "LOAD")) {
        return try functions_mod.cmdFunctionLoad(arena_allocator, &storage.function_store, subargs, aof);
    }
    // ... other subcommands in future iterations
}

if (std.ascii.eqlIgnoreCase(command, "FCALL")) {
    return try functions_mod.cmdFcall(arena_allocator, &storage.function_store, storage, args, /* ... */);
}
```

### 6.3 RDB Persistence

**File**: `src/storage/persistence.zig` (extend existing RDB save/load)

Add custom RDB opcode (e.g., 250) for function libraries, save library metadata (name, engine, code).

### 6.4 AOF Persistence

Handled in `cmdFunctionLoad()` — logs `FUNCTION LOAD [REPLACE] <code>` to AOF.

---

## 7. Testing Strategy

### 7.1 Unit Tests (25+ tests)

- FunctionStore: add/retrieve library, reject duplicates, REPLACE flag
- Shebang parsing: valid/invalid formats, edge cases
- FunctionRegistrationContext: register/deduplicate functions

### 7.2 Integration Tests (12+ tests)

- FUNCTION LOAD: basic registration, REPLACE flag, duplicate errors
- FCALL: call registered functions, error on missing function
- RDB: save/load libraries, verify persistence
- AOF: replay FUNCTION LOAD commands

### 7.3 Redis Compatibility Tests

Use `redis-cli` to compare byte-by-byte with Redis 7.0+ for all commands.

---

## 8. Implementation Roadmap (Iteration 163)

### 8.1 Phase 2 (TDD) — Order of Implementation

1. **Storage layer**: `src/storage/functions.zig` (Library, FunctionInfo, FunctionStore)
2. **Shebang parser**: `src/commands/functions.zig` (parseShebang)
3. **FUNCTION LOAD command**: `src/commands/functions.zig` (cmdFunctionLoad)
4. **Lua bridge**: `src/scripting/lua_engine.zig` (FunctionRegistrationContext, luaRegisterFunction)
5. **FCALL command**: `src/commands/functions.zig` (cmdFcall — stub)
6. **Integration**: Wire to Storage, command routing
7. **Persistence**: RDB save/load, AOF logging

### 8.2 Estimated Complexity

| Component | LOC | Complexity |
|-----------|-----|------------|
| Storage layer | 250 | Medium |
| Shebang parser | 50 | Small |
| FUNCTION LOAD | 150 | Medium |
| Lua bridge | 200 | Large (C interop) |
| FCALL (stub) | 80 | Small |
| Integration | 50 | Small |
| Persistence | 100 | Medium |
| Tests | 400 | Medium |
| **Total** | **1,280** | **Large** |

### 8.3 Success Criteria

- [ ] All unit tests pass (30+ tests)
- [ ] All integration tests pass (12+ tests)
- [ ] FUNCTION LOAD successfully registers libraries
- [ ] REPLACE flag works correctly
- [ ] Duplicate detection works (library + function names)
- [ ] redis.register_function() callable from Lua
- [ ] FCALL calls registered functions (basic execution)
- [ ] RDB persistence saves/loads libraries
- [ ] AOF logs FUNCTION LOAD commands
- [ ] Zero memory leaks (std.testing.allocator)
- [ ] Byte-compatible with Redis 7.0+ for FUNCTION LOAD responses

---

## 9. Future Iterations (Phase 11.2-11.4)

### Iteration 164: FUNCTION Management Commands
- FUNCTION DELETE <library>
- FUNCTION FLUSH [ASYNC|SYNC]
- FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
- FUNCTION STATS (running function info)

### Iteration 165: Read-Only Functions
- FCALL_RO / FUNCTION CALL_RO
- Function flags: no-writes, allow-stale
- Enforce read-only semantics

### Iteration 166: Advanced Features
- FUNCTION DUMP / FUNCTION RESTORE
- FUNCTION KILL (kill running function)
- Function flags: allow-oom, no-cluster, allow-cross-slot-keys, raw-arguments
- Complete Lua bridge for all function features

---

## 10. References

### Official Redis Documentation
- [FUNCTION LOAD](https://redis.io/docs/latest/commands/function-load/)
- [Redis Functions Introduction](https://redis.io/docs/latest/develop/interact/programmability/functions-intro/)
- [FCALL](https://redis.io/docs/latest/commands/fcall/)
- [FUNCTION LIST](https://redis.io/docs/latest/commands/function-list/)

### Zoltraak Internal References
- `src/scripting/lua_engine.zig` — Existing Lua 5.1 engine
- `src/scripting/redis_api.zig` — redis.call/pcall bridge
- `src/commands/scripting.zig` — EVAL/EVALSHA implementation
- `src/storage/scripting.zig` — ScriptStore (SHA1 caching)
- `docs/PRD.md` — Phase 11 roadmap

---

## 11. Summary

**Iteration 163** lays the **foundation for Redis Functions** in Zoltraak by implementing:

1. **Persistent library storage** via `FunctionStore`
2. **FUNCTION LOAD** command with Shebang parsing and REPLACE flag
3. **redis.register_function()** Lua API for declaring functions
4. **FCALL** command for invoking registered functions
5. **RDB/AOF persistence** for libraries

This iteration enables the core workflow:
```
FUNCTION LOAD → Library registration → Function invocation via FCALL
```

The architecture is designed for extensibility:
- **Function flags** (no-writes, allow-oom, etc.) — reserved but not yet enforced
- **FUNCTION CALL_RO** — command structure ready, enforcement pending
- **FUNCTION DELETE/FLUSH/LIST** — storage layer supports, commands pending

**Next steps**: Iteration 164 will implement the remaining management commands (DELETE, FLUSH, LIST, STATS) to complete Phase 11 Redis Functions support.

---

**End of Specification**
