# Iteration 254 — Quick Reference Summary

## Target Commands

| Command | Arity | ACL | Stub? | Notes |
|---------|-------|-----|-------|-------|
| MODULE LOAD path [arg ...] | ≥3 | @admin @slow @dangerous | Yes | Returns ERR (stub) |
| MODULE UNLOAD name | 3 | @admin @slow @dangerous | Yes | Returns ERR (stub) |
| MODULE LIST | 2 | @admin @slow @dangerous | Yes | Returns empty array |
| MODULE HELP | 2 | @slow | No | Returns 4-line help array |

## RESP Format Examples

### MODULE HELP (Full Implementation)
```
*4\r\n
$80\r\n
MODULE LOAD <path> [arg ...] - Load a module at runtime with optional arguments\r\n
$35\r\n
MODULE UNLOAD <name> - Unload a module\r\n
$33\r\n
MODULE LIST - List all loaded modules\r\n
$34\r\n
MODULE HELP - Print this help message\r\n
```

### MODULE LIST (Stub - Empty)
```
*0\r\n
```

### MODULE LIST (Future - RESP2 with 1 module)
```
*1\r\n
*8\r\n
$4\r\nname\r\n
$8\r\nmymodule\r\n
$3\r\nver\r\n
:12345\r\n
$4\r\npath\r\n
$23\r\n/usr/lib/mymodule.so\r\n
$4\r\nargs\r\n
*2\r\n
$4\r\narg1\r\n
$4\r\narg2\r\n
```

### MODULE LIST (Future - RESP3 with 1 module)
```
*1\r\n
%4\r\n
+name\r\n
$8\r\nmymodule\r\n
+ver\r\n
:12345\r\n
+path\r\n
$23\r\n/usr/lib/mymodule.so\r\n
+args\r\n
*2\r\n
$4\r\narg1\r\n
$4\r\narg2\r\n
```

### MODULE LOAD (Stub Error)
```
-ERR Module loading not yet supported\r\n
```

### MODULE UNLOAD (Stub Error)
```
-ERR Module unloading not yet supported\r\n
```

## File Checklist

### New Files
- [ ] `src/storage/modules.zig` (ModuleStore, stub)
- [ ] `src/commands/modules.zig` (command handlers)
- [ ] `tests/test_modules.zig` (storage unit tests)
- [ ] `tests/test_modules_integration.zig` (command integration tests)

### Modified Files
- [ ] `src/storage/memory.zig` (add module_store field)
- [ ] `src/commands/strings.zig` (MODULE dispatcher)
- [ ] `build.zig` (add test steps)
- [ ] `tests/integration_test.sh` (add E2E tests)
- [ ] `docs/milestones.md` (Phase 17 tracking)
- [ ] `README.md` (MODULE commands list)

## Key Error Messages

| Condition | Error Message |
|-----------|---------------|
| Stub LOAD | `ERR Module loading not yet supported` |
| Stub UNLOAD | `ERR Module unloading not yet supported` |
| Missing args | `ERR wrong number of arguments for 'module\|<subcommand>' command` |
| Unknown subcommand | `ERR unknown MODULE subcommand` |
| Future: Module not found | `ERR Error loading module: file not found` |
| Future: Already loaded | `ERR Module already loaded` |
| Future: Init failed | `ERR Module initialization failed` |
| Future: Has data types | `ERR Error unloading module: the module exports one or more module-side data types, can't unload` |
| Future: No such module | `ERR No such module with that name` |

## Test Coverage Requirements

### Storage Layer (4 tests)
1. ✅ ModuleStore init/deinit
2. ✅ loadModule returns NotSupported
3. ✅ unloadModule returns NotSupported
4. ✅ listModules returns empty array

### Command Layer (9 tests)
1. ✅ MODULE HELP returns 4 lines
2. ✅ MODULE LIST RESP2 returns empty array
3. ✅ MODULE LIST RESP3 returns empty array
4. ✅ MODULE LOAD stub returns error
5. ✅ MODULE LOAD validates arguments (arity check)
6. ✅ MODULE LOAD accepts path with args
7. ✅ MODULE UNLOAD stub returns error
8. ✅ MODULE UNLOAD validates arguments (arity check)
9. ✅ MODULE UNLOAD rejects multiple args

### E2E Tests (6 scenarios)
1. ✅ MODULE HELP contains all subcommands
2. ✅ MODULE LIST returns empty array
3. ✅ MODULE LOAD returns error (stub)
4. ✅ MODULE UNLOAD returns error (stub)
5. ✅ MODULE with no args returns arity error
6. ✅ MODULE UNKNOWN returns unknown subcommand error

## Implementation Order

1. **Storage layer first** (`src/storage/modules.zig`)
   - ModuleStore struct with stub methods
   - Integration into Storage struct

2. **Command layer** (`src/commands/modules.zig`)
   - cmdModuleHelp (full)
   - cmdModuleList (stub - empty array)
   - cmdModuleLoad (stub - error)
   - cmdModuleUnload (stub - error)

3. **Dispatcher integration** (`src/commands/strings.zig`)
   - MODULE routing with subcommand dispatch
   - Add to skip_redirect_cmds

4. **Tests** (TDD)
   - Write failing tests first
   - Implement to pass tests
   - Zero memory leaks required

5. **Documentation**
   - Update docs/milestones.md
   - Update README.md

## Future Iterations Preview

### Iteration 255 - Dynamic Library Loading
- std.DynLib.open() platform abstraction
- Symbol resolution for RedisModule_OnLoad
- Module context creation

### Iteration 256 - Module API Function Table
- RedisModule_Init implementation
- RedisModule_CreateCommand
- RedisModule_Call (command execution)
- RedisModule_ReplyWith* (response builders)

### Iteration 257 - Custom Data Types
- RedisModule_RegisterDataType
- RDB serialization callbacks
- AOF rewrite callbacks

### Iteration 258 - Background Operations
- Thread-safe contexts
- Blocking command support
- Timer registration

### Iteration 259 - Hooks & Notifications
- Key miss/change/delete hooks
- Cron hooks
- Module-level event system

### Iteration 260 - MODULE LOADEX
- CONFIG argument parsing
- ARGS keyword support
- Configuration passing to modules

## Phase 17 Progress Tracker

- **Iteration 254**: Foundation (LOAD/UNLOAD/LIST/HELP stubs) ← YOU ARE HERE
- **Iterations 255-260**: Real module loading, API, custom types, hooks
- **Target**: 8-10 iterations total
- **Phase 17 Completion**: ~5% after this iteration

## Stub → Real Migration Checklist

When implementing real module loading (Iteration 255+):

1. Replace ModuleStore stub methods with real implementations
2. Add ModuleInfo struct with handle field
3. Implement dynamic library loading (std.DynLib)
4. Add module context (RedisModuleCtx)
5. Implement OnLoad callback invocation
6. Update error messages (remove "not yet supported")
7. Update cmdModuleLoad to handle real success (+OK)
8. Update cmdModuleUnload to handle real unload
9. Update cmdModuleList to return real module data
10. Add comprehensive tests for real loading/unloading
11. Update docs to mark commands as "Full" instead of "Stub"

## Quick Build & Test

```bash
# Build
zig build

# Run all tests
zig build test

# Run server
./zig-out/bin/zoltraak

# Manual testing
redis-cli -p 6379
> MODULE HELP
> MODULE LIST
> MODULE LOAD /fake.so
> MODULE UNLOAD fake
```

## Expected redis-cli Output

```redis
127.0.0.1:6379> MODULE HELP
1) "MODULE LOAD <path> [arg ...] - Load a module at runtime with optional arguments"
2) "MODULE UNLOAD <name> - Unload a module"
3) "MODULE LIST - List all loaded modules"
4) "MODULE HELP - Print this help message"

127.0.0.1:6379> MODULE LIST
(empty array)

127.0.0.1:6379> MODULE LOAD /fake.so
(error) ERR Module loading not yet supported

127.0.0.1:6379> MODULE UNLOAD fake
(error) ERR Module unloading not yet supported

127.0.0.1:6379> MODULE UNKNOWN
(error) ERR unknown MODULE subcommand
```

## Commit Message

```
feat(modules): implement Iteration 254 — MODULE API foundation (Phase 17)

- Storage: ModuleStore stub with init/deinit
- Commands: MODULE LOAD/UNLOAD (stubs), MODULE LIST (empty), MODULE HELP (full)
- Dispatcher: MODULE routing in strings.zig
- Tests: 4 unit + 9 integration + 6 E2E tests
- Phase 17: 0% → 5% complete (1/8-10 iterations)

MODULE LOAD/UNLOAD return "not yet supported" error (real dynamic
loading deferred to Iteration 255+). MODULE LIST returns empty array.
MODULE HELP returns full help text. All tests pass, zero memory leaks.
```

## Related PRD Sections

- **Phase 17** (docs/PRD.md lines 727-751): Modules API requirements
- **Dependencies** (docs/PRD.md line 930): Phase 17 depends on Phase 8 (Cluster)
- **Estimated iterations**: 8-10 total for Phase 17

## Key Design Decisions

1. **Stub-first approach**: Command routing + syntax validation now, real loading later
2. **4-field MODULE LIST format**: Implement Redis 8.0 extended format (name/ver/path/args)
3. **RESP3 support**: Both RESP2 (flat array) and RESP3 (map) for MODULE LIST
4. **Case-insensitive subcommands**: MODULE load == MODULE LOAD
5. **Defer std.DynLib**: Platform-specific dynamic loading is complex, isolate to later iteration
6. **Extension points ready**: ModuleInfo struct designed for future handle/OnLoad fields

## Redis Compatibility Target

| Feature | Redis 4.0 | Zoltraak v0.1 | Zoltraak v1.0 Goal |
|---------|-----------|---------------|--------------------|
| MODULE LOAD | ✅ | ⚠️ Stub | ✅ Full |
| MODULE UNLOAD | ✅ | ⚠️ Stub | ✅ Full |
| MODULE LIST (2 fields) | ✅ | ⚠️ Stub | ✅ Full |
| MODULE LIST (4 fields) | ❌ (8.0+) | ⚠️ Stub | ✅ Full |
| MODULE HELP | ❌ (5.0+) | ✅ Full | ✅ Full |
| Dynamic loading | ✅ | ❌ | ✅ Full |
| Custom commands | ✅ | ❌ | ✅ Full |
| Custom data types | ✅ | ❌ | ✅ Full |

---

**Ready for TDD Implementation**
- Read full spec: `.claude/specs/iteration-254-module-api.md`
- Next: `unit-test-writer` agent to write failing tests
- Then: `zig-implementor` agent to make tests pass
