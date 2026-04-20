# Iteration 212 Development Plan — BF.INSERT Implementation

## Overview

**Iteration**: 212
**Phase**: 15.3 (Probabilistic Data Structures)
**Feature**: BF.INSERT (Bloom Filter Insert with auto-creation)
**Status**: Planning
**Redis Compatibility Target**: 100% specification compliance

---

## What is BF.INSERT?

`BF.INSERT` is the primary user-facing Bloom filter command that combines:
1. **Auto-creation** with customizable parameters (CAPACITY, ERROR, EXPANSION, NONSCALING)
2. **Batch insertion** of multiple items
3. **Conditional creation** (NOCREATE flag prevents creation if key doesn't exist)

**Key difference from BF.ADD/BF.MADD**:
- BF.ADD: Single item, auto-create with defaults only
- BF.MADD: Multiple items, auto-create with defaults only
- BF.INSERT: **Multiple items, full parameter control for creation** ← New!

---

## Current State (Before Iteration 212)

### Already Implemented ✅

| Command | Status | Notes |
|---------|--------|-------|
| BF.RESERVE | Done | Manual filter creation with full parameters |
| BF.ADD | Done | Single item, auto-create with defaults |
| BF.EXISTS | Done | Single item membership test |
| BF.MADD | Done | Multiple items, auto-create with defaults |
| BF.MEXISTS | Done | Multiple items, membership test array |

### Storage Layer ✅

**File**: `/Users/fn/codespace/zoltraak/src/storage/bloom.zig`

Already supports:
- `BloomFilterValue` struct with all needed fields
- Automatic scaling (sub-filter creation)
- NONSCALING mode (prevents scaling, allows error rate growth)
- `add()` method returning 1/0 (new/duplicate)
- Optimal parameter calculation (k hashes, m bits)
- Double hashing with MurmurHash3

### Command Layer Partial ✅

**File**: `/Users/fn/codespace/zoltraak/src/commands/bloom.zig`

Implemented:
- `cmdBfReserve()` — Full parameter parsing
- `cmdBfAdd()` — Single item with auto-create
- `cmdBfMadd()` — Batch with auto-create
- `cmdBfExists()`, `cmdBfMexists()` — Membership tests

### Missing

**BF.INSERT command handler** — Not yet implemented

---

## Specification Summary

### Command Syntax

```
BF.INSERT key [CAPACITY n] [ERROR x] [EXPANSION x] [NOCREATE] [NONSCALING] ITEMS item [item ...]
```

### Core Behaviors

#### 1. Filter Creation (if doesn't exist)

| Scenario | Behavior |
|----------|----------|
| No NOCREATE flag | Create with CAPACITY (default: 100), ERROR (default: 0.01), EXPANSION (default: 2), NONSCALING (default: false) |
| NOCREATE flag present | Return error: `ERR no such key` |
| Filter already exists | Ignore CAPACITY, ERROR, EXPANSION, NONSCALING; use existing filter's config |

#### 2. Batch Insertion

- Insert items in order
- Collect results: 1 (new), 0 (duplicate), or error string (NONSCALING overflow)
- Return array of results

#### 3. Capacity Overflow

**Scaling filters (default)**:
- Automatically create sub-filter when capacity * expansion threshold reached
- Insertion continues seamlessly
- Lookup checks all sub-filters

**Non-scaling filters (NONSCALING flag)**:
- When capacity reached, return error string for overflowing items
- Items before overflow are still inserted (partial success)
- No new sub-filters created

#### 4. Return Value

**RESP2**: `[1, 0, 1, ...]` or `[1, "ERR filter is full", ...]`
**RESP3**: `[true, false, true, ...]` or `[true, "ERR filter is full", ...]`

---

## Implementation Plan

### Phase 1: Tests First (TDD)

**File to create**: `tests/test_bf_insert.zig`

#### Test Categories

**1. Filter Creation Tests** (8 tests)
```zig
test "BF.INSERT auto-create with defaults"
test "BF.INSERT auto-create with CAPACITY"
test "BF.INSERT auto-create with ERROR"
test "BF.INSERT auto-create with EXPANSION"
test "BF.INSERT auto-create with NONSCALING"
test "BF.INSERT with CAPACITY + EXPANSION + NONSCALING"
test "BF.INSERT ignores creation params when filter exists"
test "BF.INSERT preserves existing filter configuration"
```

**2. NOCREATE Tests** (4 tests)
```zig
test "BF.INSERT NOCREATE with existing filter inserts"
test "BF.INSERT NOCREATE with nonexistent filter returns error"
test "BF.INSERT NOCREATE + CAPACITY returns error"
test "BF.INSERT NOCREATE + ERROR returns error"
```

**3. Item Insertion Tests** (6 tests)
```zig
test "BF.INSERT single item returns [1]"
test "BF.INSERT multiple new items returns all 1s"
test "BF.INSERT mixed new/duplicate items returns mix"
test "BF.INSERT duplicate items in same call"
test "BF.INSERT with binary-safe items"
test "BF.INSERT returns correct array length"
```

**4. Return Value Tests** (5 tests)
```zig
test "BF.INSERT returns 1 for new items (RESP2)"
test "BF.INSERT returns 0 for duplicates (RESP2)"
test "BF.INSERT RESP3 returns true for new (if RESP3)"
test "BF.INSERT RESP3 returns false for duplicates (if RESP3)"
test "BF.INSERT with zero items returns error"
```

**5. Scaling Behavior Tests** (4 tests)
```zig
test "BF.INSERT scaling filter creates sub-filters automatically"
test "BF.INSERT nonscaling filter rejects overflow"
test "BF.INSERT nonscaling returns error string in array"
test "BF.INSERT nonscaling partial success committed"
```

**6. Error Cases Tests** (6 tests)
```zig
test "BF.INSERT invalid CAPACITY (negative)"
test "BF.INSERT invalid ERROR (0 or 1)"
test "BF.INSERT invalid EXPANSION (0)"
test "BF.INSERT WRONGTYPE error (key is string)"
test "BF.INSERT missing ITEMS keyword"
test "BF.INSERT empty ITEMS list"
```

**7. Parameter Validation Tests** (4 tests)
```zig
test "BF.INSERT CAPACITY must be positive integer"
test "BF.INSERT ERROR must be valid float (0,1)"
test "BF.INSERT EXPANSION must be positive integer"
test "BF.INSERT NOCREATE + CAPACITY is invalid"
```

**8. Edge Cases Tests** (5 tests)
```zig
test "BF.INSERT very large CAPACITY"
test "BF.INSERT very small ERROR (0.0001)"
test "BF.INSERT EXPANSION = 1"
test "BF.INSERT after filter expires (TTL)"
test "BF.INSERT concurrent operations (thread safety)"
```

**Total: 42 comprehensive tests**

### Phase 2: Implementation

**File to modify**: `/Users/fn/codespace/zoltraak/src/commands/bloom.zig`

#### Implementation Steps

1. **Function signature**
```zig
pub fn cmdBfInsert(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue
) !RespValue {
    // Implementation
}
```

2. **Argument parsing** (parse order matters)
```
Parse: key, [CAPACITY n], [ERROR x], [EXPANSION x], [NOCREATE], [NONSCALING], ITEMS item [item ...]

Error checks:
- NOCREATE + CAPACITY → error
- NOCREATE + ERROR → error
- ITEMS not found → error
- < 1 item → error
- Invalid CAPACITY (not positive integer) → error
- Invalid ERROR (not float in 0,1) → error
- Invalid EXPANSION (not positive integer) → error
```

3. **Filter logic**
```zig
if (filter exists) {
    // Ignore creation params, use existing config
    add items to existing filter
} else if (NOCREATE) {
    return error "no such key"
} else {
    create new filter with (CAPACITY, ERROR, EXPANSION, NONSCALING)
    add items to new filter
}
```

4. **Result array construction**
```zig
// Allocate results array
var results = try allocator.alloc(RespValue, num_items)
errdefer allocator.free(results)

// For each item:
// - Call filter.add(item) → 1 or 0
// - If NONSCALING + overflow → error string
// - Store in results[i]

return RespValue{ .array = results }
```

5. **RESP3 support**
```zig
// If client.protocol_version == 3:
//   results[i] = RespValue{ .boolean = true/false }
// Else (RESP2):
//   results[i] = RespValue{ .integer = 1/0 }
```

#### Code Structure

```zig
fn cmdBfInsert(...) !RespValue {
    // 1. Validate arity
    if (args.len < 2) return error("wrong number of arguments");

    // 2. Parse key
    const key = switch (args[0]) { ... };

    // 3. Parse optional flags (CAPACITY, ERROR, EXPANSION, NOCREATE, NONSCALING)
    var capacity: u64 = 100;
    var error_rate: f64 = 0.01;
    var expansion: u16 = 2;
    var nocreate = false;
    var nonscaling = false;
    var items_start_idx: usize = 1;

    var i: usize = 1;
    while (i < args.len) {
        const arg = switch (args[i]) { .bulk_string => |s| s, else => error(...) };
        const arg_lower = toLower(arg);

        if (std.mem.eql(u8, arg_lower, "capacity")) {
            // Parse capacity argument
            i += 1;
            capacity = parseInt(args[i]);
            validate capacity > 0
        } else if (std.mem.eql(u8, arg_lower, "error")) {
            // Parse error argument
            i += 1;
            error_rate = parseFloat(args[i]);
            validate 0 < error_rate < 1
        } else if (std.mem.eql(u8, arg_lower, "expansion")) {
            // Parse expansion argument
            i += 1;
            expansion = parseInt(args[i]);
            validate expansion > 0
        } else if (std.mem.eql(u8, arg_lower, "nocreate")) {
            nocreate = true;
        } else if (std.mem.eql(u8, arg_lower, "nonscaling")) {
            nonscaling = true;
        } else if (std.mem.eql(u8, arg_lower, "items")) {
            items_start_idx = i + 1;
            break;
        }
        i += 1;
    }

    // 4. Validate NOCREATE conflicts
    if (nocreate and (capacity != 100 or error_rate != 0.01)) {
        return error("NOCREATE cannot be used with CAPACITY or ERROR");
    }

    // 5. Allocate result array for items
    const num_items = args.len - items_start_idx;
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    // 6. Lock storage and process
    storage.mutex.lock();
    defer storage.mutex.unlock();

    if (storage.data.getEntry(key)) |entry| {
        // Filter exists
        switch (entry.value_ptr.*) {
            .bloom => |*filter| {
                for (args[items_start_idx..], 0..) |arg, idx| {
                    const item = switch (arg) { .bulk_string => |s| s, else => error(...) };
                    const result = try filter.add(item);
                    results[idx] = formatResult(result); // 1/0 or error
                }
            },
            else => return error("WRONGTYPE..."),
        }
    } else {
        // Filter doesn't exist
        if (nocreate) {
            allocator.free(results);
            return error("no such key");
        }

        // Create new filter
        var filter = try BloomFilterValue.init(allocator, error_rate, capacity, expansion, nonscaling);
        errdefer filter.deinit();

        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        // Add items
        for (args[items_start_idx..], 0..) |arg, idx| {
            const item = switch (arg) { .bulk_string => |s| s, else => error(...) };
            const result = try filter.add(item);
            results[idx] = formatResult(result);
        }

        try storage.data.put(owned_key, Value{ .bloom = filter });
    }

    return RespValue{ .array = results };
}

fn formatResult(result: u8) RespValue {
    // If RESP3: return boolean
    // If RESP2: return integer
    return RespValue{ .integer = result };
}
```

3. **Command Registration**

In `/Users/fn/codespace/zoltraak/src/server.zig`:
```zig
"bf.insert" => try cmdBfInsert(allocator, storage, args),
```

### Phase 3: Code Quality Review

**Checklist**:
- [ ] Memory safety: All allocations have errdefer cleanup
- [ ] Error handling: All error paths return meaningful messages
- [ ] RESP3 support: Boolean vs integer return type handling
- [ ] Documentation: Function doc comments explain parameters, return values, errors
- [ ] Consistency: Follows pattern of BF.RESERVE, BF.MADD
- [ ] No panics: All error conditions handled gracefully

**Key Review Points**:
- Memory leak check: Allocate → defer free in all paths
- NOCREATE mutual exclusion validation
- Creation parameter handling (ignored if filter exists)
- NONSCALING overflow handling
- RESP3 boolean formatting (if applicable)

### Phase 4: Integration Testing

**Test file**: `tests/test_bf_insert_integration.zig`

**Scenarios**:
1. Full RESP protocol request-response cycle
2. Mixed operations: BF.RESERVE, BF.ADD, BF.INSERT sequentially
3. Large batch: 1000 items via BF.INSERT
4. Parameter combinations: All valid combinations of CAPACITY, ERROR, EXPANSION, NOCREATE, NONSCALING
5. redis-cli compatibility

### Phase 5: Validation

**Checklist**:
- [ ] All 42 unit tests pass
- [ ] All integration tests pass
- [ ] Memory leak check: 0 leaks (valgrind/asan)
- [ ] Byte-by-byte RESP compatibility with real Redis
- [ ] redis-benchmark throughput >= 70% of real Redis

### Phase 6: Documentation

**Updates**:
- [ ] docs/specifications/BF.INSERT.md — Detailed spec (already created)
- [ ] README.md — Add BF.INSERT to command table
- [ ] src/commands/bloom.zig — Function doc comments

---

## Complexity Estimates

| Phase | Task | LOC | Effort |
|-------|------|-----|--------|
| 2 | Argument parsing | 80 | Medium |
| 2 | Creation logic | 60 | Medium |
| 2 | Batch insertion loop | 40 | Small |
| 2 | Error handling | 50 | Medium |
| 2 | RESP3 support | 20 | Small |
| **Total** | **Implementation** | **250** | **Medium** |
| 1 | Test suite (42 tests) | 600 | Large |
| 3 | Code review & fixes | 50 | Medium |
| 4 | Integration tests | 150 | Medium |

---

## Risk Factors

1. **NOCREATE + CAPACITY/ERROR detection**: Must validate mutual exclusion
2. **Scaling filter overflow**: Ensure sub-filter creation happens at right threshold
3. **NONSCALING partial success**: Items inserted before overflow must be committed
4. **Memory leaks**: Many allocation paths (key dup, filter init, results array)
5. **RESP3 boolean support**: Ensure formatResult() respects protocol version

---

## Success Criteria

- [ ] 42 unit tests pass (100% coverage)
- [ ] 0 memory leaks (verified with valgrind/asan)
- [ ] All integration tests pass
- [ ] Byte-by-byte RESP compatibility
- [ ] redis-cli accepts and executes correctly
- [ ] Throughput >= 70% of real Redis BF.INSERT
- [ ] Code reviewed and approved

---

## Dependencies & Prerequisites

**Must be in place before implementation**:
- [x] BloomFilterValue struct (already exists)
- [x] add() method (already exists)
- [x] Storage integration (already done for BF.ADD)
- [x] RESP parser (already handles bulk strings)
- [x] Command dispatcher (already has BF.* commands)

**No blockers identified** ✅

---

## Next Steps

1. **Review this plan** with team
2. **Create test file** (`tests/test_bf_insert.zig`)
3. **Write all 42 tests** (TDD-style, failing tests first)
4. **Implement cmdBfInsert()** in `src/commands/bloom.zig`
5. **Register command** in `src/server.zig`
6. **Run full test suite**: `zig build test`
7. **Code review** and fixes
8. **Integration testing**
9. **Commit**: `feat(bloom): implement Iteration 212 — BF.INSERT command`

---

## References

- [BF.INSERT specification](../specifications/BF.INSERT.md)
- [Redis BF.INSERT docs](https://redis.io/commands/bf.insert/)
- [Bloom Filter documentation](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/)
- Current implementation: `/Users/fn/codespace/zoltraak/src/commands/bloom.zig`
- Storage layer: `/Users/fn/codespace/zoltraak/src/storage/bloom.zig`

