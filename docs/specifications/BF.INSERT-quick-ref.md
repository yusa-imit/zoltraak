# BF.INSERT — Quick Reference for Developers

## Syntax

```
BF.INSERT key [CAPACITY n] [ERROR x] [EXPANSION e] [NOCREATE] [NONSCALING] ITEMS item [item ...]
```

## One-Liner

Adds items to a Bloom filter with full control over auto-creation parameters.

---

## Essential Behaviors

### 1. Auto-Create with Defaults
```redis
> BF.INSERT myfilter ITEMS a b c
[1, 1, 1]
# Filter created: capacity=100, error=0.01, expansion=2, scaling=on
```

### 2. Auto-Create with Custom Parameters
```redis
> BF.INSERT myfilter CAPACITY 50000 ERROR 0.001 ITEMS a b c
[1, 1, 1]
# Filter created: capacity=50000, error=0.001, expansion=2 (default), scaling=on
```

### 3. Ignore Parameters if Filter Exists
```redis
> BF.INSERT myfilter CAPACITY 999 ITEMS d e
[0, 1]
# CAPACITY 999 IGNORED; uses existing filter's parameters
```

### 4. NOCREATE — Conditional Insertion
```redis
> BF.INSERT nonexistent NOCREATE ITEMS a
ERR no such key
# Error because filter doesn't exist and NOCREATE prevents creation

> BF.INSERT existing NOCREATE ITEMS a
[1]
# Success; filter already existed
```

### 5. NONSCALING — Reject Overflow
```redis
> BF.INSERT smallfilter CAPACITY 2 NONSCALING ITEMS a b c d
[1, 1, "ERR filter is full", "ERR filter is full"]
# a, b inserted; c, d rejected (partial success, not a transaction!)
```

---

## Return Values

| Scenario | Return |
|----------|--------|
| New item | `1` (RESP2) or `true` (RESP3) |
| Duplicate | `0` (RESP2) or `false` (RESP3) |
| Overflow (NONSCALING) | Error string: `"ERR filter is full"` |
| Filter doesn't exist (NOCREATE) | Error: `ERR no such key` |
| Wrong type (not Bloom filter) | Error: `WRONGTYPE ...` |

---

## Parameter Rules

| Parameter | Valid Range | Default | Notes |
|-----------|-------------|---------|-------|
| CAPACITY | > 0 | 100 | Pre-allocates memory for N items |
| ERROR | (0.0, 1.0) | 0.01 | False positive probability |
| EXPANSION | > 0 | 2 | Multiplier when scaling |
| NOCREATE | flag | false | Errors if filter doesn't exist |
| NONSCALING | flag | false | No scaling; errors on overflow |

---

## Mutual Exclusions

```
NOCREATE + CAPACITY  → ERROR
NOCREATE + ERROR     → ERROR
```

Rationale: NOCREATE means "don't create," so specifying creation parameters is contradictory.

---

## Implementation Checklist

### Argument Parsing
- [ ] Extract key
- [ ] Parse CAPACITY (validate > 0)
- [ ] Parse ERROR (validate 0 < x < 1)
- [ ] Parse EXPANSION (validate > 0)
- [ ] Check NOCREATE flag
- [ ] Check NONSCALING flag
- [ ] Extract all ITEMS
- [ ] Validate NOCREATE conflicts with CAPACITY/ERROR

### Filter Logic
- [ ] If key exists:
  - [ ] Check it's a Bloom filter (not WRONGTYPE)
  - [ ] Use existing filter (ignore creation params)
- [ ] If key doesn't exist:
  - [ ] If NOCREATE: return `ERR no such key`
  - [ ] Else: create filter with given params (or defaults)

### Insertion Loop
- [ ] For each item:
  - [ ] Call `filter.add(item)` → 1 or 0
  - [ ] Store result in array

### Return
- [ ] Allocate RespValue array
- [ ] Format each result (RESP2: integer, RESP3: boolean)
- [ ] Return array

---

## Common Patterns

### Pattern 1: Simple Batch Insert
```zig
BF.INSERT users ITEMS alice bob charlie
```

### Pattern 2: High-Precision Filter
```zig
BF.INSERT fraud_check CAPACITY 1000000 ERROR 0.0001 ITEMS tx1 tx2 tx3
```

### Pattern 3: Dynamic Growth with Custom Expansion
```zig
BF.INSERT cache CAPACITY 10000 EXPANSION 4 ITEMS item1 item2 ...
// Sub-filter size grows as: initial → initial*4 → initial*4² → ...
```

### Pattern 4: Only Update if Exists
```zig
BF.INSERT existing_only NOCREATE ITEMS newitem1 newitem2
// Fails if filter doesn't already exist
```

### Pattern 5: Fixed-Size Load (Reject Overflow)
```zig
BF.INSERT fixed CAPACITY 5000 NONSCALING ITEMS a b c d e f ...
// Returns mix of 1s (inserted) and error strings (rejected overflow)
```

---

## Key Differences from Similar Commands

### vs BF.ADD
- ADD: single item only
- INSERT: multiple items, custom parameters

### vs BF.MADD
- MADD: multiple items, hardcoded defaults (capacity=100, error=0.01)
- INSERT: multiple items, **customizable parameters**

### vs BF.RESERVE + BF.MADD
- Two commands: BF.RESERVE (create), then BF.MADD (insert)
- INSERT: one command for both (if creating) or one command for insert (if exists)

---

## Edge Cases

### Edge Case 1: Empty ITEMS
```redis
> BF.INSERT myfilter ITEMS
ERR wrong number of arguments
```

### Edge Case 2: Creating Twice
```redis
> BF.INSERT new ITEMS a
[1]
> BF.INSERT new CAPACITY 999 ITEMS b
[0]  # Creation params ignored; filter already exists
```

### Edge Case 3: Very Large Capacity
```redis
> BF.INSERT big CAPACITY 1000000000 ITEMS x
[1]
# May use significant memory; be careful!
```

### Edge Case 4: Scaling Automatically
```redis
> BF.INSERT auto CAPACITY 10 EXPANSION 2 ITEMS i1 i2 ... i100
# When items exceed 10*2=20, new sub-filter created automatically
# Continued insertion succeeds
```

### Edge Case 5: NONSCALING Reaches Capacity
```redis
> BF.INSERT static CAPACITY 3 NONSCALING ITEMS a b c d e
[1, 1, 1, "ERR filter is full", "ERR filter is full"]
# Partial success; items a, b, c inserted; d, e rejected
# This is NOT rolled back (not a transaction)
```

---

## Testing Strategy

**Unit Tests to Write**:
1. Auto-create with defaults
2. Auto-create with custom parameters
3. Existing filter (ignore params)
4. NOCREATE + existing (works)
5. NOCREATE + nonexistent (error)
6. NOCREATE + CAPACITY (mutual exclusion error)
7. Return mixed 1s and 0s
8. NONSCALING overflow
9. New items vs duplicates
10. Error cases (invalid params, wrong type)

**Integration Tests**:
1. Full RESP protocol cycle
2. redis-cli compatibility
3. Concurrent operations
4. Large batches (1000+ items)

---

## Performance Notes

**Time Complexity**: O(k * n)
- k = number of hash functions (depends on error rate)
- n = number of items being inserted

**For typical parameters** (error=0.01):
- k ≈ 7
- O(7n) hash computations per insertion batch

**Scaling Performance**:
- Insertion: O(k) — same as before (only latest filter updated)
- Lookup: O(k * m) — increases with number of sub-filters m

---

## Example Zig Implementation Skeleton

```zig
pub fn cmdBfInsert(allocator, storage, args) !RespValue {
    // 1. Parse key
    const key = args[0].bulk_string;

    // 2. Parse optional parameters
    var capacity: u64 = 100;
    var error_rate: f64 = 0.01;
    var expansion: u16 = 2;
    var nocreate = false;
    var nonscaling = false;
    var items_start_idx: usize = 1;

    // ... parse loop (case-insensitive flag matching) ...

    // 3. Validate
    if (nocreate and (capacity != 100 or error_rate != 0.01)) {
        return error("NOCREATE cannot be used with CAPACITY or ERROR");
    }

    // 4. Allocate results array
    const num_items = args.len - items_start_idx;
    const results = try allocator.alloc(RespValue, num_items);

    // 5. Handle filter logic
    if (storage.data.getEntry(key)) |entry| {
        // Filter exists: use existing config, ignore creation params
        const filter = &entry.value_ptr.*.bloom;
        for (args[items_start_idx..], 0..) |arg, i| {
            const item = arg.bulk_string;
            const result = try filter.add(item);
            results[i] = RespValue{ .integer = result };
        }
    } else {
        // Filter doesn't exist
        if (nocreate) {
            return error("no such key");
        }
        // Create new filter with given params
        var filter = try BloomFilterValue.init(allocator, error_rate, capacity, expansion, nonscaling);
        // ... insert items ...
        // ... store in storage ...
    }

    return RespValue{ .array = results };
}
```

---

## Resources

- [Full Specification](BF.INSERT.md)
- [Development Plan](../iterations/iteration-212-plan.md)
- [Command Comparison](bloom-commands-comparison.md)
- [Redis Docs](https://redis.io/commands/bf.insert/)

