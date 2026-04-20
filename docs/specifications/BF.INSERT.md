# BF.INSERT Command Specification — Iteration 212

## Executive Summary

**BF.INSERT** combines the capabilities of `BF.RESERVE` (filter creation) and `BF.MADD` (batch insertion) into a single command. It is the primary user-facing command for adding items to Bloom filters because it handles automatic filter creation while supporting full parameter customization.

---

## Command Syntax

```
BF.INSERT key [CAPACITY capacity] [ERROR error_rate]
  [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
```

### Required Arguments

| Argument | Type | Description |
|----------|------|-------------|
| `key` | string | Key name for the Bloom filter |
| `ITEMS` | varargs | One or more items to insert (binary-safe strings) |

### Optional Arguments — Creation Parameters

| Argument | Type | Default | Constraint | Behavior |
|----------|------|---------|-----------|----------|
| `CAPACITY` | integer | 100 (auto-create) | > 0 | Initial capacity for newly created filter. **Ignored if filter exists.** Cannot be used with `NOCREATE`. |
| `ERROR` | float | 0.01 (1%) | (0.0, 1.0) | Target error rate for newly created filter. **Ignored if filter exists.** Cannot be used with `NOCREATE`. |
| `EXPANSION` | integer | 2 | > 0 | Multiplier for sub-filter size when auto-scaling. **Ignored if filter exists.** |
| `NONSCALING` | flag | false | N/A | Prevents automatic sub-filter creation. **Ignored if filter exists.** Error when capacity exceeded. |

### Optional Arguments — Behavior Control

| Argument | Type | Mutual Exclusions | Behavior |
|----------|------|-------------------|----------|
| `NOCREATE` | flag | CAPACITY, ERROR | Only insert into existing filter. Return error if key doesn't exist. |

---

## Return Value

### Return Type

**RESP2**: Array of integers
- `1` — Item successfully added (was definitely new)
- `0` — Item probably already in filter (false positive possible)
- Error string — Filter is full (NONSCALING only)

**RESP3**: Array of booleans (when protocol version = 3)
- `true` — Item successfully added
- `false` — Item probably already in filter
- Error string — Filter is full (NONSCALING only)

### Return Value Examples

**3 new items, all added successfully:**
```
> BF.INSERT myfilter ITEMS foo bar baz
[1, 1, 1]
```

**Mixed: 1 new, 1 duplicate, 1 new:**
```
> BF.INSERT myfilter ITEMS qux bar quux
[1, 0, 1]
```

**NOCREATE fails on nonexistent filter:**
```
> BF.INSERT nonexistent NOCREATE ITEMS x y
ERR no such key
```

**NONSCALING filter full (capacity exceeded):**
```
> BF.INSERT staticfilter ITEMS item1 item2 item3 ...
[1, 1, "ERR filter is full"]  // Return stops at capacity
```

---

## Behavior Specification

### Filter Creation Logic

#### Case 1: Filter Does Not Exist

**Default behavior (no NOCREATE):**
1. Create new Bloom filter with parameters:
   - Capacity: `CAPACITY` arg if provided, else `100`
   - Error rate: `ERROR` arg if provided, else `0.01` (1%)
   - Expansion: `EXPANSION` arg if provided, else `2`
   - Scaling: Enabled by default, disabled if `NONSCALING` flag set
2. Insert all items
3. Return array of add results

**With NOCREATE flag:**
- Return error: `ERR no such key`
- No items are inserted
- No filter is created

#### Case 2: Filter Exists

**All creation parameters are ignored:**
- `CAPACITY`, `ERROR`, `EXPANSION`, `NONSCALING` — silently ignored
- Filter uses its existing configuration
- Insert items using existing filter's parameters
- Return array of add results

#### Case 3: Wrong Type

**If key exists but is not a Bloom filter:**
- Return error: `WRONGTYPE Operation against a key holding the wrong kind of value`
- No items are inserted

---

### Capacity Overflow Behavior

#### Scaling Filters (Default)

When a sub-filter reaches `capacity * expansion`:
1. **New sub-filter is created** with size calculated from:
   - New capacity = current filter capacity
   - Same error rate as original
   - Same expansion factor
2. **Insertion continues seamlessly**:
   - All subsequent items added to latest sub-filter
   - Both insertion and lookup check all filters
3. **Performance impact**:
   - Insertion: O(k) per item (only latest filter updated)
   - Lookup (BF.EXISTS): O(k * num_filters) (all filters checked)

#### Non-Scaling Filters (NONSCALING flag)

When capacity is reached:
1. **No new sub-filter is created**
2. **Error is returned** for items that cause overflow:
   - Return value includes error string in result array at that position
   - Items inserted before overflow are still committed
3. **Partial success**:
   - Command processes items in order
   - Returns results array with mix of 1/0/error strings
   - **Important**: This is NOT a transaction; partial results are committed

Example NONSCALING behavior:
```
> BF.INSERT staticfilter NONSCALING ITEMS a b c d e ...
# If capacity reached at item 3:
[1, 1, "ERR filter is full", "ERR filter is full", ...]
# Items a, b are in filter; c, d, etc. are not inserted
```

---

### Parameter Validation

#### Capacity
- Must be > 0
- Error if: `capacity <= 0`
- Return: `ERR capacity must be greater than 0`

#### Error Rate
- Must be in range (0.0, 1.0) — exclusive on both ends
- Error if: `error_rate <= 0 or error_rate >= 1`
- Return: `ERR error rate must be between 0 and 1`

#### Expansion
- Must be > 0
- Recommended: >= 2 for auto-scaling predictability
- Error if: `expansion <= 0`
- Return: `ERR expansion must be greater than 0`

#### NOCREATE with CAPACITY/ERROR
- Error if NOCREATE is used with CAPACITY or ERROR
- Return: `ERR NOCREATE cannot be used with CAPACITY or ERROR`

#### Items
- All items are binary-safe
- Empty items allowed (though unusual)
- Minimum 1 item required: `ITEMS must be followed by at least one item`

---

### Memory and Performance

#### Optimal Parameters Calculation

For a given capacity and error rate:
- `k = ceil(-log₂(error_rate))` hash functions
- `m = ceil(-capacity * ln(error_rate) / ln²(2))` bits needed

**Examples:**
| Error Rate | Hash Functions | Bits/Item | For 1M items |
|------------|-----------------|-----------|-------------|
| 1% | 7 | 9.6 | ~1.2 MB |
| 0.1% | 10 | 14.4 | ~1.8 MB |
| 0.01% | 14 | 19.2 | ~2.4 MB |

#### Nonscaling Hash Reduction

When `NONSCALING` is used:
- `k_nonscaling = k - 1` (one fewer hash function)
- Reduces memory by ~5-10%
- Allows error rate to increase gracefully when capacity exceeded
- Use only when you're confident capacity won't be exceeded

---

## Edge Cases and Special Behaviors

### 1. Empty ITEMS List
- **Behavior**: Error — at least one item required
- **Return**: `ERR wrong number of arguments for 'bf.insert' command`

### 2. Duplicate Items in Same Call
```
> BF.INSERT filter ITEMS x x y
# Both x's treated independently; second x likely returns 0
[1, 0, 1]
```

### 3. Very High Capacity
- Bloom filters pre-allocate memory based on capacity
- Very high capacity (millions) may cause OOM
- Recommendation: Use EXPANSION with moderate capacity for dynamic growth

### 4. Filter with 0 Capacity

If `BF.RESERVE key 0.01 0` was run, filter cannot be created:
- `BF.RESERVE` rejects it with error
- `BF.INSERT` with CAPACITY 0 also rejects

### 5. Expired Keys

If key exists but TTL has expired:
- **Behavior**: Treated as nonexistent key
- If `NOCREATE`: Return `ERR no such key`
- If default: Create new filter with provided parameters

### 6. Concurrent Operations

- All operations are atomic (protected by mutex)
- NONSCALING partial success is committed atomically per item
- No isolation between items (not ACID transaction)

---

## Comparison: BF.ADD vs BF.MADD vs BF.INSERT

| Feature | BF.ADD | BF.MADD | BF.INSERT |
|---------|--------|---------|-----------|
| Items | Single | Multiple | Multiple |
| Auto-create | Yes (defaults) | Yes (defaults) | Yes (configurable) |
| Custom capacity | No | No | **Yes** |
| Custom error rate | No | No | **Yes** |
| Custom expansion | No | No | **Yes** |
| NOCREATE support | No | No | **Yes** |
| Return type | Integer | Array of integers | Array of integers |
| Use case | Simple single items | Batch add | Full control, bulk operations |

---

## Implementation Notes for Zoltraak

### Storage Layer (bloom.zig)

The `BloomFilterValue` struct already supports all needed parameters:

```zig
pub const BloomFilterValue = struct {
    error_rate: f64,      // ✅ Already tracked
    capacity: u64,        // ✅ Already tracked
    expansion: u16,       // ✅ Already tracked
    nonscaling: bool,     // ✅ Already tracked
    num_hashes: u8,
    filters: std.ArrayList(SubFilter),
    total_items_added: u64,
    allocator: std.mem.Allocator,
    expires_at: ?i64,
};
```

The `add()` method already:
- Returns 1 for new items, 0 for duplicates
- Handles scaling automatically
- Respects nonscaling flag
- Returns error when NONSCALING filter is full

### Command Layer (bloom.zig)

Implement `cmdBfInsert()` with:

1. **Argument parsing** (in order):
   - Parse key
   - Parse optional CAPACITY, ERROR, EXPANSION, NOCREATE flags
   - Validate mutual exclusions (NOCREATE + CAPACITY/ERROR)
   - Parse ITEMS... (all remaining args)

2. **Filter creation logic**:
   - If key exists: ignore creation params
   - If key doesn't exist + NOCREATE: return error
   - If key doesn't exist + no NOCREATE: create with params

3. **Batch insertion**:
   - Allocate result array for N items
   - Loop through items, call `filter.add(item)`
   - Collect results
   - Return array

4. **Error handling**:
   - WRONGTYPE check
   - Validation errors (capacity <= 0, error_rate invalid)
   - NOCREATE logic

### Command Registration

In `src/server.zig`, register in command dispatcher:
```zig
"bf.insert" => try cmdBfInsert(allocator, storage, args),
```

---

## Test Coverage Plan

### Unit Tests (bloom.zig)

- [x] MurmurHash3 determinism
- [x] Bit operations
- [x] Parameter validation (error_rate, capacity)
- [x] Single item add
- [x] Duplicate detection
- [x] Scaling behavior
- [x] Nonscaling behavior

### Command Tests (Required for BF.INSERT)

1. **Creation scenarios**:
   - Auto-create with defaults
   - Custom CAPACITY
   - Custom ERROR rate
   - Custom EXPANSION
   - NONSCALING flag
   - Combinations (e.g., CAPACITY + EXPANSION + NONSCALING)

2. **NOCREATE behavior**:
   - Existing filter + NOCREATE → insert works
   - Nonexistent filter + NOCREATE → error
   - NOCREATE + CAPACITY → error
   - NOCREATE + ERROR → error

3. **Existing filter logic**:
   - Creation params ignored when filter exists
   - Existing config preserved

4. **Return values**:
   - New items return 1
   - Duplicates return 0
   - Mixed array of 1s and 0s
   - NONSCALING overflow returns error string

5. **Edge cases**:
   - Empty ITEMS (error)
   - Duplicate items in same call
   - Wrong type (WRONGTYPE error)
   - Very large item count
   - Binary-safe items (null bytes, special chars)

6. **RESP3 support**:
   - RESP3 returns booleans (true/false) instead of 1/0
   - Error strings unchanged

### Integration Tests

- Full request-response cycle via RESP parser
- redis-cli compatibility testing
- Compatibility with redis-benchmark

---

## Redis Specification References

- [BF.INSERT | redis.io/commands](https://redis.io/commands/bf.insert/)
- [Bloom Filter Documentation | redis.io/docs](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/)
- [RedisBloom GitHub](https://github.com/RedisBloom/RedisBloom)

---

## Version Compatibility

- **Redis**: 4.0+ (Bloom module available)
- **RedisBloom**: All versions supporting BF.INSERT
- **Valkey**: Supported (compatible command)

---

## Differences from Other Commands

### vs BF.RESERVE + BF.MADD
- BF.INSERT is more convenient (single command)
- BF.RESERVE requires explicit creation upfront
- BF.INSERT allows creation + insertion in one call

### vs BF.ADD
- BF.ADD: single item only
- BF.INSERT: batch items, full parameter control

### vs SET vs BITFIELD
- BF.INSERT: probabilistic, space-efficient, false positives possible
- SET: deterministic, no false positives, more memory
- BITFIELD: bit-level manipulation, not membership testing

---

## Protocol Compliance

### RESP2 Compliance
- Array response with integer elements
- Error strings for failures
- Simple string "OK" for creation (not returned, just array)

### RESP3 Compliance
- Array response with boolean elements (true/false)
- Error strings unchanged
- Map format not used (array of integers/booleans)

---

## Known Limitations / Future Enhancements

1. **Stub Implementation**: Memory usage, execution stats not tracked
2. **Future: Scaling optimization**: Implement sub-filter redistribution for better distribution
3. **Future: Info commands**: BF.INFO with per-filter statistics
4. **Future: Lua integration**: Support in EVAL scripts

---

## Command Categorization

- **ACL Categories**: @bloom, @write, @slow
- **Complexity**: O(k * n) where k = hash functions, n = items
- **Key space**: Writes to key
- **Transactional**: Non-atomic per-item (NONSCALING can return partial results)
- **Replication**: Full support (command sent to replicas)
- **Cluster**: Support planned (requires slot-aware routing)
