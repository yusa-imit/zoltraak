# Bloom Filter Commands — Complete Comparison

## Command Overview

Redis Bloom Filter module provides 6 primary commands (5 implemented, 1 planned):

| Command | Implemented | Items | Creation | Return | Use Case |
|---------|-------------|-------|----------|--------|----------|
| BF.RESERVE | ✅ Iteration 210 | N/A | Manual | OK | Pre-create filter with exact parameters |
| BF.ADD | ✅ Iteration 210 | 1 | Auto (defaults) | 1/0 | Add single item with defaults |
| BF.EXISTS | ✅ Iteration 210 | 1 | N/A | 1/0 | Check single item |
| BF.MADD | ✅ Iteration 211 | N | Auto (defaults) | [1/0...] | Batch add with defaults |
| BF.MEXISTS | ✅ Iteration 211 | N | N/A | [1/0...] | Batch check items |
| BF.INSERT | 🚧 Iteration 212 | N | Auto (custom) | [1/0...] | **Batch add with full control** |

---

## Side-by-Side Comparison

### 1. BF.RESERVE — Manual Creation

```
BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
```

**Purpose**: Pre-create a Bloom filter with exact parameters
**When to use**: When you know exact requirements upfront

| Aspect | Behavior |
|--------|----------|
| Items per call | N/A (creates, doesn't insert) |
| Auto-creates filter | No (must call explicitly) |
| Customization | Full (capacity, error, expansion, nonscaling) |
| Return | Simple string (OK or error) |
| Typical usage | BF.RESERVE → BF.ADD/MADD → BF.EXISTS |

**Example**:
```redis
> BF.RESERVE myfilter 0.001 10000
OK
> BF.ADD myfilter item1
1
```

---

### 2. BF.ADD — Single Item with Defaults

```
BF.ADD key item
```

**Purpose**: Add one item with automatic filter creation using defaults
**When to use**: Simple single-item operations

| Aspect | Behavior |
|--------|----------|
| Items per call | 1 |
| Auto-creates filter | Yes (defaults: 0.01 error, 100 capacity) |
| Customization | None (uses hardcoded defaults) |
| Return | Integer: 1 (new) or 0 (duplicate) |
| RESP3 | Integer (not boolean) |

**Example**:
```redis
> BF.ADD users:ids alice
1           # alice is new
> BF.ADD users:ids alice
0           # alice was already there
```

---

### 3. BF.EXISTS — Single Item Lookup

```
BF.EXISTS key item
```

**Purpose**: Check if an item is in the filter
**When to use**: Membership queries

| Aspect | Behavior |
|--------|----------|
| Items per call | 1 |
| Auto-creates filter | No (returns 0 if key doesn't exist) |
| Return | Integer: 1 (probably in) or 0 (definitely not in) |
| RESP3 | Integer (not boolean) |

**Example**:
```redis
> BF.EXISTS users:ids alice
1           # alice is in (or false positive)
> BF.EXISTS users:ids bob
0           # bob is definitely not in
```

---

### 4. BF.MADD — Batch Items with Defaults

```
BF.MADD key item [item ...]
```

**Purpose**: Add multiple items with automatic filter creation using defaults
**When to use**: Bulk operations with default parameters

| Aspect | Behavior |
|--------|----------|
| Items per call | N (1 or more) |
| Auto-creates filter | Yes (defaults: 0.01 error, 100 capacity) |
| Customization | None (uses hardcoded defaults) |
| Return | Array of integers: [1/0, 1/0, ...] |
| RESP3 | Array of integers (not booleans) |

**Example**:
```redis
> BF.MADD users:ids alice bob charlie
[1, 1, 1]     # All new
> BF.MADD users:ids alice bob dave
[0, 0, 1]     # alice & bob existed, dave is new
```

---

### 5. BF.MEXISTS — Batch Lookup

```
BF.MEXISTS key item [item ...]
```

**Purpose**: Check multiple items in one call
**When to use**: Batch membership queries

| Aspect | Behavior |
|--------|----------|
| Items per call | N (1 or more) |
| Auto-creates filter | No (returns [0,0,...] if key doesn't exist) |
| Return | Array of integers: [1/0, 1/0, ...] |
| RESP3 | Array of integers (not booleans) |

**Example**:
```redis
> BF.MEXISTS users:ids alice bob eve
[1, 1, 0]     # alice & bob in, eve not in
```

---

### 6. BF.INSERT — Batch Items with Full Control (Planned)

```
BF.INSERT key [CAPACITY n] [ERROR x] [EXPANSION e] [NOCREATE] [NONSCALING] ITEMS item [item ...]
```

**Purpose**: Add multiple items with full parameter control for automatic creation
**When to use**: Batch operations needing custom parameters or conditional creation

| Aspect | Behavior |
|--------|----------|
| Items per call | N (1 or more) |
| Auto-creates filter | Yes, but **customizable** (CAPACITY, ERROR, EXPANSION) |
| Creation parameters | CAPACITY (default 100), ERROR (default 0.01), EXPANSION (default 2) |
| NOCREATE | Only insert if exists; error if not |
| NONSCALING | Prevent sub-filter creation; error on overflow |
| Return | Array of integers: [1/0, 1/0, ...] or [1, "ERR filter is full", ...] |
| RESP3 | Array of **booleans**: [true/false, true/false, ...] (different from MADD!) |

**Examples**:

Auto-create with custom capacity:
```redis
> BF.INSERT myfilter CAPACITY 50000 ERROR 0.001 ITEMS a b c
[1, 1, 1]     # All inserted, filter created with cap 50k, error 0.1%
```

Insert only if filter exists:
```redis
> BF.INSERT myfilter NOCREATE ITEMS a b
[0, 1]        # Inserted into existing filter
> BF.INSERT newfilter NOCREATE ITEMS a
ERR no such key   # Error, didn't create
```

Non-scaling filter (error on overflow):
```redis
> BF.INSERT staticfilter CAPACITY 3 NONSCALING ITEMS a b c d e
[1, 1, 1, "ERR filter is full", "ERR filter is full"]
# a,b,c inserted; d,e rejected (partial success!)
```

---

## Operational Workflow Comparison

### Scenario 1: "I just want to track user IDs"

**With BF.ADD** (simplest):
```redis
BF.ADD active_users alice
BF.ADD active_users bob
BF.EXISTS active_users alice      → 1
```

**With BF.INSERT** (batch):
```redis
BF.INSERT active_users ITEMS alice bob charlie
BF.EXISTS active_users alice      → 1
```

### Scenario 2: "I know I'll have 1M items and need low error"

**With BF.RESERVE + MADD** (explicit):
```redis
BF.RESERVE users 0.001 1000000
BF.MADD users item1 item2 item3 ... (batch)
```

**With BF.INSERT** (all-in-one, Iteration 212):
```redis
BF.INSERT users CAPACITY 1000000 ERROR 0.001 ITEMS item1 item2 item3 ...
```

### Scenario 3: "Conditional insert only if filter exists"

**With BF.MADD** (not possible):
```redis
# Can't prevent creation; must check manually
EXISTS myfilter           → 0/1
BF.MADD myfilter item1    # Creates if doesn't exist (unwanted)
```

**With BF.INSERT + NOCREATE** (Iteration 212):
```redis
BF.INSERT myfilter NOCREATE ITEMS item1  # Error if doesn't exist (desired)
```

### Scenario 4: "Load fixed-size filter, reject overflow"

**With BF.RESERVE + NONSCALING + MADD** (multi-step):
```redis
BF.RESERVE cache 0.01 1000 NONSCALING
BF.MADD cache item1 item2 item3    # Can't handle overflow per-item
```

**With BF.INSERT + NONSCALING** (Iteration 212):
```redis
BF.INSERT cache CAPACITY 1000 NONSCALING ITEMS item1 item2 item3
# Returns [1, 1, "ERR filter is full"] — partial success visible
```

---

## Decision Matrix: Which Command to Use?

```
┌─────────────────────────────────────────────────────────────┐
│ Need MULTIPLE items?                                       │
└─────────────────────────────────────────────────────────────┘
                    ↓
        ┌─────────────────────────┐
        │   YES → MADD or INSERT  │
        │   NO  → ADD             │
        └─────────────────────────┘
                    ↓
        ┌────────────────────────────────────────┐
        │ Need custom creation parameters?       │
        │ (CAPACITY, ERROR, EXPANSION, NOCREATE)│
        └────────────────────────────────────────┘
                    ↓
        ┌──────────────────────────┐
        │ YES → INSERT (Iter 212)  │
        │ NO  → MADD (Iter 211)    │
        └──────────────────────────┘
                    ↓
        ┌──────────────────────────────────┐
        │ For lookup, always use EXISTS    │
        │ or MEXISTS (not insertion cmds)  │
        └──────────────────────────────────┘
```

---

## Feature Matrix

| Feature | RESERVE | ADD | MADD | INSERT | EXISTS | MEXISTS |
|---------|---------|-----|------|--------|--------|---------|
| Single item | N/A | ✅ | ✅ | ✅ | ✅ | N/A |
| Multiple items | N/A | ❌ | ✅ | ✅ | ❌ | ✅ |
| Manual creation | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Auto-create | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ |
| Custom CAPACITY | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Custom ERROR | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Custom EXPANSION | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| NOCREATE | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| NONSCALING | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Returns [1/0] | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Return Value Examples

### BF.ADD / BF.EXISTS

```redis
> BF.ADD myfilter item1
1                           # New item
> BF.ADD myfilter item1
0                           # Duplicate
```

### BF.MADD / BF.MEXISTS

```redis
> BF.MADD myfilter item1 item2 item3
[1, 1, 1]                   # All new
> BF.MADD myfilter item1 item2 item4
[0, 0, 1]                   # Two existing, one new
```

### BF.INSERT (Iteration 212)

```redis
> BF.INSERT myfilter ITEMS item1 item2 item3
[1, 1, 1]                   # All new
> BF.INSERT myfilter ITEMS item1 item2 item4
[0, 0, 1]                   # Mixed
> BF.INSERT myfilter NONSCALING ITEMS ...
[1, 1, "ERR filter is full"]  # Partial success with overflow
```

---

## Memory Efficiency

All Bloom filters use the same underlying storage engine with:
- Optimal bit calculation based on error rate and capacity
- Double hashing (MurmurHash3 → two 64-bit values)
- Sub-filter scaling (default) or fixed size (NONSCALING)

**Memory for 1M items**:
| Error Rate | Hash Funcs | Bits/Item | Total Memory |
|------------|------------|-----------|--------------|
| 1% (0.01) | 7 | 9.6 | ~1.2 MB |
| 0.1% (0.001) | 10 | 14.4 | ~1.8 MB |
| 0.01% (0.0001) | 14 | 19.2 | ~2.4 MB |

BF.INSERT allows you to set ERROR exactly, vs BF.ADD/MADD which hardcode 0.01.

---

## Implementation Status

| Command | Iteration | Status | Estimated LOC | Complexity |
|---------|-----------|--------|----------------|------------|
| BF.RESERVE | 210 | ✅ | 120 | Medium |
| BF.ADD | 210 | ✅ | 80 | Small |
| BF.EXISTS | 210 | ✅ | 70 | Small |
| BF.MADD | 211 | ✅ | 100 | Medium |
| BF.MEXISTS | 211 | ✅ | 90 | Medium |
| **BF.INSERT** | **212** | 🚧 **Planned** | **250** | **Medium** |

---

## Redis Specification References

- [BF.RESERVE](https://redis.io/commands/bf.reserve/) — Manual creation
- [BF.ADD](https://redis.io/commands/bf.add/) — Single item
- [BF.MADD](https://redis.io/commands/bf.madd/) — Batch with defaults
- [BF.INSERT](https://redis.io/commands/bf.insert/) — **Batch with custom params (Iteration 212)**
- [BF.EXISTS](https://redis.io/commands/bf.exists/) — Check single
- [BF.MEXISTS](https://redis.io/commands/bf.mexists/) — Check batch
- [Bloom Filter Guide](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/)

---

## Next Steps

1. **Iteration 212** (BF.INSERT): Implement batch insertion with full parameter control
2. **Future iterations**: BF.INFO, BF.CARD, BF.SCAN (Phase 15 remaining commands)

