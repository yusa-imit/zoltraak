# Iteration 52 Specification: Redis 8.0 Hash Field Operations

**Commands**: HGETDEL, HGETEX, HSETEX

**Redis Version**: 8.0.0+

**Status**: Specification Phase

---

## Overview

This iteration implements three Redis 8.0 hash commands that combine GET/SET operations with atomic field deletion and expiration management. These commands extend the hash field-level TTL infrastructure from Iteration 50 (HEXPIRE, HTTL, etc.).

### Command Summary

| Command | Purpose | Atomic Operation |
|---------|---------|------------------|
| **HGETDEL** | Get and delete hash fields | GET + DELETE |
| **HGETEX** | Get fields and set/update expiration | GET + EXPIRE |
| **HSETEX** | Set fields with expiration | SET + EXPIRE |

---

## 1. HGETDEL — Get and Delete Hash Fields

### Specification Reference
- **Official Docs**: [redis.io/commands/hgetdel](https://redis.io/docs/latest/commands/hgetdel/)
- **Available Since**: Redis 8.0.0
- **Time Complexity**: O(N) where N is the number of specified fields
- **ACL Categories**: `@write`, `@hash`, `@fast`

### Command Syntax

```
HGETDEL key FIELDS numfields field [field ...]
```

### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `key` | string | Yes | Hash key name |
| `FIELDS` | keyword | Yes | Literal keyword "FIELDS" |
| `numfields` | integer | Yes | Number of fields to retrieve and delete |
| `field` | string | Yes | Field name (repeatable, must match numfields) |

### Return Value

**RESP2/RESP3**: Array reply

- Returns array of values corresponding to requested fields in the same order
- Non-existent fields return `nil` (null bulk string)
- Example: `["value1", nil, "value3"]` for 3 fields where field2 doesn't exist

### Behavior

1. **Atomic Operation**: Field retrieval and deletion happen atomically
2. **Key Auto-Deletion**: When all fields are deleted, the hash key is automatically removed
3. **Non-Existent Fields**: Returns `nil` for missing fields, but doesn't fail
4. **Order Preservation**: Results match the order of requested fields
5. **Field-Level TTL**: Any field-level TTL is discarded when field is deleted

### Error Conditions

| Error | Condition | RESP Message |
|-------|-----------|--------------|
| `ERR wrong number of arguments` | args.len < 4 | "ERR wrong number of arguments for 'hgetdel' command" |
| `ERR syntax error` | Missing FIELDS keyword | "ERR syntax error" |
| `ERR numfields mismatch` | Actual fields != numfields | "ERR numfields does not match number of fields" |
| `ERR value is not an integer` | numfields not parseable | "ERR value is not an integer or out of range" |
| `WRONGTYPE` | Key exists but not a hash | "WRONGTYPE Operation against a key holding the wrong kind of value" |

### Edge Cases

| Case | Behavior |
|------|----------|
| Key doesn't exist | Return array of `nil` for all fields |
| Some fields don't exist | Return `nil` for missing, values for existing |
| All fields deleted | Hash key is automatically removed |
| Empty hash after partial delete | Key remains (only deleted if ALL fields gone) |
| Field with TTL deleted | TTL is discarded, field is removed |
| numfields = 0 | Error: "ERR numfields must be greater than 0" |

### Examples

```redis
redis> HSET mykey field1 "Hello" field2 "World" field3 "!"
(integer) 3

redis> HGETDEL mykey FIELDS 2 field3 field4
1) "!"
2) (nil)

redis> HGETALL mykey
1) "field1"
2) "Hello"
3) "field2"
4) "World"

redis> HGETDEL mykey FIELDS 2 field1 field2
1) "Hello"
2) "World"

redis> KEYS *
(empty array)
```

### RESP3 Considerations

- No special RESP3 behavior
- Returns array in both RESP2 and RESP3
- Null values are represented as RESP3 null type when protocol version is 3

---

## 2. HGETEX — Get Fields and Set Expiration

### Specification Reference
- **Official Docs**: [redis.io/commands/hgetex](https://redis.io/docs/latest/commands/hgetex/)
- **Available Since**: Redis 8.0.0
- **Time Complexity**: O(N) where N is the number of specified fields
- **ACL Categories**: `@write`, `@hash`, `@fast`

### Command Syntax

```
HGETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST] FIELDS numfields field [field ...]
```

### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `key` | string | Yes | Hash key name |
| `EX seconds` | integer | No* | Set expiration in seconds (relative) |
| `PX milliseconds` | integer | No* | Set expiration in milliseconds (relative) |
| `EXAT unix-time-seconds` | integer | No* | Set expiration at Unix timestamp (seconds) |
| `PXAT unix-time-milliseconds` | integer | No* | Set expiration at Unix timestamp (milliseconds) |
| `PERSIST` | keyword | No* | Remove TTL from fields |
| `FIELDS` | keyword | Yes | Literal keyword "FIELDS" |
| `numfields` | integer | Yes | Number of fields to retrieve |
| `field` | string | Yes | Field name (repeatable, must match numfields) |

**Note**: The expiration options (EX, PX, EXAT, PXAT, PERSIST) are **mutually exclusive**. Only one can be specified.

### Return Value

**RESP2/RESP3**: Array reply

- Returns array of values associated with given fields, in the same order
- Non-existent fields return `nil`
- Expiration is set/updated on existing fields, ignored for non-existent fields

### Behavior

1. **Atomic Operation**: Field retrieval and expiration setting happen atomically
2. **Expiration Side Effect**: Requested expiration is applied to all retrieved fields
3. **Non-Existent Fields**: Returns `nil`, no expiration set (field must exist first)
4. **PERSIST Option**: Removes any existing TTL from fields
5. **No Expiration Option**: If no expiration option given, just returns values (GET-only mode)
6. **Interaction with HEXPIRE**: Uses same underlying field-level TTL mechanism

### Error Conditions

| Error | Condition | RESP Message |
|-------|-----------|--------------|
| `ERR wrong number of arguments` | args.len < 4 | "ERR wrong number of arguments for 'hgetex' command" |
| `ERR syntax error` | Missing FIELDS keyword or multiple expiration options | "ERR syntax error" or "ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified" |
| `ERR numfields mismatch` | Actual fields != numfields | "ERR numfields does not match number of fields" |
| `ERR value is not an integer` | Invalid expiration or numfields | "ERR value is not an integer or out of range" |
| `WRONGTYPE` | Key exists but not a hash | "WRONGTYPE Operation against a key holding the wrong kind of value" |

### Edge Cases

| Case | Behavior |
|------|----------|
| Key doesn't exist | Return array of `nil`, no expiration set |
| Field doesn't exist | Return `nil` for that field, no expiration set |
| No expiration option | Acts like HMGET (just returns values) |
| PERSIST on field without TTL | Returns value, no-op on expiration |
| Negative expiration time | Error: "ERR invalid expire time" |
| Past timestamp (EXAT/PXAT) | Field expires immediately on next access |
| numfields = 0 | Error: "ERR numfields must be greater than 0" |

### Examples

```redis
redis> HSET mykey field1 "Hello" field2 "World"
(integer) 2

redis> HGETEX mykey EX 120 FIELDS 1 field1
1) "Hello"

redis> HTTL mykey FIELDS 1 field1
1) (integer) 115

redis> HGETEX mykey PERSIST FIELDS 1 field1
1) "Hello"

redis> HTTL mykey FIELDS 1 field1
1) (integer) -1

redis> HGETEX mykey EXAT 1740470400 FIELDS 2 field1 field2
1) "Hello"
2) "World"

redis> HGETEX mykey EX 60 PX 1000 FIELDS 1 field1
(error) ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified
```

### RESP3 Considerations

- No special RESP3 behavior
- Returns array in both RESP2 and RESP3

---

## 3. HSETEX — Set Fields with Expiration

### Specification Reference
- **Official Docs**: [redis.io/commands/hsetex](https://redis.io/docs/latest/commands/hsetex/)
- **Available Since**: Redis 8.0.0
- **Time Complexity**: O(N) where N is the number of fields being set
- **ACL Categories**: `@write`, `@hash`, `@fast`

### Command Syntax

```
HSETEX key [FNX | FXX] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL] FIELDS numfields field value [field value ...]
```

### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `key` | string | Yes | Hash key name |
| `FNX` | keyword | No** | Only set fields if **none** of them exist |
| `FXX` | keyword | No** | Only set fields if **all** of them exist |
| `EX seconds` | integer | No* | Set expiration in seconds (relative) |
| `PX milliseconds` | integer | No* | Set expiration in milliseconds (relative) |
| `EXAT unix-time-seconds` | integer | No* | Set expiration at Unix timestamp (seconds) |
| `PXAT unix-time-milliseconds` | integer | No* | Set expiration at Unix timestamp (milliseconds) |
| `KEEPTTL` | keyword | No* | Retain existing TTL on fields |
| `FIELDS` | keyword | Yes | Literal keyword "FIELDS" |
| `numfields` | integer | Yes | Number of field-value pairs |
| `field value` | string pairs | Yes | Field name and value (repeatable, must be numfields pairs) |

**Note**:
- Expiration options (EX, PX, EXAT, PXAT, KEEPTTL) are **mutually exclusive**
- FNX and FXX are **mutually exclusive**

### Return Value

**Integer reply**:
- `1` if all fields were set successfully
- `0` if no fields were set (FNX/FXX condition failed)

### Behavior

1. **Atomic Operation**: All field-value pairs are set atomically with expiration
2. **All-or-Nothing with Conditions**:
   - **FNX**: Returns 0 if ANY field already exists (sets nothing)
   - **FXX**: Returns 0 if ANY field doesn't exist (sets nothing)
   - **No flag**: Always sets all fields, returns 1
3. **Expiration Handling**:
   - Applies specified expiration to all fields being set
   - **KEEPTTL**: Retains existing field TTL (or no TTL if field was new)
   - Previous TTL is discarded unless KEEPTTL is used
4. **Hash Creation**: Creates hash key if it doesn't exist (unless FXX is used)

### Error Conditions

| Error | Condition | RESP Message |
|-------|-----------|--------------|
| `ERR wrong number of arguments` | args.len < 5 or field-value pairs incomplete | "ERR wrong number of arguments for 'hsetex' command" |
| `ERR syntax error` | Missing FIELDS, multiple exclusive options | "ERR syntax error" or "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified" or "ERR FNX and FXX are mutually exclusive" |
| `ERR numfields mismatch` | Actual pairs != numfields | "ERR numfields does not match number of field-value pairs" |
| `ERR value is not an integer` | Invalid expiration or numfields | "ERR value is not an integer or out of range" |
| `WRONGTYPE` | Key exists but not a hash | "WRONGTYPE Operation against a key holding the wrong kind of value" |

### Edge Cases

| Case | Behavior |
|------|----------|
| Key doesn't exist | Creates hash with all fields, returns 1 (unless FXX) |
| Key doesn't exist + FXX | Returns 0, nothing set |
| Some fields exist + FNX | Returns 0, nothing set |
| All fields exist + FXX | Sets all fields, returns 1 |
| KEEPTTL on new field | No TTL set (field didn't exist before) |
| KEEPTTL on existing field with TTL | Retains original TTL |
| KEEPTTL on existing field without TTL | No TTL (remains persistent) |
| No expiration option | Fields are set without TTL |
| Negative expiration | Error: "ERR invalid expire time" |
| Past timestamp (EXAT/PXAT) | Fields expire immediately on next access |
| numfields = 0 | Error: "ERR numfields must be greater than 0" |
| FNX + FXX together | Error: "ERR FNX and FXX are mutually exclusive" |

### Examples

```redis
redis> HSETEX mykey EXAT 1740470400 FIELDS 2 field1 "Hello" field2 "World"
(integer) 1

redis> HTTL mykey FIELDS 2 field1 field2
1) (integer) 55627
2) (integer) 55627

redis> HSETEX mykey FNX EX 60 FIELDS 2 field1 "Hello" field2 "World"
(integer) 0

redis> HSETEX mykey FXX KEEPTTL FIELDS 2 field1 "hello" field2 "world"
(integer) 1

redis> HTTL mykey FIELDS 2 field1 field2
1) (integer) 55481
2) (integer) 55481

redis> HSETEX mykey FXX EX 60 KEEPTTL FIELDS 1 field1 "test"
(error) ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified
```

### RESP3 Considerations

- No special RESP3 behavior
- Returns integer in both RESP2 and RESP3

---

## Implementation Requirements

### Storage Layer (`src/storage/memory.zig`)

#### New Methods Required

```zig
/// Get and delete hash fields atomically
/// Returns array of values (caller must free), nil for non-existent fields
/// Removes key if all fields deleted
pub fn hgetdel(
    self: *Storage,
    allocator: std.mem.Allocator,
    key: []const u8,
    fields: []const []const u8,
) error{ WrongType, OutOfMemory }![]?[]const u8

/// Get hash field values and optionally set/update expiration
/// Returns array of values (caller must free), nil for non-existent fields
/// If expires_at_ms is null, just returns values (no expiration change)
/// If persist is true, removes TTL from fields
pub fn hgetex(
    self: *Storage,
    allocator: std.mem.Allocator,
    key: []const u8,
    fields: []const []const u8,
    expires_at_ms: ?i64,
    persist: bool,
) error{ WrongType, OutOfMemory }![]?[]const u8

/// Set hash fields with expiration and conditional flags
/// Returns true if all fields set, false if FNX/FXX condition failed
/// options: bit flags (FNX=1, FXX=2)
/// If expires_at_ms is null and !keep_ttl, no expiration set
/// If keep_ttl is true, retains existing field TTL
pub fn hsetex(
    self: *Storage,
    key: []const u8,
    fields: []const []const u8,
    values: []const []const u8,
    expires_at_ms: ?i64,
    options: u8,
    keep_ttl: bool,
) error{ WrongType, OutOfMemory }!bool
```

#### Integration with Existing Infrastructure

- **FieldValue**: Already has `expires_at: ?i64` for per-field TTL (Iteration 50)
- **HashValue**: Has `data: StringHashMap(FieldValue)` for field storage
- **Field expiration cleanup**: Leverage existing `isExpired()` checks before access
- **Key auto-deletion**: Use existing pattern from `hdel()` — remove key if `data.count() == 0`

### Command Layer (`src/commands/hashes.zig`)

#### New Command Handlers

```zig
pub fn cmdHgetdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8
pub fn cmdHgetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8
pub fn cmdHsetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8
```

#### Argument Parsing Patterns

**HGETDEL**:
1. Validate args.len >= 4
2. Extract key (args[1])
3. Validate FIELDS keyword (args[2])
4. Parse numfields (args[3])
5. Extract fields (args[4..4+numfields])
6. Validate args.len == 4 + numfields

**HGETEX**:
1. Validate args.len >= 4
2. Extract key (args[1])
3. Parse optional expiration option (EX/PX/EXAT/PXAT/PERSIST) — position varies
4. Find FIELDS keyword position
5. Parse numfields after FIELDS
6. Extract fields
7. Compute expires_at_ms based on option
8. Validate mutual exclusivity of expiration options

**HSETEX**:
1. Validate args.len >= 5
2. Extract key (args[1])
3. Parse optional FNX/FXX flag
4. Parse optional expiration option (EX/PX/EXAT/PXAT/KEEPTTL)
5. Find FIELDS keyword position
6. Parse numfields after FIELDS
7. Extract field-value pairs (numfields * 2 args)
8. Validate args.len matches expected
9. Validate mutual exclusivity (FNX/FXX, expiration options)

### Command Routing (`src/server.zig`)

Add command dispatch entries:

```zig
"hgetdel" => try hashes_mod.cmdHgetdel(arena, storage, args),
"hgetex" => try hashes_mod.cmdHgetex(arena, storage, args),
"hsetex" => try hashes_mod.cmdHsetex(arena, storage, args),
```

### Protocol Considerations

- All commands return arrays or integers — no special RESP3 native types
- Use existing `Writer.writeArray()` for field values
- Use existing `Writer.writeInteger()` for HSETEX result
- Null values in arrays: `Writer.writeNull()` in RESP2, RESP3 null type in RESP3

---

## Testing Strategy

### Unit Tests (in command handler files)

**HGETDEL**:
- Basic get-and-delete
- Non-existent fields return nil
- Key auto-deletion when all fields deleted
- WRONGTYPE error
- Argument validation errors

**HGETEX**:
- Get with EX expiration
- Get with PX, EXAT, PXAT options
- PERSIST removes TTL
- No expiration option (GET-only)
- Non-existent fields return nil
- Mutual exclusivity errors
- WRONGTYPE error

**HSETEX**:
- Basic set with expiration
- FNX succeeds on new fields
- FNX fails when any field exists
- FXX succeeds when all fields exist
- FXX fails when any field missing
- KEEPTTL retains existing TTL
- Mutual exclusivity errors (FNX+FXX, EX+PX+KEEPTTL)
- WRONGTYPE error

### Integration Tests

**Test File**: `tests/test_hash_field_operations.zig`

```zig
test "HGETDEL - atomic get and delete" {
    // Set fields, HGETDEL some, verify deleted and returned
}

test "HGETDEL - key auto-deletion" {
    // Delete all fields, verify key removed
}

test "HGETEX - set expiration on existing fields" {
    // HGETEX with EX, verify TTL with HTTL
}

test "HGETEX - PERSIST removes TTL" {
    // Set TTL with HEXPIRE, HGETEX PERSIST, verify TTL removed
}

test "HSETEX - conditional set with FNX" {
    // FNX on non-existent fields succeeds, FNX on existing fails
}

test "HSETEX - conditional set with FXX" {
    // FXX on existing fields succeeds, FXX with missing field fails
}

test "HSETEX - KEEPTTL retains expiration" {
    // Set field with TTL, HSETEX KEEPTTL, verify TTL unchanged
}

test "HGETEX + HSETEX interaction" {
    // HSETEX with expiration, HGETEX to verify, HGETEX PERSIST
}
```

### Redis Compatibility Validation

**Differential Testing**:
1. Start real Redis 8.0+ instance on port 6380
2. Run identical command sequences against Zoltraak (6379) and Redis (6380)
3. Compare RESP responses byte-by-byte
4. Verify TTL values within reasonable tolerance (±100ms)

**Test Cases**:
- All examples from specification
- Edge cases (empty hash, non-existent key, WRONGTYPE)
- Expiration edge cases (past timestamps, negative values)
- Conditional failures (FNX, FXX)
- KEEPTTL with and without existing TTL

---

## Documentation Updates

### README.md

Add to Iteration 51+ table:

```markdown
| 52 | Hash field operations (Redis 8.0) — HGETDEL (atomic get+delete), HGETEX (get+expire), HSETEX (set+expire with FNX/FXX/KEEPTTL) |
```

### CLAUDE.md

Update iteration count and add to Phase 2 section:

```markdown
- 52: Redis 8.0 hash field operations (HGETDEL, HGETEX, HSETEX) — atomic get-delete, get-expire, set-expire with conditionals
```

---

## Known Limitations

None. Full compatibility with Redis 8.0 HGETDEL, HGETEX, HSETEX behavior expected.

---

## Related Features

- **Iteration 50**: Hash field-level TTL (HEXPIRE, HPEXPIRE, HTTL, HPTTL, HEXPIREAT, HPEXPIREAT, HPERSIST, HEXPIRETIME, HPEXPIRETIME)
- **Existing Hash Commands**: HSET, HGET, HDEL, HMGET, HGETALL, HEXISTS
- **Atomic Operations**: GETDEL, GETEX (string equivalents)

---

## Sources

- [HGETDEL Command Documentation](https://redis.io/docs/latest/commands/hgetdel/)
- [HGETEX Command Documentation](https://redis.io/docs/latest/commands/hgetex/)
- [HSETEX Command Documentation](https://redis.io/docs/latest/commands/hsetex/)
- [Redis 8.0 Commands Reference](https://redis.io/docs/latest/commands/redis-8-0-commands/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
