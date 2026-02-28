# Iteration 52 Quick Reference

## Commands

| Command | Syntax | Return | Purpose |
|---------|--------|--------|---------|
| **HGETDEL** | `key FIELDS n field...` | Array | Atomic GET + DELETE |
| **HGETEX** | `key [EX\|PX\|EXAT\|PXAT\|PERSIST] FIELDS n field...` | Array | Atomic GET + EXPIRE |
| **HSETEX** | `key [FNX\|FXX] [EX\|PX\|EXAT\|PXAT\|KEEPTTL] FIELDS n field val...` | 1/0 | Atomic SET + EXPIRE |

## Storage API

```zig
// src/storage/memory.zig

pub fn hgetdel(self: *Storage, allocator, key, fields) ![]?[]const u8
// Returns array of values, deletes fields, removes key if empty

pub fn hgetex(self: *Storage, allocator, key, fields, expires_at_ms, persist) ![]?[]const u8
// Returns array of values, sets/removes expiration on fields

pub fn hsetex(self: *Storage, key, fields, values, expires_at_ms, options, keep_ttl) !bool
// Sets fields with expiration, returns true if set, false if FNX/FXX failed
// options: FNX=1, FXX=2
```

## Command Handlers

```zig
// src/commands/hashes.zig

pub fn cmdHgetdel(allocator, storage, args) ![]const u8
pub fn cmdHgetex(allocator, storage, args) ![]const u8
pub fn cmdHsetex(allocator, storage, args) ![]const u8
```

## Key Behaviors

### HGETDEL
- Returns nil for non-existent fields
- Auto-deletes key when all fields deleted
- Atomic operation (no race between read and delete)

### HGETEX
- No expiration option = acts like HMGET
- PERSIST removes TTL
- Expiration only set on existing fields
- EX/PX/EXAT/PXAT/PERSIST are mutually exclusive

### HSETEX
- FNX: all fields must NOT exist (returns 0 if any exists)
- FXX: all fields must exist (returns 0 if any missing)
- KEEPTTL: retains existing field TTL, no-op on new fields
- EX/PX/EXAT/PXAT/KEEPTTL are mutually exclusive
- FNX and FXX are mutually exclusive
- All-or-nothing semantics

## Error Messages

```
ERR wrong number of arguments for 'hgetdel' command
ERR wrong number of arguments for 'hgetex' command
ERR wrong number of arguments for 'hsetex' command
ERR syntax error
ERR numfields does not match number of fields
ERR numfields does not match number of field-value pairs
ERR value is not an integer or out of range
ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified
ERR FNX and FXX are mutually exclusive
ERR numfields must be greater than 0
ERR invalid expire time
WRONGTYPE Operation against a key holding the wrong kind of value
```

## Test Checklist

- [ ] HGETDEL basic operation
- [ ] HGETDEL key auto-deletion
- [ ] HGETDEL with nil fields
- [ ] HGETEX with EX/PX/EXAT/PXAT
- [ ] HGETEX with PERSIST
- [ ] HGETEX without expiration
- [ ] HGETEX mutual exclusivity error
- [ ] HSETEX basic with expiration
- [ ] HSETEX FNX success/failure
- [ ] HSETEX FXX success/failure
- [ ] HSETEX KEEPTTL preservation
- [ ] HSETEX mutual exclusivity errors
- [ ] All WRONGTYPE errors
- [ ] All argument validation errors
- [ ] Redis compatibility (byte-exact RESP)

## Implementation Order

1. Storage methods (4.5h)
2. Command handlers (6h)
3. Command routing (0.5h)
4. Unit tests (3h)
5. Integration tests (2h)
6. Compatibility validation (1.5h)
7. Performance validation (1h)
8. Documentation (0.5h)

**Total**: ~19 hours

## Quick Examples

```redis
# HGETDEL
> HSET h f1 v1 f2 v2
(integer) 2
> HGETDEL h FIELDS 2 f1 f3
1) "v1"
2) (nil)
> HGETALL h
1) "f2"
2) "v2"

# HGETEX
> HSET h f1 v1
(integer) 1
> HGETEX h EX 60 FIELDS 1 f1
1) "v1"
> HTTL h FIELDS 1 f1
1) (integer) 57

# HSETEX
> HSETEX h FNX EX 60 FIELDS 2 f1 v1 f2 v2
(integer) 1
> HSETEX h FNX EX 60 FIELDS 1 f1 v1
(integer) 0
> HSETEX h FXX KEEPTTL FIELDS 1 f1 new_v1
(integer) 1
```

## Completion Criteria

- ✅ All 3 commands implemented
- ✅ All storage methods working
- ✅ 15+ unit tests passing
- ✅ 10+ integration tests passing
- ✅ 100% Redis compatibility on examples
- ✅ >= 70% Redis performance
- ✅ 0 memory leaks
- ✅ Documentation updated
