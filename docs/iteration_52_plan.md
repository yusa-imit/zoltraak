# Zoltraak Iteration Plan: Redis 8.0 Hash Field Operations (Iteration 52)

## Current State Summary

Zoltraak has completed 51 iterations implementing 150+ Redis commands. Iteration 50 introduced **hash field-level TTL** infrastructure:

- **Storage**: `HashValue.data` maps field names to `FieldValue` structs with `data` + `expires_at: ?i64`
- **Commands**: HEXPIRE, HPEXPIRE, HTTL, HPTTL, HEXPIREAT, HPEXPIREAT, HPERSIST, HEXPIRETIME, HPEXPIRETIME
- **Storage API**: `hexpire()`, `hpersist()`, `httl()`, `hpttl()`, `hexpiretime()`, `hpexpiretime()`

### Existing Hash Commands
- Basic: HSET, HGET, HDEL, HMGET, HGETALL, HEXISTS, HLEN
- Advanced: HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD
- Field TTL: Full expiration management suite (Iteration 50)
- Protocol-aware: HGETALL returns RESP3 map (%), HKEYS returns RESP3 set (~)

### What's Missing

Redis 8.0 introduced **atomic hash field operations** that combine GET/SET with DELETE/EXPIRE in a single command:

1. **HGETDEL** — Atomically GET field values and DELETE fields (like GETDEL for strings)
2. **HGETEX** — Atomically GET field values and SET/UPDATE expiration (like GETEX for strings)
3. **HSETEX** — Atomically SET field values with expiration and conditionals (FNX/FXX/KEEPTTL)

These commands are critical for Redis 8.0 compatibility and enable race-free hash field management patterns.

---

## Specification Reference

**Official Redis Documentation**:
- [HGETDEL](https://redis.io/docs/latest/commands/hgetdel/)
- [HGETEX](https://redis.io/docs/latest/commands/hgetex/)
- [HSETEX](https://redis.io/docs/latest/commands/hsetex/)
- [Redis 8.0 Commands Reference](https://redis.io/docs/latest/commands/redis-8-0-commands/)

**Detailed Specification**: `/Users/fn/Desktop/codespace/zoltraak/docs/iteration_52_spec.md`

---

## Gap Analysis

### High Priority (Core Functionality)

#### 1. HGETDEL Implementation (Medium Complexity)
**Status**: Missing
**Impact**: HIGH — Core atomic operation, common pattern in distributed systems
**Dependencies**: Iteration 50 field-level TTL infrastructure

**Requirements**:
- Atomic get-and-delete for multiple hash fields
- Returns array of values (nil for non-existent fields)
- Auto-deletes hash key when all fields removed
- Syntax: `HGETDEL key FIELDS numfields field [field ...]`

**Complexity Factors**:
- Must atomically retrieve values before deletion
- Needs proper memory management (clone values before free)
- Key auto-deletion logic (if `data.count() == 0`)

#### 2. HGETEX Implementation (Medium-High Complexity)
**Status**: Missing
**Impact**: HIGH — Extends field-level TTL with atomic read-modify pattern
**Dependencies**: Iteration 50 `hexpire()`, `hpersist()` methods

**Requirements**:
- Atomic get with expiration update
- Supports 5 mutually exclusive options: EX, PX, EXAT, PXAT, PERSIST
- Syntax: `HGETEX key [EX|PX|EXAT|PXAT|PERSIST] FIELDS numfields field [field ...]`
- Returns array of values, sets expiration on existing fields only

**Complexity Factors**:
- Complex argument parsing (optional expiration before FIELDS keyword)
- Mutual exclusivity validation
- Integration with existing `hexpire()` method
- PERSIST option needs to call `hpersist()`

#### 3. HSETEX Implementation (High Complexity)
**Status**: Missing
**Impact**: HIGH — Replaces multiple commands (HSET + HEXPIRE + conditional logic)
**Dependencies**: Iteration 50 field-level TTL, existing `hset()` method

**Requirements**:
- Atomic set with expiration and conditionals
- Supports 2 mutually exclusive conditional flags: FNX (all fields must not exist), FXX (all fields must exist)
- Supports 5 mutually exclusive expiration options: EX, PX, EXAT, PXAT, KEEPTTL
- Syntax: `HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT|KEEPTTL] FIELDS numfields field value [field value ...]`
- Returns 1 if all fields set, 0 if conditional failed

**Complexity Factors**:
- Most complex argument parsing (2 optional flag groups + FIELDS + pairs)
- All-or-nothing semantics (FNX/FXX check before any modification)
- KEEPTTL requires preserving existing field TTL
- Need to track which fields exist before operation

### Medium Priority (Extended Features)

None — all three commands are core features for Redis 8.0 compatibility.

### Low Priority (Nice to Have)

None — full specification compliance is required.

---

## Recommended Next Steps

### Phase 1: Storage Layer Implementation

**File**: `src/storage/memory.zig`

**Tasks**:

1. **Implement `hgetdel()` method** (Est. 1 hour)
   ```zig
   pub fn hgetdel(
       self: *Storage,
       allocator: std.mem.Allocator,
       key: []const u8,
       fields: []const []const u8,
   ) error{ WrongType, OutOfMemory }![]?[]const u8
   ```
   - Allocate result array
   - Lock storage, get hash entry
   - For each field: clone value if exists, then remove from hash
   - Check if hash empty → remove key
   - Return result array

2. **Implement `hgetex()` method** (Est. 1.5 hours)
   ```zig
   pub fn hgetex(
       self: *Storage,
       allocator: std.mem.Allocator,
       key: []const u8,
       fields: []const []const u8,
       expires_at_ms: ?i64,
       persist: bool,
   ) error{ WrongType, OutOfMemory }![]?[]const u8
   ```
   - Allocate result array
   - Lock storage, get hash entry
   - For each field: clone value if exists
   - If `persist`, call `hpersist()` on fields
   - Else if `expires_at_ms`, update field TTL
   - Return result array

3. **Implement `hsetex()` method** (Est. 2 hours)
   ```zig
   pub fn hsetex(
       self: *Storage,
       key: []const u8,
       fields: []const []const u8,
       values: []const []const u8,
       expires_at_ms: ?i64,
       options: u8, // FNX=1, FXX=2
       keep_ttl: bool,
   ) error{ WrongType, OutOfMemory }!bool
   ```
   - Lock storage
   - Check FNX condition: if any field exists, return false
   - Check FXX condition: if any field missing, return false
   - For each field-value pair:
     - If KEEPTTL, preserve existing `expires_at` or use null
     - Else, use provided `expires_at_ms`
     - Set field with new value + computed expiration
   - Return true

### Phase 2: Command Handler Implementation

**File**: `src/commands/hashes.zig`

**Tasks**:

1. **Implement `cmdHgetdel()`** (Est. 1.5 hours)
   - Validate args.len >= 4
   - Extract key
   - Validate FIELDS keyword at args[2]
   - Parse numfields from args[3]
   - Validate args.len == 4 + numfields
   - Extract field names from args[4..4+numfields]
   - Call `storage.hgetdel()`
   - Build RESP array response (values or nil)
   - Handle WRONGTYPE error

2. **Implement `cmdHgetex()`** (Est. 2 hours)
   - Validate args.len >= 4
   - Extract key
   - Parse optional expiration option (scan for EX/PX/EXAT/PXAT/PERSIST)
   - Ensure mutual exclusivity (track seen options, error if >1)
   - Find FIELDS keyword position
   - Parse numfields
   - Validate args.len matches expected
   - Extract field names
   - Compute `expires_at_ms` based on option type
   - Call `storage.hgetex()`
   - Build RESP array response
   - Handle WRONGTYPE error

3. **Implement `cmdHsetex()`** (Est. 2.5 hours)
   - Validate args.len >= 5
   - Extract key
   - Parse optional FNX/FXX flag (scan args, ensure mutual exclusivity)
   - Parse optional expiration option (EX/PX/EXAT/PXAT/KEEPTTL, ensure mutual exclusivity)
   - Validate FNX/FXX not both present
   - Validate only one expiration option
   - Find FIELDS keyword position
   - Parse numfields
   - Validate args.len == FIELDS_pos + 1 + numfields * 2
   - Extract field-value pairs
   - Compute `expires_at_ms` based on option
   - Build options bitmask (FNX=1, FXX=2)
   - Call `storage.hsetex()`
   - Return 1 if success, 0 if conditional failed
   - Handle WRONGTYPE error

### Phase 3: Command Routing

**File**: `src/server.zig`

**Tasks**:

1. Add command dispatch entries to `executeCommand()`:
   ```zig
   "hgetdel" => try hashes_mod.cmdHgetdel(arena, storage, args),
   "hgetex" => try hashes_mod.cmdHgetex(arena, storage, args),
   "hsetex" => try hashes_mod.cmdHsetex(arena, storage, args),
   ```

### Phase 4: Unit Tests

**File**: `src/commands/hashes.zig` (inline tests)

**Test Coverage** (minimum 15 tests):

**HGETDEL**:
- Basic get-and-delete
- Non-existent fields return nil
- Key auto-deletion when all fields removed
- Partial field deletion (key remains)
- WRONGTYPE error
- Argument count error
- numfields mismatch error

**HGETEX**:
- Get with EX expiration (verify with HTTL)
- Get with PX expiration
- Get with EXAT expiration
- Get with PXAT expiration
- Get with PERSIST (removes TTL)
- Get without expiration option (like HMGET)
- Non-existent fields return nil
- Mutual exclusivity error (EX + PX)
- WRONGTYPE error

**HSETEX**:
- Basic set with EX expiration
- FNX succeeds on new fields
- FNX fails when any field exists
- FXX succeeds when all fields exist
- FXX fails when any field missing
- KEEPTTL retains existing field TTL
- KEEPTTL on new field (no TTL)
- Mutual exclusivity errors (FNX+FXX, EX+KEEPTTL)
- WRONGTYPE error

### Phase 5: Integration Tests

**File**: `tests/test_hash_field_operations.zig` (new file)

**Test Scenarios** (minimum 10 tests):

1. **HGETDEL atomicity**: Verify values returned match what was deleted
2. **HGETDEL key cleanup**: Delete all fields, verify KEYS returns empty
3. **HGETEX expiration flow**: HSETEX → HGETEX EX → HTTL → wait → HGET (expired)
4. **HGETEX PERSIST**: HEXPIRE → HGETEX PERSIST → HTTL (should be -1)
5. **HSETEX FNX race**: Concurrent HSETEX FNX, only one succeeds
6. **HSETEX FXX race**: HDEL + HSETEX FXX, fails if field missing
7. **HSETEX KEEPTTL preservation**: Set TTL, update value with KEEPTTL, verify TTL unchanged
8. **Cross-command interaction**: HSET → HGETEX EX → HGETDEL → verify all gone
9. **Expiration edge case**: HSETEX with past EXAT, immediate expiry
10. **Error propagation**: HGETDEL/HGETEX/HSETEX on string key → WRONGTYPE

### Phase 6: Redis Compatibility Validation

**Differential Testing** (against Redis 8.0+):

1. Start Redis 8.0 instance on port 6380
2. Run comparison script:
   ```bash
   # Test HGETDEL
   redis-cli -p 6379 HSET h1 f1 v1 f2 v2 f3 v3
   redis-cli -p 6380 HSET h1 f1 v1 f2 v2 f3 v3
   diff <(redis-cli -p 6379 HGETDEL h1 FIELDS 2 f1 f4) \
        <(redis-cli -p 6380 HGETDEL h1 FIELDS 2 f1 f4)

   # Test HGETEX
   redis-cli -p 6379 HSET h2 f1 v1
   redis-cli -p 6380 HSET h2 f1 v1
   diff <(redis-cli -p 6379 HGETEX h2 EX 100 FIELDS 1 f1) \
        <(redis-cli -p 6380 HGETEX h2 EX 100 FIELDS 1 f1)

   # Test HSETEX
   diff <(redis-cli -p 6379 HSETEX h3 FNX EX 60 FIELDS 1 f1 v1) \
        <(redis-cli -p 6380 HSETEX h3 FNX EX 60 FIELDS 1 f1 v1)
   ```

3. Verify all examples from specification document
4. Verify error messages match Redis exactly

### Phase 7: Performance Validation

**Benchmarks** (using redis-benchmark or custom script):

1. HGETDEL throughput vs HMGET + HDEL
2. HGETEX throughput vs HMGET + HEXPIRE
3. HSETEX throughput vs HSET + HEXPIRE
4. Memory overhead validation (no leaks under load)

**Acceptance Criteria**:
- Throughput >= 70% of equivalent multi-command sequence
- Zero memory leaks after 1M operations
- Latency p99 < 1ms for 100-field operations

---

## Implementation Notes

### Zig-Specific Considerations

1. **Memory Management**:
   - `hgetdel()` must clone field values before deletion (use `allocator.dupe()`)
   - Result arrays must be freed by caller (document with comments)
   - Use `defer` for cleanup in error paths

2. **Mutex Locking**:
   - All three methods require `storage.mutex.lock()` at start
   - Use `defer self.mutex.unlock()` immediately after lock
   - Keep critical sections minimal (clone data, then release lock)

3. **Argument Parsing**:
   - Use `std.mem.eql(u8, str, "KEYWORD")` for keyword matching
   - Use `std.fmt.parseInt()` for numeric parsing with error handling
   - Build flags with bitwise operations: `options |= 1` for FNX

4. **Error Handling**:
   - Return `error.WrongType` for type mismatches
   - Return `error.OutOfMemory` for allocation failures
   - Use `catch |err|` blocks to convert storage errors to RESP errors

5. **Existing Patterns**:
   - Follow `hexpire()` pattern for TTL updates
   - Follow `hset()` pattern for field creation/update
   - Follow `hdel()` pattern for field removal + key cleanup
   - Follow `cmdHexpire()` pattern for argument parsing with FIELDS

### Integration with Iteration 50 Infrastructure

**Reusable Components**:
- `FieldValue` struct with `expires_at` (no changes needed)
- `hexpire()` method for field-level TTL setting
- `hpersist()` method for TTL removal
- FIELDS keyword parsing pattern from HEXPIRE commands

**New Logic Required**:
- **HGETDEL**: Clone-before-delete pattern (not in existing commands)
- **HGETEX**: Combine read with conditional expiration update
- **HSETEX**: All-or-nothing check (FNX/FXX) before any modification

### Potential Challenges

1. **HSETEX FNX/FXX Semantics**:
   - Challenge: Must check ALL fields before setting ANY fields
   - Solution: Two-pass approach — validate conditions first, then set if passed

2. **KEEPTTL Implementation**:
   - Challenge: Preserve existing `expires_at` for existing fields
   - Solution: Before update, read current `expires_at`, use it if `keep_ttl=true`

3. **Argument Parsing Complexity**:
   - Challenge: Multiple optional argument groups, order varies
   - Solution: Scan args for keywords, track positions, validate at end

4. **Memory Safety**:
   - Challenge: HGETDEL must return values of deleted fields
   - Solution: Clone values to temporary array before removing from hash

---

## Definition of Done

### Must Complete:
- [ ] All 3 storage methods implemented and tested
- [ ] All 3 command handlers implemented and tested
- [ ] Command routing configured
- [ ] Minimum 15 unit tests passing (0 failures, 0 leaks)
- [ ] Minimum 10 integration tests passing
- [ ] Redis compatibility: 100% match on examples from spec
- [ ] Performance: >= 70% of Redis throughput
- [ ] Zero memory leaks after full test suite
- [ ] Documentation updated (README.md, CLAUDE.md)
- [ ] No regressions in existing hash commands

### Quality Gates:
- [ ] `zig build test` — 0 failures, 0 memory leaks
- [ ] Differential testing vs Redis 8.0 — byte-exact RESP match
- [ ] Zig quality review — no Critical/Important issues
- [ ] Code review — clean architecture, proper separation of concerns

---

## Estimated Effort

| Phase | Tasks | Est. Hours | Complexity |
|-------|-------|-----------|------------|
| Storage Layer | 3 methods | 4.5 | Medium-High |
| Command Handlers | 3 commands | 6.0 | High |
| Command Routing | 3 entries | 0.5 | Low |
| Unit Tests | 15+ tests | 3.0 | Medium |
| Integration Tests | 10+ tests | 2.0 | Medium |
| Compatibility Validation | Differential testing | 1.5 | Medium |
| Performance Validation | Benchmarks | 1.0 | Low |
| Documentation | README, CLAUDE | 0.5 | Low |
| **Total** | | **19.0** | **High** |

**Suggested Timeline**: 2-3 development sessions (6-8 hours each)

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Complex argument parsing errors | HIGH | Extensive unit tests, compare with redis-cli |
| FNX/FXX logic bugs | HIGH | Integration tests with race conditions |
| Memory leaks in clone-before-delete | MEDIUM | Use testing allocator, valgrind validation |
| Performance regression | LOW | Benchmark vs multi-command equivalents |
| KEEPTTL edge cases | MEDIUM | Test with existing TTL, no TTL, expired fields |

---

## Success Criteria

**Functional**:
- All 3 commands work identically to Redis 8.0
- Zero memory leaks
- All error messages match Redis exactly

**Performance**:
- HGETDEL >= 70% of HMGET + HDEL throughput
- HGETEX >= 70% of HMGET + HEXPIRE throughput
- HSETEX >= 70% of HSET + HEXPIRE throughput

**Quality**:
- Zero Critical/Important issues from code review
- 100% test coverage of command logic paths
- Clean integration with Iteration 50 TTL infrastructure

---

## Post-Implementation Notes

**Future Optimizations**:
- Consider batch optimization if HGETDEL/HGETEX/HSETEX become hot paths
- Potential for lazy deletion in HGETDEL (mark deleted, cleanup later)

**Related Future Work**:
- More Redis 8.0 commands (LGETDEL, SGETDEL, etc. if added)
- Lua scripting integration (atomic multi-step operations)

**Documentation Needs**:
- Update PRD Phase 1 checklist
- Add examples to README usage section
- Document atomicity guarantees

---

## References

- [HGETDEL Redis Docs](https://redis.io/docs/latest/commands/hgetdel/)
- [HGETEX Redis Docs](https://redis.io/docs/latest/commands/hgetex/)
- [HSETEX Redis Docs](https://redis.io/docs/latest/commands/hsetex/)
- [Iteration 52 Detailed Spec](/Users/fn/Desktop/codespace/zoltraak/docs/iteration_52_spec.md)
- [Iteration 50 Implementation](https://github.com/yusa-imit/zoltraak/commit/hash-field-ttl) (reference)
- [RESP Protocol Spec](https://redis.io/docs/latest/develop/reference/protocol-spec/)
