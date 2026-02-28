# Iteration 52 Implementation Checklist

## Pre-Implementation

- [x] Redis specification analyzed (HGETDEL, HGETEX, HSETEX)
- [x] Detailed specification document created (`iteration_52_spec.md`)
- [x] Implementation plan created (`iteration_52_plan.md`)
- [x] Existing infrastructure reviewed (Iteration 50 TTL)
- [x] Architecture patterns identified (command dispatch, storage API)

## Phase 1: Storage Layer Implementation

**File**: `/Users/fn/Desktop/codespace/zoltraak/src/storage/memory.zig`

### hgetdel() Method
- [ ] Function signature matches spec
- [ ] Allocates result array for field values
- [ ] Locks storage mutex
- [ ] Handles key-level expiration check
- [ ] Handles WRONGTYPE error
- [ ] For each field:
  - [ ] Clones value if field exists (before deletion)
  - [ ] Stores nil for non-existent fields
  - [ ] Removes field from hash
- [ ] Checks if hash empty → removes key
- [ ] Unlocks mutex
- [ ] Returns result array
- [ ] Error handling with errdefer cleanup
- [ ] Inline unit test added

### hgetex() Method
- [ ] Function signature matches spec (expires_at_ms, persist params)
- [ ] Allocates result array for field values
- [ ] Locks storage mutex
- [ ] Handles key-level expiration check
- [ ] Handles WRONGTYPE error
- [ ] For each field:
  - [ ] Clones value if field exists
  - [ ] Stores nil for non-existent fields
- [ ] If persist=true:
  - [ ] Removes TTL from existing fields
- [ ] Else if expires_at_ms is not null:
  - [ ] Updates field-level TTL
- [ ] Unlocks mutex
- [ ] Returns result array
- [ ] Error handling with errdefer cleanup
- [ ] Inline unit test added

### hsetex() Method
- [ ] Function signature matches spec (options bitmask, keep_ttl param)
- [ ] Locks storage mutex
- [ ] Handles key-level expiration check
- [ ] Handles WRONGTYPE error
- [ ] FNX (options & 1) condition check:
  - [ ] If ANY field exists, return false (no modification)
- [ ] FXX (options & 2) condition check:
  - [ ] If ANY field missing, return false (no modification)
- [ ] For each field-value pair:
  - [ ] Determines expiration (keep_ttl vs expires_at_ms)
  - [ ] Sets field with value + computed expiration
- [ ] Unlocks mutex
- [ ] Returns true
- [ ] Error handling with errdefer cleanup
- [ ] Inline unit test added

## Phase 2: Command Handler Implementation

**File**: `/Users/fn/Desktop/codespace/zoltraak/src/commands/hashes.zig`

### cmdHgetdel()
- [ ] Function signature correct (allocator, storage, args)
- [ ] Validates args.len >= 4
- [ ] Extracts key from args[1]
- [ ] Validates FIELDS keyword at args[2]
- [ ] Parses numfields from args[3]
- [ ] Validates args.len == 4 + numfields
- [ ] Extracts field names (args[4..4+numfields])
- [ ] Calls storage.hgetdel()
- [ ] Builds RESP array response
  - [ ] Null values for nil fields
  - [ ] Bulk strings for values
- [ ] Handles WRONGTYPE error
- [ ] Handles OutOfMemory error
- [ ] Returns proper error messages
- [ ] Inline unit test: basic operation
- [ ] Inline unit test: key auto-deletion
- [ ] Inline unit test: nil fields
- [ ] Inline unit test: WRONGTYPE
- [ ] Inline unit test: argument errors

### cmdHgetex()
- [ ] Function signature correct
- [ ] Validates args.len >= 4
- [ ] Extracts key from args[1]
- [ ] Scans for expiration option (EX/PX/EXAT/PXAT/PERSIST)
- [ ] Validates mutual exclusivity (error if >1 option)
- [ ] Finds FIELDS keyword position
- [ ] Parses numfields
- [ ] Validates args.len matches expected
- [ ] Extracts field names
- [ ] Computes expires_at_ms based on option:
  - [ ] EX: current_time + seconds * 1000
  - [ ] PX: current_time + milliseconds
  - [ ] EXAT: unix_seconds * 1000
  - [ ] PXAT: unix_milliseconds
  - [ ] PERSIST: persist=true
  - [ ] None: expires_at_ms=null
- [ ] Calls storage.hgetex()
- [ ] Builds RESP array response
- [ ] Handles all error conditions
- [ ] Inline unit test: EX option
- [ ] Inline unit test: PX option
- [ ] Inline unit test: EXAT option
- [ ] Inline unit test: PXAT option
- [ ] Inline unit test: PERSIST option
- [ ] Inline unit test: No expiration option
- [ ] Inline unit test: Mutual exclusivity error
- [ ] Inline unit test: WRONGTYPE

### cmdHsetex()
- [ ] Function signature correct
- [ ] Validates args.len >= 5
- [ ] Extracts key from args[1]
- [ ] Scans for FNX flag
- [ ] Scans for FXX flag
- [ ] Validates FNX and FXX not both present
- [ ] Scans for expiration option (EX/PX/EXAT/PXAT/KEEPTTL)
- [ ] Validates expiration options mutual exclusivity
- [ ] Finds FIELDS keyword position
- [ ] Parses numfields
- [ ] Validates args.len == FIELDS_pos + 1 + numfields * 2
- [ ] Extracts field-value pairs
- [ ] Computes expires_at_ms based on option
- [ ] Builds options bitmask (FNX=1, FXX=2)
- [ ] Calls storage.hsetex()
- [ ] Returns 1 if success, 0 if conditional failed
- [ ] Handles all error conditions
- [ ] Inline unit test: Basic with EX
- [ ] Inline unit test: FNX success (new fields)
- [ ] Inline unit test: FNX failure (existing fields)
- [ ] Inline unit test: FXX success (all exist)
- [ ] Inline unit test: FXX failure (any missing)
- [ ] Inline unit test: KEEPTTL preservation
- [ ] Inline unit test: KEEPTTL on new field
- [ ] Inline unit test: FNX+FXX mutual exclusivity error
- [ ] Inline unit test: EX+KEEPTTL mutual exclusivity error
- [ ] Inline unit test: WRONGTYPE

## Phase 3: Command Routing

**File**: `/Users/fn/Desktop/codespace/zoltraak/src/commands/strings.zig` (executeCommand)

### Write Command List Updates
- [ ] Add "HGETDEL" to replica read-only guard (line ~135)
- [ ] Add "HGETEX" to replica read-only guard
- [ ] Add "HSETEX" to replica read-only guard
- [ ] Add "HGETDEL" to AOF write command list (line ~203)
- [ ] Add "HGETEX" to AOF write command list
- [ ] Add "HSETEX" to AOF write command list

### Command Dispatch
- [ ] Add HGETDEL dispatch after HPEXPIRETIME (line ~458)
  ```zig
  } else if (std.mem.eql(u8, cmd_upper, "HGETDEL")) {
      break :blk try hashes.cmdHgetdel(allocator, storage, array);
  ```
- [ ] Add HGETEX dispatch
  ```zig
  } else if (std.mem.eql(u8, cmd_upper, "HGETEX")) {
      break :blk try hashes.cmdHgetex(allocator, storage, array);
  ```
- [ ] Add HSETEX dispatch
  ```zig
  } else if (std.mem.eql(u8, cmd_upper, "HSETEX")) {
      break :blk try hashes.cmdHsetex(allocator, storage, array);
  ```

## Phase 4: Testing

### Unit Tests (inline in hashes.zig)
- [ ] All 15+ unit tests passing
- [ ] Zero memory leaks detected
- [ ] Test coverage >= 90% of command logic paths

### Integration Tests
**File**: `/Users/fn/Desktop/codespace/zoltraak/tests/test_hash_field_operations.zig`

- [ ] Test file created
- [ ] HGETDEL atomicity test
- [ ] HGETDEL key cleanup test
- [ ] HGETEX expiration flow test
- [ ] HGETEX PERSIST test
- [ ] HSETEX FNX test (success + failure)
- [ ] HSETEX FXX test (success + failure)
- [ ] HSETEX KEEPTTL preservation test
- [ ] Cross-command interaction test
- [ ] Expiration edge case test (past timestamp)
- [ ] WRONGTYPE error propagation test
- [ ] All integration tests passing
- [ ] Zero memory leaks

### Build & Test
- [ ] `zig build` — compiles successfully
- [ ] `zig build test` — all tests pass (0 failures, 0 leaks)
- [ ] `zig build test-integration` — integration tests pass

## Phase 5: Redis Compatibility Validation

### Differential Testing
- [ ] Redis 8.0+ instance available on port 6380
- [ ] HGETDEL examples from spec validated
- [ ] HGETEX examples from spec validated
- [ ] HSETEX examples from spec validated
- [ ] Error messages match Redis exactly
- [ ] Edge cases validated (nil fields, key cleanup, TTL)
- [ ] RESP response byte-exact match

### Compatibility Criteria
- [ ] >= 95% command compatibility
- [ ] 100% match on specification examples
- [ ] Error messages identical to Redis

## Phase 6: Performance Validation

### Benchmarks
- [ ] HGETDEL throughput measured
- [ ] HGETEX throughput measured
- [ ] HSETEX throughput measured
- [ ] Compared vs multi-command equivalents
- [ ] Memory overhead measured (no leaks after 1M ops)
- [ ] Latency p99 < 1ms for 100-field ops

### Performance Criteria
- [ ] >= 70% of Redis throughput
- [ ] < 10% regression vs existing hash commands
- [ ] Zero memory leaks under load

## Phase 7: Code Quality Review

### Zig Quality Review
- [ ] Memory safety verified (allocator patterns correct)
- [ ] Error handling complete (all paths covered)
- [ ] Mutex locking correct (defer unlock, minimal critical section)
- [ ] Idiomatic Zig code (comptime where appropriate)
- [ ] Doc comments added for all public functions
- [ ] No Critical/Important issues identified

### Architecture Review
- [ ] Separation of concerns clean (storage vs command vs protocol)
- [ ] API design consistent with existing patterns
- [ ] Integration with Iteration 50 TTL correct
- [ ] No code duplication
- [ ] Maintainability score: Good/Excellent

## Phase 8: Documentation

### README.md Updates
- [ ] Hash Commands table updated with HGETDEL, HGETEX, HSETEX
- [ ] Command syntax documented
- [ ] Return values documented
- [ ] Examples added

### CLAUDE.md Updates
- [ ] Iteration 52 added to completed iterations table
- [ ] Command count updated
- [ ] Iteration description added

### PRD Updates (if applicable)
- [ ] Phase 1 checklist updated (if relevant)

## Phase 9: Cleanup

### Temporary Files
- [ ] `docs/iteration_52_spec.md` — KEEP (reference)
- [ ] `docs/iteration_52_plan.md` — DELETE after implementation
- [ ] `docs/iteration_52_summary.md` — DELETE after implementation
- [ ] `docs/iteration_52_checklist.md` — DELETE after completion

### Process Cleanup
- [ ] All background processes killed (`pkill -f zoltraak`)
- [ ] Port 6379 released (`lsof -ti :6379 | xargs kill`)
- [ ] No debug prints left in code
- [ ] No TODO comments left unresolved

## Phase 10: Commit & Push

### Git Commit
- [ ] All changes staged
- [ ] Commit message: `feat(hash): implement Iteration 52 — HGETDEL, HGETEX, HSETEX`
- [ ] Commit body includes:
  - Commands added
  - Test count
  - Redis compatibility status
  - Performance notes

### Git Push
- [ ] Pushed to origin/main
- [ ] CI/CD passes (if configured)
- [ ] No force push used

### Discord Notification
- [ ] Message sent:
  ```
  openclaw message send --channel discord --target user:264745080709971968 \
    --message "[zoltraak] Iteration 52 complete — HGETDEL, HGETEX, HSETEX (Redis 8.0 hash field operations). 3 commands, 15+ tests, 100% spec compliance."
  ```

## Post-Implementation

### Verification
- [ ] Clean build from scratch (`rm -rf zig-out zig-cache && zig build`)
- [ ] Full test suite passes
- [ ] Redis compatibility re-verified
- [ ] Performance benchmarks re-run
- [ ] No regressions in existing tests

### Documentation Check
- [ ] README.md renders correctly
- [ ] CLAUDE.md updated
- [ ] No broken links
- [ ] Examples are copy-paste ready

## Completion Criteria (ALL must be checked)

- [ ] All 3 storage methods implemented and tested
- [ ] All 3 command handlers implemented and tested
- [ ] Command routing configured
- [ ] 15+ unit tests passing (0 failures, 0 leaks)
- [ ] 10+ integration tests passing
- [ ] Redis compatibility: 100% match on spec examples
- [ ] Performance: >= 70% of Redis throughput
- [ ] Zero memory leaks after full test suite
- [ ] Documentation updated (README.md, CLAUDE.md)
- [ ] No regressions in existing hash commands
- [ ] Code quality review: 0 Critical/Important issues
- [ ] All temporary files cleaned up
- [ ] Changes committed and pushed
- [ ] Discord notification sent

---

## Notes

- This checklist should be followed sequentially
- Do not skip phases — each builds on the previous
- If any test fails, stop and fix before proceeding
- Quality gates (Phase 5, Phase 6, Phase 7) are BLOCKING
- Keep this checklist updated during implementation
- Mark items complete as you go

**Estimated Time**: 19 hours (see iteration_52_plan.md)

**Success Metric**: All checkboxes completed, zero failures, 100% Redis compatibility
