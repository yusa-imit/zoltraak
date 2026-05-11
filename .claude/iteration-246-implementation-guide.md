# Iteration 246 Implementation Guide — Keyspace Notifications for Data Structures

## Status

**COMPLETED**: List commands (21 commands) ✅
**REMAINING**: Sets (7 commands), Hashes (6 commands), Sorted Sets (17 commands), Streams (9 commands) = 39 commands

## Completed Work

### List Commands (21 commands) — DONE ✅

All list commands have been updated with:
1. ✅ Imported notification infrastructure (notifications_mod, pubsub_mod)
2. ✅ Added helper functions (notifyListEvent, notifyGenericEvent)
3. ✅ Updated all 21 command signatures to accept `ps: *PubSub, db_index: u32`
4. ✅ Updated all 21 dispatch calls in strings.zig with `client_registry.getSelectedDb(client_id)`
5. ✅ Added notification logic to all critical commands

**List commands with full notification support**:
- LPUSH, RPUSH (fire "lpush"/"rpush")
- LPOP, RPOP (fire "lpop"/"rpop" + "del" if empty)
- LSET (fires "lset")
- LTRIM (fires "ltrim" + "del" if empty)
- LREM (fires "lrem" + "del" if removed)
- LPUSHX, RPUSHX (fires "lpush"/"rpush" if key exists)
- LINSERT (fires "linsert" if pivot found)
- LMOVE (fires source pop + dest push + "del" in correct order)
- RPOPLPUSH (fires "rpop" + "lpush" + "del" in correct order)
- Blocking commands (BLPOP, BRPOP, BLMOVE, LMPOP, BLMPOP): signatures updated, notifications deferred

**All 59 unit tests pass** ✅

---

## Remaining Implementation Tasks

### Part 2: Set Commands (7 commands)

**File**: `src/commands/sets.zig`

#### Step 1: Infrastructure (Already Done)
- ✅ Added imports: `notifications_mod`, `pubsub_mod`
- ✅ Added `PubSub` const
- ✅ Added `notifySetEvent()` and `notifyGenericEvent()` helpers

#### Step 2: Update signatures (REQUIRED)

For each of these commands, update the function signature:
```zig
// OLD
pub fn cmdSADD(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ...) ![]const u8

// NEW
pub fn cmdSADD(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32, ...) ![]const u8
```

Commands to update:
1. `cmdSadd` - Add `ps, db_index` parameters (before client_registry if present)
2. `cmdSrem` - Add `ps, db_index` parameters
3. `cmdSpop` - Add `ps, db_index` parameters
4. `cmdSmove` - Add `ps, db_index` parameters
5. `cmdSinterstore` - Add `ps, db_index` parameters
6. `cmdSunionstore` - Add `ps, db_index` parameters
7. `cmdSdiffstore` - Add `ps, db_index` parameters

Non-notifiable commands (can add `_ = ps; _ = db_index;` placeholders):
- cmdSismember, cmdSmembers, cmdScard, cmdSunion, cmdSinter, cmdSdiff, cmdSrandmember, cmdSmismember, cmdSintercard

#### Step 3: Update dispatch in strings.zig

Update all set command calls around lines 958-983:
```zig
// Example SADD
else if (std.mem.eql(u8, cmd_upper, "SADD")) {
    const selected_db = client_registry.getSelectedDb(client_id);
    break :blk try sets.cmdSadd(allocator, storage, array, ps, selected_db, client_registry, client_id);
}

// Example SREM
else if (std.mem.eql(u8, cmd_upper, "SREM")) {
    const selected_db = client_registry.getSelectedDb(client_id);
    break :blk try sets.cmdSrem(allocator, storage, array, ps, selected_db);
}
```

#### Step 4: Add notification logic

**SADD** (line ~55):
```zig
const added = storage.sadd(key, members) catch |err| { ... };
if (added > 0) {
    notifySetEvent(allocator, storage, ps, db_index, key, "sadd");
}
```

**SREM** (line ~107):
```zig
const removed = storage.srem(key, members) catch |err| { ... };
if (removed > 0) {
    notifySetEvent(allocator, storage, ps, db_index, key, "srem");
    if (!storage.exists(key)) {
        notifyGenericEvent(allocator, storage, ps, db_index, key, "del");
    }
}
```

**SPOP** (line ~1086):
```zig
const popped = storage.spop(allocator, key, count) catch |err| { ... };
if (popped != null and popped.?.len > 0) {
    notifySetEvent(allocator, storage, ps, db_index, key, "spop");
    if (!storage.exists(key)) {
        notifyGenericEvent(allocator, storage, ps, db_index, key, "del");
    }
}
```

**SMOVE**:
```zig
const moved = storage.smove(src, dst, member) catch |err| { ... };
if (moved) {
    notifySetEvent(allocator, storage, ps, db_index, src, "srem");
    notifySetEvent(allocator, storage, ps, db_index, dst, "sadd");
    if (!storage.exists(src)) {
        notifyGenericEvent(allocator, storage, ps, db_index, src, "del");
    }
}
```

**SINTERSTORE, SUNIONSTORE, SDIFFSTORE**:
```zig
const result_count = storage.sinterstore(dst, sources) catch |err| { ... };
notifySetEvent(allocator, storage, ps, db_index, dst, "sinterstore");
if (result_count == 0 and key_existed) {
    notifyGenericEvent(allocator, storage, ps, db_index, dst, "del");
}
```

---

### Part 3: Hash Commands (6 commands)

**File**: `src/commands/hashes.zig`

Same pattern as sets:
1. Add imports and helpers
2. Update 6 command signatures: HSET, HSETNX, HMSET, HDEL, HINCRBY, HINCRBYFLOAT
3. Update dispatch calls in strings.zig (~lines 985-1010)
4. Add notification logic

**Key events**:
- HSET/HSETNX/HMSET: fire "hset"
- HDEL: fire "hdel" + "del" if empty
- HINCRBY: fire "hincrby"
- HINCRBYFLOAT: fire "hincrbyfloat"

---

### Part 4: Sorted Set Commands (17 commands)

**File**: `src/commands/sorted_sets.zig`

**Critical**: ZADD with INCR flag should fire "zincr" not "zadd"

Commands needing notifications (extract from specification):
- ZADD (check INCR option!)
- ZREM
- ZINCRBY
- ZPOPMIN
- ZPOPMAX
- BZPOPMIN, BZPOPMAX
- ZMPOP, BZMPOP
- ZREMRANGEBYRANK
- ZREMRANGEBYSCORE
- ZREMRANGEBYLEX
- ZINTERSTORE
- ZUNIONSTORE
- ZDIFFSTORE

---

### Part 5: Stream Commands (9 commands)

**File**: `src/commands/streams.zig` and `src/commands/streams_advanced.zig`

**CRITICAL**: Streams NEVER fire "del" event (unlike lists/sets/hashes/zsets)

Commands:
- XADD: fire "xadd" + "xtrim" (if trimming occurred)
- XDEL: fire "xdel" (no del event)
- XTRIM: fire "xtrim" (no del event)
- XSETID: fire "xsetid"
- XGROUP CREATE: fire "xgroup-create"
- XGROUP CREATECONSUMER: fire "xgroup-createconsumer"
- XGROUP DELCONSUMER: fire "xgroup-delconsumer"
- XGROUP DESTROY: fire "xgroup-destroy"
- XGROUP SETID: fire "xgroup-setid"

---

## Implementation Checklist

### Sets (7 commands)
- [ ] Add imports to sets.zig
- [ ] Add helpers to sets.zig
- [ ] Update cmdSadd signature
- [ ] Update cmdSrem signature
- [ ] Update cmdSpop signature
- [ ] Update cmdSmove signature
- [ ] Update cmdSinterstore signature
- [ ] Update cmdSunionstore signature
- [ ] Update cmdSdiffstore signature
- [ ] Update all 16 set command dispatch calls in strings.zig
- [ ] Add notification logic to 7 commands
- [ ] Test: `zig build test`

### Hashes (6 commands)
- [ ] Add imports to hashes.zig
- [ ] Add helpers to hashes.zig
- [ ] Update 6 command signatures
- [ ] Update hash command dispatch in strings.zig
- [ ] Add notification logic to 6 commands
- [ ] Test: `zig build test`

### Sorted Sets (17 commands)
- [ ] Add imports to sorted_sets.zig
- [ ] Add helpers to sorted_sets.zig
- [ ] Update 17 command signatures
- [ ] Update sorted set command dispatch in strings.zig
- [ ] Add notification logic (remember ZADD INCR = "zincr")
- [ ] Test: `zig build test`

### Streams (9 commands)
- [ ] Add imports to streams.zig and streams_advanced.zig
- [ ] Add helpers to both files
- [ ] Update 9 command signatures
- [ ] Update stream command dispatch in strings.zig
- [ ] Add notification logic (NO del events!)
- [ ] Test: `zig build test`

---

## Testing

Run after each section:
```bash
zig build
zig build test
```

All 59+ tests must pass with no memory leaks.

---

## Next Steps

1. **Immediately after this guide is written**: Continue with Part 2 (Sets)
2. Keep following the established pattern from list commands
3. Remember to update dispatch calls for ALL commands, even read-only ones
4. Test frequently to catch issues early
5. Commit after each data type is complete

---

## Notes

- Pattern is identical for all data types
- Key difference: Streams never fire "del" event
- ZADD has special case: check for INCR option
- Multi-key operations (SMOVE, LMOVE, etc.) must fire source event first
- Empty collection deletion is critical for lists/sets/hashes/zsets
