# Redis Keyspace Notifications Specification — Iteration 246
## Data Structure Commands (Lists, Sets, Hashes, Sorted Sets, Streams)

## Overview

This iteration extends keyspace notifications to cover all major Redis data structures. Iteration 245 completed the foundation (string and generic commands). This iteration adds notification support for:
- **List commands** (flag `l`) - LPUSH, RPUSH, LPOP, RPOP, LTRIM, LREM, LINSERT, LSET, LMOVE, RPOPLPUSH, etc.
- **Set commands** (flag `s`) - SADD, SREM, SPOP, SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE
- **Hash commands** (flag `h`) - HSET, HDEL, HINCRBY, HINCRBYFLOAT
- **Sorted set commands** (flag `z`) - ZADD, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE
- **Stream commands** (flag `t`) - XADD, XDEL, XTRIM, XSETID, XGROUP CREATE/DESTROY/CREATECONSUMER/DELCONSUMER/SETID

**Foundation**: Iteration 245 already implemented `src/storage/notifications.zig` with all helper functions and infrastructure. This iteration focuses on integrating notification calls into data structure command handlers.

---

## 1. Official Redis Documentation Reference

- **Keyspace notifications spec**: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- **List commands**: https://redis.io/commands/?group=list
- **Set commands**: https://redis.io/commands/?group=set
- **Hash commands**: https://redis.io/commands/?group=hash
- **Sorted set commands**: https://redis.io/commands/?group=sorted-set
- **Stream commands**: https://redis.io/commands/?group=stream

---

## 2. Command Mapping by Data Type

### 2.1 List Commands (Flag: `l`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| LPUSH | `lpush` | | Always fires on success |
| LPUSHX | `lpush` | | Only if key exists |
| RPUSH | `rpush` | | Always fires on success |
| RPUSHX | `rpush` | | Only if key exists |
| LPOP | `lpop` | `del` (if list empty) | Order: `lpop`, then `del` |
| RPOP | `rpop` | `del` (if list empty) | Order: `rpop`, then `del` |
| LINSERT | `linsert` | | Only if pivot found |
| LSET | `lset` | | Only if index valid |
| LREM | `lrem` | `del` (if list empty) | Order: `lrem`, then `del` |
| LTRIM | `ltrim` | `del` (if list empty) | Order: `ltrim`, then `del` |
| LMOVE | `lpop`/`rpop` (source) + `lpush`/`rpush` (dest) | `del` (if source empty) | **Guaranteed order**: source event before dest event |
| BLMOVE | Same as LMOVE | | Blocking version fires same events |
| RPOPLPUSH | `rpop` (source) + `lpush` (dest) | `del` (if source empty) | **Guaranteed order**: `rpop` before `lpush` |
| BRPOPLPUSH | Same as RPOPLPUSH | | Blocking version fires same events |
| LMPOP | `lpop`/`rpop` | `del` (if list empty) | One event per key popped from |
| BLMPOP | Same as LMPOP | | Blocking version fires same events |
| BLPOP | `lpop` | `del` (if list empty) | Only fires on successful pop |
| BRPOP | `rpop` | `del` (if list empty) | Only fires on successful pop |

**Critical Implementation Notes**:
1. **Empty list deletion**: When LPOP/RPOP/LREM/LTRIM removes the last element(s), fire the operation event first, then `del` (generic flag, not list flag)
2. **Multi-key operations**: LMOVE/RPOPLPUSH fire events on **both** source and destination keys
3. **Event ordering**: For LMOVE/RPOPLPUSH, source event **must** fire before destination event
4. **Blocking commands**: BLPOP, BRPOP, BLMOVE, etc. fire events only when they successfully pop (not when they block)
5. **Failed operations**: LPUSHX on non-existent key fires no event; LINSERT with missing pivot fires no event

### 2.2 Set Commands (Flag: `s`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| SADD | `sadd` | | Only if at least one member added |
| SREM | `srem` | `del` (if set empty) | Only if at least one member removed |
| SPOP | `spop` | `del` (if set empty) | Fires even if count=1 |
| SMOVE | `srem` (source) + `sadd` (dest) | `del` (if source empty) | **Guaranteed order**: `srem` before `sadd` |
| SINTERSTORE | `sinterstore` | `del` (if result empty and key existed) | Event on destination key only |
| SUNIONSTORE | `sunionstore` | `del` (if result empty and key existed) | Event on destination key only |
| SDIFFSTORE | `sdiffstore` | `del` (if result empty and key existed) | Event on destination key only |

**Critical Implementation Notes**:
1. **No event if no change**: SADD with all duplicates fires no event; SREM with non-existent members fires no event
2. **SPOP with count**: Even if popping multiple members, fire single `spop` event
3. **SMOVE**: Two events on two keys (source and dest), source first
4. **STORE operations**: Fire event on destination key only (not on source keys)
5. **Empty result**: SINTERSTORE creating empty set fires `sinterstore` + `del` if destination key already existed

### 2.3 Hash Commands (Flag: `h`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| HSET | `hset` | | Fires if at least one field added/updated |
| HSETNX | `hset` | | Only if field didn't exist |
| HMSET | `hset` | | Deprecated alias of HSET |
| HDEL | `hdel` | `del` (if hash empty) | Only if at least one field deleted |
| HINCRBY | `hincrby` | | Always fires on success |
| HINCRBYFLOAT | `hincrbyfloat` | | Always fires on success |

**Critical Implementation Notes**:
1. **HSET multi-field**: Fire single `hset` event even if setting multiple fields
2. **HSET update vs create**: Event fires whether fields are new or updated (no distinction)
3. **HDEL**: Fire only if at least one field actually deleted
4. **Empty hash deletion**: HDEL removing last field fires `hdel` + `del`
5. **HINCRBY/HINCRBYFLOAT**: Always fire event (create field with value if missing)

### 2.4 Sorted Set Commands (Flag: `z`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| ZADD | `zadd` | | Fires if at least one member added/updated |
| ZADD with INCR | `zincr` | | **Not** `zadd` when INCR option used |
| ZREM | `zrem` | `del` (if zset empty) | Only if at least one member removed |
| ZINCRBY | `zincr` | | Always fires on success |
| ZPOPMIN | `zpopmin` | `del` (if zset empty) | Fires even if count=1 |
| ZPOPMAX | `zpopmax` | `del` (if zset empty) | Fires even if count=1 |
| BZPOPMIN | `zpopmin` | `del` (if zset empty) | Only fires on successful pop |
| BZPOPMAX | `zpopmax` | `del` (if zset empty) | Only fires on successful pop |
| ZMPOP | `zpopmin`/`zpopmax` | `del` (if zset empty) | Event depends on MIN/MAX option |
| BZMPOP | Same as ZMPOP | | Blocking version fires same events |
| ZREMRANGEBYRANK | `zremrangebyrank` | `del` (if zset empty) | Only if at least one member removed |
| ZREMRANGEBYSCORE | `zremrangebyscore` | `del` (if zset empty) | Only if at least one member removed |
| ZREMRANGEBYLEX | `zremrangebylex` | `del` (if zset empty) | Only if at least one member removed |
| ZINTERSTORE | `zinterstore` | `del` (if result empty and key existed) | Event on destination key only |
| ZUNIONSTORE | `zunionstore` | `del` (if result empty and key existed) | Event on destination key only |
| ZDIFFSTORE | `zdiffstore` | `del` (if result empty and key existed) | Event on destination key only |

**Critical Implementation Notes**:
1. **ZADD with INCR**: Fires `zincr` event, **not** `zadd` (special case)
2. **ZADD options**: NX/XX/GT/LT affect whether event fires (no change = no event)
3. **ZADD multi-member**: Fire single `zadd` event even if adding multiple members
4. **ZREM**: Fire only if at least one member actually removed
5. **ZPOPMIN/ZPOPMAX with count**: Fire single event even if popping multiple members
6. **STORE operations**: Fire event on destination key only (not on source keys)

### 2.5 Stream Commands (Flag: `t`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| XADD | `xadd` | `xtrim` (if MAXLEN/MINID used) | **Order**: `xadd`, then `xtrim` if trimming |
| XDEL | `xdel` | | Always fires on success |
| XTRIM | `xtrim` | | Only if at least one entry removed |
| XSETID | `xsetid` | | Always fires on success |
| XGROUP CREATE | `xgroup-create` | | Always fires on success |
| XGROUP CREATECONSUMER | `xgroup-createconsumer` | | Always fires on success |
| XGROUP DELCONSUMER | `xgroup-delconsumer` | | Only if consumer existed |
| XGROUP DESTROY | `xgroup-destroy` | | Only if group existed |
| XGROUP SETID | `xgroup-setid` | | Always fires on success |

**Critical Implementation Notes**:
1. **XADD with trimming**: Fire `xadd` first, then `xtrim` if MAXLEN/MINID option used
2. **XDEL**: Fire even if deleting non-existent entries (as long as command succeeds)
3. **XTRIM**: Fire only if at least one entry actually removed
4. **XGROUP subcommands**: Each fires its own specific event (e.g., `xgroup-create`, not just `xgroup`)
5. **No del event**: Streams are never deleted by removal operations (unlike lists/sets/hashes/zsets)

---

## 3. Event Order Specifications

### 3.1 Commands with Multiple Events

**LMOVE / BLMOVE**:
```
Order guaranteed:
1. lpop or rpop event (on source key)
2. lpush or rpush event (on destination key)
3. del event (on source key if empty)

Example: LMOVE src dst LEFT RIGHT
  → __keyspace@0__:src "lpop"
  → __keyevent@0__:lpop "src"
  → __keyspace@0__:dst "rpush"
  → __keyevent@0__:rpush "dst"
  (if src empty after pop):
  → __keyspace@0__:src "del"
  → __keyevent@0__:del "src"
```

**RPOPLPUSH / BRPOPLPUSH**:
```
Order guaranteed:
1. rpop event (on source key)
2. lpush event (on destination key)
3. del event (on source key if empty)
```

**SMOVE**:
```
Order guaranteed:
1. srem event (on source key)
2. sadd event (on destination key)
3. del event (on source key if empty)
```

**XADD with MAXLEN/MINID**:
```
Order guaranteed:
1. xadd event
2. xtrim event (if trimming occurred)
```

### 3.2 Empty Collection Deletion

When a removal operation empties a collection, fire the operation event first, then `del`:

```
LPOP mylist (last element):
  → __keyspace@0__:mylist "lpop"
  → __keyevent@0__:lpop "mylist"
  → __keyspace@0__:mylist "del"
  → __keyevent@0__:del "mylist"
```

**Important**: The `del` event uses the **generic flag** (`g`), not the data type flag. Check both flags:
- List operation event requires `l` flag
- `del` event requires `g` flag
- If config is "Kl" (list only), no `del` event fires
- If config is "Klg" (list + generic), both fire

---

## 4. Edge Cases and Error Conditions

### 4.1 No Event When Command Doesn't Modify Key

**Rule**: If a command doesn't actually modify the target key, no event is generated.

**Examples**:
- `SADD myset member` (member already exists) → No event
- `SREM myset nonexistent` → No event
- `LPUSHX nonexistent value` → No event (key doesn't exist)
- `HDEL myhash nonexistent_field` → No event (field doesn't exist)
- `ZADD myzset NX 1 member` (member exists) → No event (NX prevents update)
- `LINSERT mylist BEFORE nonexistent value` → No event (pivot not found)

**Implementation**: Only call `publishNotification()` after verifying the operation actually modified data.

### 4.2 Multi-Key Operations

**LMOVE, RPOPLPUSH, SMOVE**: Fire events on **both** keys (source and destination).

**STORE operations** (SINTERSTORE, SUNIONSTORE, SDIFFSTORE, ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE): Fire event on **destination key only** (not on source keys).

### 4.3 Blocking Commands

**BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX, BLMPOP, BZMPOP**: Fire events only when they **successfully pop** (not when they block waiting).

**Implementation**: Publish notification after the blocking operation completes and returns data.

### 4.4 Empty Collection Deletion - Flag Interaction

When a collection becomes empty, the `del` event uses the **generic flag** (`g`), not the data type flag.

**Example scenarios**:

| Config | LPOP (last elem) | Events Fired |
|--------|------------------|--------------|
| "Kl" | List flag only | `lpop` only (no `del`) |
| "Kg" | Generic flag only | No events (list events disabled) |
| "Klg" | Both flags | `lpop` + `del` |
| "KEA" | All flags | `lpop` + `del` (all channels) |

**Implementation**:
```zig
// After successful LPOP
try publishListNotification(allocator, storage, pubsub, key, "lpop");

// If list is now empty
if (list_is_empty) {
    try publishGenericNotification(allocator, storage, pubsub, key, "del");
}
```

### 4.5 ZADD with INCR Option

**Special case**: `ZADD key INCR score member` fires `zincr` event, **not** `zadd`.

**Implementation**:
```zig
if (incr_option) {
    try publishSortedSetNotification(allocator, storage, pubsub, key, "zincr");
} else {
    try publishSortedSetNotification(allocator, storage, pubsub, key, "zadd");
}
```

### 4.6 Stream Commands Never Fire `del`

Unlike lists/sets/hashes/zsets, streams are **never deleted** by removal operations (XDEL, XTRIM). Even if all entries are deleted, the stream key itself remains with its consumer groups intact.

**No `del` event** for:
- XDEL (even if all entries deleted)
- XTRIM (even if all entries trimmed)

---

## 5. Implementation Pattern

### 5.1 Helper Functions

Add helper functions to each command file:

**src/commands/lists.zig**:
```zig
const notifications_mod = @import("../storage/notifications.zig");

/// Publish keyspace notification for a list command
fn publishListNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();

    // Check if list events are enabled
    if (!notifications_mod.shouldNotify(flags, .list)) return;

    // Publish to __keyspace@0__:<key> and __keyevent@0__:<event>
    try notifications_mod.publishNotification(
        allocator,
        pubsub_state,
        0, // DB index (always 0 for now)
        key,
        event,
        flags,
    );
}

/// Publish keyspace notification for a generic key command (e.g., del)
fn publishGenericNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();

    // Check if generic events are enabled
    if (!notifications_mod.shouldNotify(flags, .generic)) return;

    try notifications_mod.publishNotification(
        allocator,
        pubsub_state,
        0,
        key,
        event,
        flags,
    );
}
```

**src/commands/sets.zig**:
```zig
fn publishSetNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();
    if (!notifications_mod.shouldNotify(flags, .set)) return;
    try notifications_mod.publishNotification(allocator, pubsub_state, 0, key, event, flags);
}
```

**src/commands/hashes.zig**:
```zig
fn publishHashNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();
    if (!notifications_mod.shouldNotify(flags, .hash)) return;
    try notifications_mod.publishNotification(allocator, pubsub_state, 0, key, event, flags);
}
```

**src/commands/sorted_sets.zig**:
```zig
fn publishSortedSetNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();
    if (!notifications_mod.shouldNotify(flags, .sorted_set)) return;
    try notifications_mod.publishNotification(allocator, pubsub_state, 0, key, event, flags);
}
```

**src/commands/streams.zig**:
```zig
fn publishStreamNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();
    if (!notifications_mod.shouldNotify(flags, .stream)) return;
    try notifications_mod.publishNotification(allocator, pubsub_state, 0, key, event, flags);
}
```

### 5.2 Command Integration Examples

**LPUSH example**:
```zig
pub fn cmdLpush(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, pubsub_state: *PubSub) ![]const u8 {
    // ... existing argument parsing ...

    const key = /* extract key */;
    const elements = /* extract elements */;

    // Execute LPUSH
    const length = try storage.lpush(key, elements, null);

    // Publish notification
    try publishListNotification(allocator, storage, pubsub_state, key, "lpush");

    // ... write response ...
}
```

**LPOP with empty check example**:
```zig
pub fn cmdLpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, pubsub_state: *PubSub) ![]const u8 {
    // ... existing argument parsing ...

    const key = /* extract key */;
    const count = /* extract optional count */;

    // Check if list exists before pop
    const existed = storage.exists(key);

    // Execute LPOP
    const result = try storage.lpop(key, count);

    if (result != null and existed) {
        // Publish lpop event
        try publishListNotification(allocator, storage, pubsub_state, key, "lpop");

        // Check if list is now empty
        if (!storage.exists(key)) {
            // List was deleted, publish del event
            try publishGenericNotification(allocator, storage, pubsub_state, key, "del");
        }
    }

    // ... write response ...
}
```

**LMOVE multi-key example**:
```zig
pub fn cmdLmove(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, pubsub_state: *PubSub) ![]const u8 {
    // ... existing argument parsing ...

    const source = /* extract source key */;
    const destination = /* extract destination key */;
    const wherefrom = /* LEFT or RIGHT */;
    const whereto = /* LEFT or RIGHT */;

    // Check if source exists before move
    const source_existed = storage.exists(source);

    // Execute LMOVE
    const element = try storage.lmove(source, destination, wherefrom, whereto);

    if (element != null and source_existed) {
        // Publish source event (lpop or rpop)
        const source_event = if (wherefrom == .LEFT) "lpop" else "rpop";
        try publishListNotification(allocator, storage, pubsub_state, source, source_event);

        // Publish destination event (lpush or rpush)
        const dest_event = if (whereto == .LEFT) "lpush" else "rpush";
        try publishListNotification(allocator, storage, pubsub_state, destination, dest_event);

        // Check if source is now empty
        if (!storage.exists(source)) {
            try publishGenericNotification(allocator, storage, pubsub_state, source, "del");
        }
    }

    // ... write response ...
}
```

**ZADD with INCR option example**:
```zig
pub fn cmdZadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, pubsub_state: *PubSub) ![]const u8 {
    // ... existing argument parsing ...

    const key = /* extract key */;
    const incr_option = /* parse INCR flag */;

    // Execute ZADD
    const result = try storage.zadd(key, /* ... */);

    if (result.changed > 0) {
        // Choose event based on INCR option
        const event = if (incr_option) "zincr" else "zadd";
        try publishSortedSetNotification(allocator, storage, pubsub_state, key, event);
    }

    // ... write response ...
}
```

**XADD with trimming example**:
```zig
pub fn cmdXadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, pubsub_state: *PubSub) ![]const u8 {
    // ... existing argument parsing ...

    const key = /* extract key */;
    const maxlen_option = /* parse MAXLEN/MINID */;

    // Execute XADD
    const entry_id = try storage.xadd(key, /* ... */);

    // Publish xadd event
    try publishStreamNotification(allocator, storage, pubsub_state, key, "xadd");

    // If trimming occurred, publish xtrim event
    if (maxlen_option and trimmed_count > 0) {
        try publishStreamNotification(allocator, storage, pubsub_state, key, "xtrim");
    }

    // ... write response ...
}
```

### 5.3 Accessing PubSub State

**Critical**: Command handlers need access to `PubSub` state to publish notifications. This requires:

1. Passing `pubsub_state: *PubSub` parameter to command functions
2. Updating command dispatch in `src/server.zig` to pass pubsub state

**Current signature** (Iteration 245):
```zig
pub fn cmdLpush(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8
```

**New signature** (Iteration 246):
```zig
pub fn cmdLpush(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, pubsub_state: *PubSub) ![]const u8
```

**Note**: Check if `pubsub_state` is already being passed to commands. If Iteration 245 already updated the signatures for string commands, follow the same pattern.

---

## 6. Testing Requirements

### 6.1 Unit Tests

Add tests to each command file to verify notification behavior:

**tests/test_list_notifications.zig**:
```zig
test "LPUSH fires lpush event" {
    // CONFIG SET notify-keyspace-events "KEl"
    // SUBSCRIBE __keyspace@0__:mylist
    // LPUSH mylist value
    // Assert: received "lpush" message
}

test "LPOP on last element fires lpop + del events" {
    // CONFIG SET notify-keyspace-events "KElg"
    // LPUSH mylist value
    // SUBSCRIBE __keyspace@0__:mylist
    // LPOP mylist
    // Assert: received "lpop" then "del" messages
}

test "LPOP with 'Kl' flag does not fire del event" {
    // CONFIG SET notify-keyspace-events "Kl" (no 'g' flag)
    // LPUSH mylist value
    // SUBSCRIBE __keyspace@0__:mylist
    // LPOP mylist
    // Assert: received "lpop" only (no "del")
}

test "LMOVE fires events on both keys in order" {
    // CONFIG SET notify-keyspace-events "KEl"
    // LPUSH src value
    // SUBSCRIBE __keyspace@0__:src
    // SUBSCRIBE __keyspace@0__:dst
    // LMOVE src dst LEFT RIGHT
    // Assert: received "lpop" on src, then "rpush" on dst
}

test "LPUSHX on non-existent key fires no event" {
    // CONFIG SET notify-keyspace-events "KEl"
    // SUBSCRIBE __keyspace@0__:mylist
    // LPUSHX mylist value (key doesn't exist)
    // Assert: no messages received
}
```

**tests/test_set_notifications.zig**:
```zig
test "SADD fires sadd event" {
    // CONFIG SET notify-keyspace-events "KEs"
    // SUBSCRIBE __keyspace@0__:myset
    // SADD myset member
    // Assert: received "sadd" message
}

test "SPOP on last element fires spop + del events" {
    // CONFIG SET notify-keyspace-events "KEsg"
    // SADD myset member
    // SUBSCRIBE __keyspace@0__:myset
    // SPOP myset
    // Assert: received "spop" then "del" messages
}

test "SMOVE fires events on both keys" {
    // CONFIG SET notify-keyspace-events "KEs"
    // SADD src member
    // SUBSCRIBE __keyspace@0__:src
    // SUBSCRIBE __keyspace@0__:dst
    // SMOVE src dst member
    // Assert: received "srem" on src, then "sadd" on dst
}

test "SINTERSTORE fires event on destination only" {
    // CONFIG SET notify-keyspace-events "KEs"
    // SADD set1 a
    // SADD set2 a
    // SUBSCRIBE __keyspace@0__:dest
    // SINTERSTORE dest set1 set2
    // Assert: received "sinterstore" message on dest only
}
```

**tests/test_hash_notifications.zig**:
```zig
test "HSET fires hset event" {
    // CONFIG SET notify-keyspace-events "KEh"
    // SUBSCRIBE __keyspace@0__:myhash
    // HSET myhash field value
    // Assert: received "hset" message
}

test "HDEL on last field fires hdel + del events" {
    // CONFIG SET notify-keyspace-events "KEhg"
    // HSET myhash field value
    // SUBSCRIBE __keyspace@0__:myhash
    // HDEL myhash field
    // Assert: received "hdel" then "del" messages
}

test "HINCRBY fires hincrby event" {
    // CONFIG SET notify-keyspace-events "KEh"
    // SUBSCRIBE __keyspace@0__:myhash
    // HINCRBY myhash field 1
    // Assert: received "hincrby" message
}
```

**tests/test_sorted_set_notifications.zig**:
```zig
test "ZADD fires zadd event" {
    // CONFIG SET notify-keyspace-events "KEz"
    // SUBSCRIBE __keyspace@0__:myzset
    // ZADD myzset 1 member
    // Assert: received "zadd" message
}

test "ZADD with INCR fires zincr event (not zadd)" {
    // CONFIG SET notify-keyspace-events "KEz"
    // SUBSCRIBE __keyspace@0__:myzset
    // ZADD myzset INCR 1 member
    // Assert: received "zincr" message (not "zadd")
}

test "ZREM on last member fires zrem + del events" {
    // CONFIG SET notify-keyspace-events "KEzg"
    // ZADD myzset 1 member
    // SUBSCRIBE __keyspace@0__:myzset
    // ZREM myzset member
    // Assert: received "zrem" then "del" messages
}

test "ZINTERSTORE fires event on destination only" {
    // CONFIG SET notify-keyspace-events "KEz"
    // ZADD zset1 1 a
    // ZADD zset2 1 a
    // SUBSCRIBE __keyspace@0__:dest
    // ZINTERSTORE dest 2 zset1 zset2
    // Assert: received "zinterstore" message on dest only
}
```

**tests/test_stream_notifications.zig**:
```zig
test "XADD fires xadd event" {
    // CONFIG SET notify-keyspace-events "KEt"
    // SUBSCRIBE __keyspace@0__:mystream
    // XADD mystream * field value
    // Assert: received "xadd" message
}

test "XADD with MAXLEN fires xadd + xtrim events" {
    // CONFIG SET notify-keyspace-events "KEt"
    // XADD mystream * f1 v1
    // XADD mystream * f2 v2
    // SUBSCRIBE __keyspace@0__:mystream
    // XADD mystream MAXLEN 1 * f3 v3
    // Assert: received "xadd" then "xtrim" messages
}

test "XDEL fires xdel event" {
    // CONFIG SET notify-keyspace-events "KEt"
    // XADD mystream * field value (returns entry-id)
    // SUBSCRIBE __keyspace@0__:mystream
    // XDEL mystream entry-id
    // Assert: received "xdel" message
}

test "XGROUP CREATE fires xgroup-create event" {
    // CONFIG SET notify-keyspace-events "KEt"
    // XADD mystream * field value
    // SUBSCRIBE __keyspace@0__:mystream
    // XGROUP CREATE mystream mygroup $
    // Assert: received "xgroup-create" message
}

test "XDEL all entries does NOT fire del event" {
    // CONFIG SET notify-keyspace-events "KEtg"
    // XADD mystream * field value (returns entry-id)
    // SUBSCRIBE __keyspace@0__:mystream
    // XDEL mystream entry-id
    // Assert: received "xdel" only (no "del" - stream still exists)
}
```

### 6.2 Integration Tests

Create comprehensive integration test suite:

**tests/test_keyspace_notifications_integration.zig**:
```zig
test "All data type flags work together" {
    // CONFIG SET notify-keyspace-events "KEA" (all flags)
    // Test list, set, hash, zset, stream commands
    // Verify all events fire correctly
}

test "Selective flag filtering works" {
    // CONFIG SET notify-keyspace-events "KEl" (list only)
    // LPUSH fires events
    // SADD does NOT fire events
}

test "Event ordering for multi-key commands" {
    // Test LMOVE, RPOPLPUSH, SMOVE
    // Verify source event always before destination event
}

test "Empty collection deletion with flag interaction" {
    // Test with "Kl" vs "Klg" configs
    // Verify del event only fires when 'g' flag present
}

test "Pattern subscriptions work" {
    // PSUBSCRIBE __key*__:*
    // Test various commands
    // Verify all events received via pattern
}
```

### 6.3 Redis Compatibility Tests

Create Python script to compare against real Redis:

**tests/redis_compat_datastructure_notifications.py**:
```python
#!/usr/bin/env python3
"""
Redis compatibility test: Data structure notifications byte-by-byte comparison.
"""
import redis
import subprocess
import time
import threading

def test_list_notifications(port):
    """Test list command notifications."""
    r = redis.Redis(host='127.0.0.1', port=port, decode_responses=True)
    r.config_set('notify-keyspace-events', 'KElg')

    pubsub = r.pubsub()
    pubsub.subscribe('__keyspace@0__:mylist')

    # Wait for subscription
    msg = pubsub.get_message(timeout=1)
    assert msg['type'] == 'subscribe'

    # Execute LPUSH
    def execute():
        time.sleep(0.1)
        r.lpush('mylist', 'value')

    t = threading.Thread(target=execute)
    t.start()

    # Receive notification
    msg = pubsub.get_message(timeout=2)
    t.join()

    assert msg is not None
    assert msg['type'] == 'message'
    assert msg['channel'] == '__keyspace@0__:mylist'
    assert msg['data'] == 'lpush'

    print(f"✓ List notifications work on port {port}")

def test_empty_list_deletion(port):
    """Test lpop on last element fires lpop + del."""
    r = redis.Redis(host='127.0.0.1', port=port, decode_responses=True)
    r.config_set('notify-keyspace-events', 'KElg')

    # Setup
    r.lpush('mylist', 'value')

    pubsub = r.pubsub()
    pubsub.subscribe('__keyspace@0__:mylist')

    # Wait for subscription
    msg = pubsub.get_message(timeout=1)

    # Execute LPOP
    def execute():
        time.sleep(0.1)
        r.lpop('mylist')

    t = threading.Thread(target=execute)
    t.start()

    # Receive notifications
    msg1 = pubsub.get_message(timeout=2)
    msg2 = pubsub.get_message(timeout=2)
    t.join()

    assert msg1['data'] == 'lpop'
    assert msg2['data'] == 'del'

    print(f"✓ Empty list deletion works on port {port}")

# Add similar tests for sets, hashes, zsets, streams...

if __name__ == '__main__':
    # Test against Redis
    redis_proc = subprocess.Popen(['redis-server', '--port', '6379'])
    time.sleep(1)

    # Test against Zoltraak
    zoltraak_proc = subprocess.Popen(['./zig-out/bin/zoltraak', '--port', '6380'])
    time.sleep(1)

    try:
        print("Testing Redis...")
        test_list_notifications(6379)
        test_empty_list_deletion(6379)

        print("\nTesting Zoltraak...")
        test_list_notifications(6380)
        test_empty_list_deletion(6380)

        print("\n✅ All compatibility tests passed!")

    finally:
        redis_proc.terminate()
        zoltraak_proc.terminate()
        redis_proc.wait()
        zoltraak_proc.wait()
```

---

## 7. Implementation Checklist

### Phase 1: Helper Functions
- [ ] Add `publishListNotification()` to `src/commands/lists.zig`
- [ ] Add `publishSetNotification()` to `src/commands/sets.zig`
- [ ] Add `publishHashNotification()` to `src/commands/hashes.zig`
- [ ] Add `publishSortedSetNotification()` to `src/commands/sorted_sets.zig`
- [ ] Add `publishStreamNotification()` to `src/commands/streams.zig`
- [ ] Add `publishGenericNotification()` to each file (for `del` events)

### Phase 2: List Commands (src/commands/lists.zig)
- [ ] cmdLpush() → publish `"lpush"` event
- [ ] cmdLpushx() → publish `"lpush"` event (if successful)
- [ ] cmdRpush() → publish `"rpush"` event
- [ ] cmdRpushx() → publish `"rpush"` event (if successful)
- [ ] cmdLpop() → publish `"lpop"` event + `"del"` if empty
- [ ] cmdRpop() → publish `"rpop"` event + `"del"` if empty
- [ ] cmdLinsert() → publish `"linsert"` event (if successful)
- [ ] cmdLset() → publish `"lset"` event
- [ ] cmdLrem() → publish `"lrem"` event + `"del"` if empty
- [ ] cmdLtrim() → publish `"ltrim"` event + `"del"` if empty
- [ ] cmdLmove() → publish source + dest events + `"del"` if source empty
- [ ] cmdBlmove() → same as LMOVE
- [ ] cmdRpoplpush() → publish `"rpop"` + `"lpush"` + `"del"` if source empty
- [ ] cmdBlpop() → publish `"lpop"` + `"del"` if empty (on success)
- [ ] cmdBrpop() → publish `"rpop"` + `"del"` if empty (on success)
- [ ] cmdLmpop() → publish `"lpop"`/`"rpop"` + `"del"` if empty
- [ ] cmdBlmpop() → same as LMPOP

### Phase 3: Set Commands (src/commands/sets.zig)
- [ ] cmdSadd() → publish `"sadd"` event (if added)
- [ ] cmdSrem() → publish `"srem"` event + `"del"` if empty
- [ ] cmdSpop() → publish `"spop"` event + `"del"` if empty
- [ ] cmdSmove() → publish `"srem"` + `"sadd"` + `"del"` if source empty
- [ ] cmdSinterstore() → publish `"sinterstore"` event (+ `"del"` if result empty)
- [ ] cmdSunionstore() → publish `"sunionstore"` event (+ `"del"` if result empty)
- [ ] cmdSdiffstore() → publish `"sdiffstore"` event (+ `"del"` if result empty)

### Phase 4: Hash Commands (src/commands/hashes.zig)
- [ ] cmdHset() → publish `"hset"` event (if modified)
- [ ] cmdHsetnx() → publish `"hset"` event (if successful)
- [ ] cmdHmset() → publish `"hset"` event (if modified)
- [ ] cmdHdel() → publish `"hdel"` event + `"del"` if empty
- [ ] cmdHincrby() → publish `"hincrby"` event
- [ ] cmdHincrbyfloat() → publish `"hincrbyfloat"` event

### Phase 5: Sorted Set Commands (src/commands/sorted_sets.zig)
- [ ] cmdZadd() → publish `"zadd"` or `"zincr"` event (check INCR option)
- [ ] cmdZrem() → publish `"zrem"` event + `"del"` if empty
- [ ] cmdZincrby() → publish `"zincr"` event
- [ ] cmdZpopmin() → publish `"zpopmin"` event + `"del"` if empty
- [ ] cmdZpopmax() → publish `"zpopmax"` event + `"del"` if empty
- [ ] cmdBzpopmin() → same as ZPOPMIN (on success)
- [ ] cmdBzpopmax() → same as ZPOPMAX (on success)
- [ ] cmdZmpop() → publish `"zpopmin"`/`"zpopmax"` + `"del"` if empty
- [ ] cmdBzmpop() → same as ZMPOP
- [ ] cmdZremrangebyrank() → publish `"zremrangebyrank"` + `"del"` if empty
- [ ] cmdZremrangebyscore() → publish `"zremrangebyscore"` + `"del"` if empty
- [ ] cmdZremrangebylex() → publish `"zremrangebylex"` + `"del"` if empty
- [ ] cmdZinterstore() → publish `"zinterstore"` (+ `"del"` if result empty)
- [ ] cmdZunionstore() → publish `"zunionstore"` (+ `"del"` if result empty)
- [ ] cmdZdiffstore() → publish `"zdiffstore"` (+ `"del"` if result empty)

### Phase 6: Stream Commands (src/commands/streams.zig)
- [ ] cmdXadd() → publish `"xadd"` + `"xtrim"` if trimming
- [ ] cmdXdel() → publish `"xdel"` event
- [ ] cmdXtrim() → publish `"xtrim"` event (if removed entries)
- [ ] cmdXsetid() → publish `"xsetid"` event
- [ ] cmdXgroupCreate() → publish `"xgroup-create"` event
- [ ] cmdXgroupCreateconsumer() → publish `"xgroup-createconsumer"` event
- [ ] cmdXgroupDelconsumer() → publish `"xgroup-delconsumer"` event (if existed)
- [ ] cmdXgroupDestroy() → publish `"xgroup-destroy"` event (if existed)
- [ ] cmdXgroupSetid() → publish `"xgroup-setid"` event

### Phase 7: Integration Tests
- [ ] Create `tests/test_list_notifications.zig`
- [ ] Create `tests/test_set_notifications.zig`
- [ ] Create `tests/test_hash_notifications.zig`
- [ ] Create `tests/test_sorted_set_notifications.zig`
- [ ] Create `tests/test_stream_notifications.zig`
- [ ] Create `tests/test_keyspace_notifications_integration.zig`

### Phase 8: Python Compatibility Test
- [ ] Create `tests/redis_compat_datastructure_notifications.py`
- [ ] Test list notifications
- [ ] Test set notifications
- [ ] Test hash notifications
- [ ] Test sorted set notifications
- [ ] Test stream notifications
- [ ] Test multi-key operation event ordering
- [ ] Test empty collection deletion
- [ ] Test flag filtering

### Phase 9: Documentation
- [ ] Update README.md with notification support for all data types
- [ ] Update CLAUDE.md iteration count
- [ ] Update docs/milestones.md

---

## 8. Success Criteria

1. **All integration tests pass** (`zig build test`)
2. **Python compatibility test passes** (byte-by-byte RESP match with Redis)
3. **No memory leaks** (`zig build test` with allocator leak detection)
4. **All data type flags work** (`l`, `s`, `h`, `z`, `t`)
5. **Event ordering correct** (LMOVE, RPOPLPUSH, SMOVE, etc.)
6. **Empty collection deletion works** (fires `del` with `g` flag)
7. **Multi-key operations correct** (events on both keys)
8. **Failed operations produce no events** (LPUSHX on non-existent key, etc.)
9. **ZADD with INCR fires `zincr` not `zadd`**
10. **Stream commands never fire `del` event**
11. **Flag filtering works** (selective enabling of data types)
12. **Pattern subscriptions work** (PSUBSCRIBE __key*__:*)

---

## 9. Performance Considerations

### 9.1 Minimize Allocations
- Check `notification_flags` early and return if `0`
- Use stack-allocated buffers for event names (short strings)
- Reuse allocator for temporary channel name formatting

### 9.2 Atomicity
- `notification_flags` is atomic (`std.atomic.Value(u16)`)
- No locks needed for reading flags
- CONFIG SET updates flags atomically

### 9.3 Pub/Sub Overhead
- Only subscribers to notification channels receive messages
- Fire-and-forget: no retries if no subscribers
- Messages dropped if subscriber queue full (max 1024 per subscriber)

---

## 10. Future Iterations

Out of scope for Iteration 246 (deferred to future):

1. **Expiration events** (`x` flag):
   - Integrate `"expired"` event into active/lazy expiration logic
   - Modify `src/storage/memory.zig` expiration code

2. **Eviction events** (`e` flag):
   - Fire `"evicted"` when keys evicted for maxmemory
   - Integrate with eviction policy

3. **Key miss events** (`m` flag):
   - Fire `"miss"` when accessing non-existent key
   - Add to `storage.get()` and similar methods

4. **Special events** (`n`, `o`, `c` flags):
   - `"new"` - when new key created
   - `"overwritten"` - when key overwritten
   - `"type_changed"` - when key type changes

5. **Module events** (`d` flag):
   - Module-specific key type events
   - Phase 17 (modules)

---

## 11. References

- **Redis keyspace notifications**: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- **List commands**: https://redis.io/commands/?group=list
- **Set commands**: https://redis.io/commands/?group=set
- **Hash commands**: https://redis.io/commands/?group=hash
- **Sorted set commands**: https://redis.io/commands/?group=sorted-set
- **Stream commands**: https://redis.io/commands/?group=stream
- **Zoltraak notifications module**: `/Users/fn/codespace/zoltraak/src/storage/notifications.zig`
- **Zoltraak pubsub module**: `/Users/fn/codespace/zoltraak/src/storage/pubsub.zig`
- **Iteration 245 spec**: `/Users/fn/codespace/zoltraak/.claude/iteration-245-keyspace-notifications-spec.md`

---

**End of Specification**
