# Redis Keyspace Notifications - Complete Specification for Zoltraak

**Status**: Implementation in progress (basic structure exists in `src/storage/notifications.zig`)
**Target**: Full Redis 7.x+ compatibility
**References**:
- [Redis Keyspace Notifications Documentation](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)
- [Redis notify-keyspace-events config](https://redis.io/docs/latest/develop/reference/keyspace-notifications/)
- [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)

---

## 1. Overview

Keyspace notifications allow clients to subscribe to Pub/Sub channels to receive events affecting the Redis dataset in real-time. Two distinct types of events are generated for each operation:

1. **Keyspace events**: Published to `__keyspace@<db>__:<key>` with event type as message
2. **Keyevent events**: Published to `__keyevent@<db>__:<event>` with key name as message

**Example**: `DEL mykey` in database 0 generates:
```
PUBLISH __keyspace@0__:mykey del
PUBLISH __keyevent@0__:del mykey
```

**Default state**: **DISABLED** (requires explicit configuration due to CPU overhead)

---

## 2. All Event Type Flags

### 2.1 Core Flags (Required)

| Flag | Name | Description |
|------|------|-------------|
| `K` | Keyspace | Enable keyspace events (`__keyspace@<db>__:<key>`) |
| `E` | Keyevent | Enable keyevent events (`__keyevent@<db>__:<event>`) |

**At least one of K or E must be present** to enable any notifications.

### 2.2 Data Type Command Flags

| Flag | Name | Description | Commands Covered |
|------|------|-------------|------------------|
| `g` | Generic | Generic commands (type-independent) | DEL, UNLINK, RENAME, RENAMENX, MOVE, COPY, TOUCH, RESTORE, MIGRATE, PERSIST, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, EXPIRETIME, PEXPIRETIME, TTL, PTTL |
| `$` | String | String commands | SET, SETNX, SETEX, PSETEX, MSET, MSETNX, APPEND, SETRANGE, GETSET, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, GETDEL, GETEX, MSETEX, DELEX, DIGEST |
| `l` | List | List commands | LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LINSERT, LSET, LTRIM, LREM, LMOVE, RPOPLPUSH, BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP, LMPOP |
| `s` | Set | Set commands | SADD, SREM, SPOP, SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE |
| `h` | Hash | Hash commands | HSET, HSETNX, HMSET, HDEL, HINCRBY, HINCRBYFLOAT, HGETDEL, HGETEX, HSETEX |
| `z` | Sorted Set | Sorted set commands | ZADD, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX, BZMPOP, ZMPOP, ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE, ZRANGESTORE |
| `t` | Stream | Stream commands | XADD, XDEL, XTRIM, XSETID, XGROUP CREATE, XGROUP DESTROY, XACKDEL, XDELEX, XCFGSET |
| `d` | Module | Module key type events | Module-defined events (via RedisModule_NotifyKeyspaceEvent) |

### 2.3 Special Event Flags

| Flag | Name | Description | Trigger Condition |
|------|------|-------------|-------------------|
| `x` | Expired | Key expired (TTL reached) | Background or foreground expiration of keys with TTL |
| `e` | Evicted | Key evicted (maxmemory) | Key removed by eviction policy due to memory limit |
| `m` | Missed | Key miss | Read operation on non-existent key (NOT included in `A`) |
| `n` | New | New key created | First SET on previously non-existent key (NOT included in `A`) |
| `o` | Overwritten | Key overwritten | SET on existing key (NOT included in `A`) |
| `c` | Type-changed | Key type changed | Key changes from one data type to another (NOT included in `A`) |

**IMPORTANT**: Flags `m`, `n`, `o`, `c` are **NOT** included in the `A` alias and must be explicitly enabled.

### 2.4 Alias Flag

| Flag | Name | Expansion | Description |
|------|------|-----------|-------------|
| `A` | All | `g$lshztdxe` | All command events + expired + evicted (excludes `m`, `n`, `o`, `c`) |

---

## 3. Channel Naming Patterns

### 3.1 Keyspace Channel Format

```
__keyspace@<db>__:<key>
```

**Purpose**: Notify which events affected a specific key
**Message**: Event type (e.g., `set`, `del`, `expire`)
**Example**:
```redis
SUBSCRIBE __keyspace@0__:mykey
# Receives messages: "set", "del", "expired", etc.
```

### 3.2 Keyevent Channel Format

```
__keyevent@<db>__:<event>
```

**Purpose**: Notify which keys were affected by a specific event type
**Message**: Key name
**Example**:
```redis
SUBSCRIBE __keyevent@0__:expired
# Receives messages: "session:123", "cache:abc", etc.
```

### 3.3 Database Index

- `<db>` is the database index (0-15 by default, 0-16383 max)
- Single-database mode (Zoltraak current state): always `0`
- Multi-database mode: each DB generates events independently

---

## 4. Configuration Parameter: notify-keyspace-events

### 4.1 Syntax

```redis
CONFIG GET notify-keyspace-events
CONFIG SET notify-keyspace-events "<flags>"
CONFIG REWRITE  # Persist to config file (future feature)
```

**Valid flag strings**:
```
""          # Disabled (default)
"KEA"       # All events (keyspace + keyevent + all command types)
"Kgx"       # Keyspace + generic commands + expired
"E$l"       # Keyevent + string commands + list commands
"KElshz"    # Both channels + lists + sets + hashes + sorted sets
"AKE"       # Same as "KEA" (order doesn't matter)
"KExmn"     # Keyspace + expired + missed + new key events
```

### 4.2 Validation Rules

1. **Case-sensitive**: `K` != `k`, `E` != `e`
2. **Order-independent**: `KEA` = `AKE` = `EAK`
3. **At least one of K or E**: `"g$"` alone is invalid (no effect)
4. **Duplicate flags ignored**: `"KKKEA"` = `"KEA"`
5. **Unknown flags ignored**: `"KEZ"` = `"KE"` (Z is ignored)
6. **Empty string valid**: `""` disables all notifications

### 4.3 Implementation Notes

- Store as bitfield (u16 sufficient for all flags)
- Parse on CONFIG SET and store in `Storage.notification_flags`
- Re-parse on CONFIG GET to return canonical string representation
- No runtime penalty when disabled (flags == 0)

---

## 5. Command Event Mappings

### 5.1 Generic Commands (flag `g`)

| Command | Event | Notes |
|---------|-------|-------|
| DEL | `del` | Fired per key deleted |
| UNLINK | `del` | Same event as DEL (async deletion) |
| RENAME | `rename_from` (old key), `rename_to` (new key) | Two events generated |
| RENAMENX | `rename_from`, `rename_to` | Only if successful |
| EXPIRE | `expire` | Sets TTL |
| PEXPIRE | `expire` | Same event (milliseconds) |
| EXPIREAT | `expire` | Unix timestamp variant |
| PEXPIREAT | `expire` | Unix timestamp (ms) variant |
| PERSIST | `persist` | Removes TTL |
| MOVE | `move_from` (source DB), `move_to` (dest DB) | Cross-DB move |
| COPY | `copy_to` (dest key) | Source key not notified (no change) |
| TOUCH | `touch` | LRU/LFU update |
| RESTORE | `restore` | Deserialize from RDB |
| MIGRATE | Similar to MOVE | Cross-instance move |

### 5.2 String Commands (flag `$`)

| Command | Event | Notes |
|---------|-------|-------|
| SET | `set` | Always fired |
| SETNX | `set` | Only if key didn't exist |
| SETEX | `set` + `expire` | Two events |
| PSETEX | `set` + `expire` | Two events |
| MSET | `set` | One event per key |
| MSETNX | `set` | One event per key (if successful) |
| APPEND | `append` | Existing key modified |
| SETRANGE | `setrange` | Partial overwrite |
| GETSET | `set` | Deprecated (use SET GET) |
| GETDEL | `getdel` + `del` | Two events (Redis 8.0+) |
| GETEX | `getex` | Get with TTL update |
| INCR, INCRBY, INCRBYFLOAT | `incrby` | Numeric operation |
| DECR, DECRBY | `decrby` | Numeric operation |
| MSETEX | `set` + `expire` | Per-key (Redis 8.4+) |
| DELEX | `del` | Conditional delete (Redis 8.4+) |
| DIGEST | `digest` | Key digest (Redis 8.4+) |

### 5.3 List Commands (flag `l`)

| Command | Event |
|---------|-------|
| LPUSH, LPUSHX | `lpush` |
| RPUSH, RPUSHX | `rpush` |
| LPOP | `lpop` |
| RPOP | `rpop` |
| LINSERT | `linsert` |
| LSET | `lset` |
| LTRIM | `ltrim` |
| LREM | `lrem` |
| LMOVE | `lmove` |
| RPOPLPUSH | `rpop` (source), `lpush` (dest) |
| BLPOP, BRPOP | Same as LPOP/RPOP when unblocked |
| BLMOVE | Same as LMOVE when unblocked |
| BLMPOP, LMPOP | Same as LPOP/RPOP |

### 5.4 Set Commands (flag `s`)

| Command | Event |
|---------|-------|
| SADD | `sadd` |
| SREM | `srem` |
| SPOP | `spop` |
| SMOVE | `srem` (source), `sadd` (dest) |
| SINTERSTORE | `sinterstore` (dest key) |
| SUNIONSTORE | `sunionstore` (dest key) |
| SDIFFSTORE | `sdiffstore` (dest key) |

### 5.5 Hash Commands (flag `h`)

| Command | Event |
|---------|-------|
| HSET | `hset` |
| HSETNX | `hset` (only if field new) |
| HMSET | `hset` |
| HDEL | `hdel` |
| HINCRBY, HINCRBYFLOAT | `hincrby` |
| HGETDEL | `hdel` (Redis 8.0+) |
| HGETEX | `hgetex` (Redis 8.0+) |
| HSETEX | `hset` + `expire` (Redis 8.0+) |

### 5.6 Sorted Set Commands (flag `z`)

| Command | Event |
|---------|-------|
| ZADD | `zadd` |
| ZREM | `zrem` |
| ZINCRBY | `zincrby` |
| ZPOPMIN, ZPOPMAX | `zpopmin`, `zpopmax` |
| BZPOPMIN, BZPOPMAX | Same when unblocked |
| BZMPOP, ZMPOP | Same as ZPOPMIN/ZPOPMAX |
| ZINTERSTORE | `zinterstore` (dest) |
| ZUNIONSTORE | `zunionstore` (dest) |
| ZDIFFSTORE | `zdiffstore` (dest) |
| ZRANGESTORE | `zrangestore` (dest) |

### 5.7 Stream Commands (flag `t`)

| Command | Event |
|---------|-------|
| XADD | `xadd` |
| XDEL | `xdel` |
| XTRIM | `xtrim` |
| XSETID | `xsetid` |
| XGROUP CREATE | `xgroup-create` |
| XGROUP DESTROY | `xgroup-destroy` |
| XACKDEL | `xdel` (Redis 8.2+) |
| XDELEX | `xdel` (Redis 8.2+) |
| XCFGSET | `xcfgset` (Redis 8.6+) |

### 5.8 Special Events

| Event | Flag | Trigger | Notes |
|-------|------|---------|-------|
| `expired` | `x` | Key TTL reaches 0 | Background or lazy expiration |
| `evicted` | `e` | Maxmemory eviction policy | LRU/LFU/random eviction |
| `miss` | `m` | GET on non-existent key | Rarely used (high volume) |
| `new` | `n` | First SET of new key | Key didn't exist before |
| `overwrite` | `o` | SET on existing key | Key already existed |
| `typechange` | `c` | Key changes type | e.g., string → list |

---

## 6. Edge Cases and Special Behaviors

### 6.1 Expired Keys

**Scenario**: Key with TTL expires naturally
**Event**: `expired`
**Channel**: `__keyevent@<db>__:expired` (message: key name)
**Timing**:
- **Active expiration**: Background timer scans expired keys → immediate notification
- **Lazy expiration**: Key accessed after expiry → notification before returning NIL
- **No guarantee of immediate notification** (depends on Redis scheduling)

**Implementation notes**:
- Check TTL in `Storage.get()` and fire `expired` event before returning `null`
- Background expiration loop should fire events for batch-expired keys
- Event fires **before** key is deleted from storage

### 6.2 Evicted Keys

**Scenario**: Key evicted due to maxmemory policy
**Event**: `evicted`
**Channel**: `__keyevent@<db>__:evicted` (message: key name)
**Timing**: Immediately before key is removed from storage
**Policy dependency**:
- `noeviction`: No evictions → no events
- `allkeys-lru`, `volatile-lru`, etc.: Events for each evicted key

### 6.3 Blocking Commands

**Scenario**: `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`, `XREAD BLOCK`, `XREADGROUP BLOCK`
**Behavior**: Event fires **when the command actually executes** (unblocks), not when it blocks
**Example**:
```redis
# Client A
BLPOP mylist 0

# Client B (triggers event)
LPUSH mylist "value"
# → fires "lpop" event for mylist (Client A's BLPOP consumes it)
```

### 6.4 RENAME and RENAMENX

**Special case**: Two events generated
**Example**:
```redis
RENAME oldkey newkey
# → __keyevent@0__:rename_from → "oldkey"
# → __keyevent@0__:rename_to → "newkey"
```

**RENAMENX**: Only fires if successful (newkey didn't exist)

### 6.5 COPY

**Special case**: Only destination key fires event (source unchanged)
**Example**:
```redis
COPY src dst
# → __keyspace@0__:dst "copy_to" (only dst notified)
```

### 6.6 Module Events (flag `d`)

**Scenario**: Custom Redis module calls `RedisModule_NotifyKeyspaceEvent()`
**Behavior**: Module-defined event names (arbitrary strings)
**Zoltraak**: Not yet implemented (Phase 17 - Modules API)

### 6.7 New Key vs. Overwrite vs. Type Change

**Flag combinations**:
```redis
CONFIG SET notify-keyspace-events "KE$mn"
# $ = string commands
# m = miss events
# n = new key events

# First SET
SET newkey "value"
# → fires "set" + "new"

# Second SET (overwrite)
SET newkey "value2"
# → fires "set" only (no "new", no "overwrite" because "o" not enabled)
```

**Type change**:
```redis
CONFIG SET notify-keyspace-events "KEc"

SET mykey "string"
# → fires "set" + "new"

DEL mykey
LPUSH mykey "item"
# → fires "lpush" + "typechange" (string → list)
```

### 6.8 Multi-key Commands

**Behavior**: One event per key affected
**Examples**:
- `MSET key1 val1 key2 val2` → two `set` events
- `DEL key1 key2 key3` → three `del` events
- `SUNIONSTORE dest set1 set2` → one `sunionstore` event (only dest)

---

## 7. Performance Considerations

### 7.1 CPU Overhead

- **Disabled (default)**: Zero overhead (flag check is single bitwise AND)
- **Enabled**: ~1-5% CPU overhead depending on workload and flags
- **High-write workloads**: Can reach 10%+ overhead with `KEA` (all events)

**Recommendations**:
- Use selective flags in production: `Kgx` (generic + expired) instead of `KEA`
- Disable notification types not needed: if no string commands interest you, omit `$`
- Benchmark before enabling in latency-sensitive applications

### 7.2 Memory Overhead

- **Pub/Sub buffer**: Each notification message is queued in subscriber buffers
- **High-volume events**: `m` (miss) flag can generate massive traffic
- **Recommendation**: Never enable `m` in production without careful testing

### 7.3 Filtering Strategies

**Client-side filtering**:
```redis
# Subscribe to specific keys
PSUBSCRIBE __keyspace@0__:session:*

# Subscribe to specific events
PSUBSCRIBE __keyevent@0__:expired
```

**Server-side filtering** (via CONFIG):
```redis
# Only expired keys
CONFIG SET notify-keyspace-events "Ex"

# Only list and set operations
CONFIG SET notify-keyspace-events "KEls"
```

---

## 8. RESP Protocol Details

### 8.1 RESP2 (Pub/Sub)

**Notification format**: Standard pub/sub message
**Example**:
```
*3
$7
message
$22
__keyspace@0__:mykey
$3
set
```

**Client workflow**:
1. `SUBSCRIBE __keyspace@0__:mykey`
2. Receive standard pub/sub messages
3. Parse message as `[channel, event]`

### 8.2 RESP3 (Push Messages)

**Notification format**: Push message (server-initiated)
**Example**:
```
>3
$7
message
$22
__keyspace@0__:mykey
$3
set
```

**Advantages**:
- Server can push notifications without explicit SUBSCRIBE mode
- Client can mix regular commands and notifications
- More efficient for modern clients

**Zoltraak support**: RESP3 protocol exists, push notifications integration pending

---

## 9. Implementation Architecture for Zoltraak

### 9.1 Current State (Iteration 243)

**Implemented**:
- `src/storage/notifications.zig` - Basic structure exists
  - `NotificationFlag` enum (all flags defined)
  - `parseNotificationFlags()` - Config string parser
  - `shouldNotify()` - Flag check helper
  - `publishNotification()` - Pub/sub integration
- `src/storage/config.zig` - `notify-keyspace-events` parameter stored
- `src/storage/pubsub.zig` - Pub/sub infrastructure complete

**Missing**:
- ❌ Event dispatch in command handlers (no commands fire notifications yet)
- ❌ Expired key event generation
- ❌ Evicted key event generation
- ❌ `n`, `o`, `c`, `m` flag implementations (new/overwrite/typechange/miss)
- ❌ Integration tests for notification delivery

### 9.2 Required Changes

#### Step 1: Storage-level hooks

**File**: `src/storage/memory.zig`

Add notification dispatch to:
- `set()` - Fire `set` event (check for `n`, `o` flags)
- `del()` - Fire `del` event
- `expire()` - Fire `expire` event
- `get()` - Check TTL expiry → fire `expired` event
- Background expiration loop → fire `expired` events

**Signature**:
```zig
pub fn set(self: *MemoryStorage, key: []const u8, value: Value) !void {
    const existed = self.data.contains(key);

    // ... existing set logic ...

    // Fire notifications
    if (self.notification_flags != 0) {
        const event = "set";
        const type_flag = NotificationFlag.string; // or .list, .hash, etc.

        if (shouldNotify(self.notification_flags, type_flag)) {
            try publishNotification(
                self.allocator,
                self.pubsub,
                0, // db_index
                key,
                event,
                self.notification_flags,
            );
        }

        // Fire "new" event if key didn't exist
        if (!existed and shouldNotify(self.notification_flags, .new)) {
            try publishNotification(
                self.allocator,
                self.pubsub,
                0,
                key,
                "new",
                self.notification_flags,
            );
        }
    }
}
```

#### Step 2: Command handler integration

**Files**: All command files in `src/commands/*.zig`

Pattern for each command:
```zig
pub fn cmdSet(allocator: std.mem.Allocator, storage: *MemoryStorage, args: []RespValue) ![]u8 {
    // ... parse args ...

    // Execute storage operation (this fires notification internally)
    try storage.set(key, value);

    // ... return response ...
}
```

**No manual notification calls** in command handlers (centralized in storage layer).

#### Step 3: Expiration event dispatch

**File**: `src/storage/memory.zig`

**Active expiration** (background loop):
```zig
fn expirationLoop(self: *MemoryStorage) void {
    while (true) {
        std.time.sleep(100_000_000); // 100ms

        const now = std.time.milliTimestamp();
        var expired_keys = std.ArrayList([]const u8).init(self.allocator);
        defer expired_keys.deinit();

        // Collect expired keys
        var it = self.expirations.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* <= now) {
                try expired_keys.append(entry.key_ptr.*);
            }
        }

        // Fire events and delete
        for (expired_keys.items) |key| {
            if (shouldNotify(self.notification_flags, .expired)) {
                try publishNotification(
                    self.allocator,
                    self.pubsub,
                    0,
                    key,
                    "expired",
                    self.notification_flags,
                );
            }
            _ = self.del(key);
        }
    }
}
```

**Lazy expiration** (in `get()`):
```zig
pub fn get(self: *MemoryStorage, key: []const u8) ?Value {
    if (self.isExpired(key)) {
        // Fire expired event before deletion
        if (shouldNotify(self.notification_flags, .expired)) {
            publishNotification(
                self.allocator,
                self.pubsub,
                0,
                key,
                "expired",
                self.notification_flags,
            ) catch {};
        }
        _ = self.del(key);
        return null;
    }
    return self.data.get(key);
}
```

#### Step 4: Eviction event dispatch

**File**: `src/storage/eviction.zig`

```zig
pub fn evictKey(storage: *MemoryStorage, key: []const u8) !void {
    // Fire evicted event before deletion
    if (shouldNotify(storage.notification_flags, .evicted)) {
        try publishNotification(
            storage.allocator,
            storage.pubsub,
            0,
            key,
            "evicted",
            storage.notification_flags,
        );
    }
    _ = storage.del(key);
}
```

#### Step 5: CONFIG SET integration

**File**: `src/commands/config.zig`

```zig
pub fn cmdConfigSet(allocator: std.mem.Allocator, storage: *MemoryStorage, args: []RespValue) ![]u8 {
    const param = // ... parse param name
    const value = // ... parse value

    if (std.ascii.eqlIgnoreCase(param, "notify-keyspace-events")) {
        // Validate flags
        const flags = parseNotificationFlags(value);

        // At least one of K or E must be present if any other flag is set
        const has_ke = (flags & (@intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.keyevent))) != 0;
        const has_others = (flags & ~(@intFromEnum(NotificationFlag.keyspace) | @intFromEnum(NotificationFlag.keyevent))) != 0;

        if (has_others and !has_ke) {
            return writer.writeError("ERR notify-keyspace-events requires at least one of K or E");
        }

        // Store in config
        try storage.config.set(param, value);

        // Update runtime flags
        storage.notification_flags = flags;

        return writer.writeSimpleString("OK");
    }
    // ... other params
}
```

---

## 10. Test Cases

### 10.1 Basic Notification Delivery

```zig
test "keyspace notifications - SET fires set event" {
    const allocator = std.testing.allocator;
    var storage = try MemoryStorage.init(allocator);
    defer storage.deinit();

    // Enable notifications
    try storage.config.set("notify-keyspace-events", "KE$");
    storage.notification_flags = parseNotificationFlags("KE$");

    // Subscribe to keyspace channel
    _ = try storage.pubsub.subscribe(1, "__keyspace@0__:mykey");

    // Execute SET
    try storage.set("mykey", .{ .string = .{ .data = "value" } });

    // Check notification delivered
    const pending = storage.pubsub.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expect(std.mem.indexOf(u8, pending[0], "set") != null);
}
```

### 10.2 Expired Event

```zig
test "keyspace notifications - expired event on TTL expiry" {
    const allocator = std.testing.allocator;
    var storage = try MemoryStorage.init(allocator);
    defer storage.deinit();

    // Enable expired events
    try storage.config.set("notify-keyspace-events", "Ex");
    storage.notification_flags = parseNotificationFlags("Ex");

    // Subscribe to expired events
    _ = try storage.pubsub.subscribe(1, "__keyevent@0__:expired");

    // Set key with 1ms TTL
    try storage.set("tempkey", .{ .string = .{ .data = "value" } });
    try storage.expire("tempkey", 1);

    // Wait for expiry
    std.time.sleep(2_000_000); // 2ms

    // Access key (triggers lazy expiration)
    _ = storage.get("tempkey");

    // Check expired event delivered
    const pending = storage.pubsub.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expect(std.mem.indexOf(u8, pending[0], "tempkey") != null);
}
```

### 10.3 Flag Validation

```zig
test "keyspace notifications - K or E required" {
    const allocator = std.testing.allocator;
    var storage = try MemoryStorage.init(allocator);
    defer storage.deinit();

    // Invalid: no K or E
    const result = storage.config.set("notify-keyspace-events", "g$");
    try std.testing.expectError(error.InvalidValue, result);

    // Valid: K present
    try storage.config.set("notify-keyspace-events", "Kg$");

    // Valid: E present
    try storage.config.set("notify-keyspace-events", "Eg$");

    // Valid: both present
    try storage.config.set("notify-keyspace-events", "KEg$");
}
```

### 10.4 Pattern Subscription

```zig
test "keyspace notifications - pattern subscription" {
    const allocator = std.testing.allocator;
    var storage = try MemoryStorage.init(allocator);
    defer storage.deinit();

    // Enable notifications
    try storage.config.set("notify-keyspace-events", "KE$");
    storage.notification_flags = parseNotificationFlags("KE$");

    // Pattern subscribe to all keyspace events
    _ = try storage.pubsub.psubscribe(1, "__keyspace@0__:*");

    // Execute multiple SETs
    try storage.set("key1", .{ .string = .{ .data = "val1" } });
    try storage.set("key2", .{ .string = .{ .data = "val2" } });

    // Check both events delivered
    const pending = storage.pubsub.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 2), pending.len);
}
```

---

## 11. Redis Compatibility Checklist

### 11.1 Must-Have (Blocking 1.0)

- [x] `notify-keyspace-events` config parameter defined
- [x] Flag parsing (K, E, g, $, l, s, h, z, t, d, x, e, m, n, A)
- [ ] Validation: K or E required if other flags present
- [ ] `__keyspace@<db>__:<key>` channel format
- [ ] `__keyevent@<db>__:<event>` channel format
- [ ] Integration with existing pub/sub infrastructure
- [ ] Event dispatch for all string commands (flag `$`)
- [ ] Event dispatch for generic commands (flag `g`)
- [ ] Event dispatch for list commands (flag `l`)
- [ ] Event dispatch for set commands (flag `s`)
- [ ] Event dispatch for hash commands (flag `h`)
- [ ] Event dispatch for sorted set commands (flag `z`)
- [ ] Event dispatch for stream commands (flag `t`)
- [ ] Expired key events (flag `x`)
- [ ] Evicted key events (flag `e`)
- [ ] Multi-database support (events per DB index)
- [ ] Pattern subscription support (`PSUBSCRIBE __keyspace@*__:*`)

### 11.2 Should-Have (Target for 1.0)

- [ ] New key events (flag `n`)
- [ ] Overwrite events (flag `o`)
- [ ] Type-change events (flag `c`)
- [ ] Performance optimization (flag check caching)
- [ ] RESP3 push notification integration
- [ ] Benchmarks vs Redis (overhead < 10% with `KEA`)

### 11.3 Nice-to-Have (Post-1.0)

- [ ] Key miss events (flag `m`) - High volume, rarely used
- [ ] Module events (flag `d`) - Requires Phase 17 (Modules API)
- [ ] Per-command notification enable/disable flags
- [ ] Notification rate limiting (prevent DoS)

---

## 12. Implementation Priorities

### Iteration 244: Foundation
- Add CONFIG SET validation for notify-keyspace-events
- Add notification dispatch to `MemoryStorage.set()`
- Add notification dispatch to `MemoryStorage.del()`
- Integration test: basic keyspace event delivery

### Iteration 245: Core Commands
- Add notifications to all string commands
- Add notifications to generic commands (EXPIRE, RENAME, etc.)
- Integration test: full string command coverage

### Iteration 246: Data Structures
- Add notifications to list commands
- Add notifications to set commands
- Add notifications to hash commands
- Add notifications to sorted set commands
- Integration test: data structure event coverage

### Iteration 247: Special Events
- Implement expired key events (active + lazy expiration)
- Implement evicted key events (maxmemory integration)
- Integration test: expired and evicted events

### Iteration 248: Advanced Flags
- Implement new key events (flag `n`)
- Implement overwrite events (flag `o`)
- Implement type-change events (flag `c`)
- Integration test: advanced flag combinations

### Iteration 249: Performance & Polish
- Benchmark notification overhead (0-10% target)
- Add RESP3 push notification support
- Differential testing vs Redis for all event types
- Performance regression tests

---

## 13. References

- [Redis Keyspace Notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)
- [Redis CONFIG SET](https://redis.io/commands/config-set/)
- [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

---

**Document Version**: 1.0
**Date**: 2026-05-10
**Author**: redis-spec-analyzer (Claude Code Agent)
**Status**: Ready for implementation
