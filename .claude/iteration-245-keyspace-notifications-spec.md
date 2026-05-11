# Redis Keyspace Notifications Specification — Iteration 245

## Overview

Redis keyspace notifications allow clients to subscribe to Pub/Sub channels to receive events affecting the Redis data set. This iteration integrates notification calls into command handlers for string commands and generic key commands (DEL, RENAME, EXPIRE, etc.).

**Foundation**: `src/storage/notifications.zig` already provides `parseNotificationFlags()`, `shouldNotify()`, and `publishNotification()`. This iteration adds notification publishing to actual command execution paths.

**Target**: Enable keyspace notifications for:
- **String commands**: SET, GET, APPEND, INCR, DECR, etc.
- **Generic key commands**: DEL, EXPIRE, PERSIST, RENAME, etc.

---

## 1. Official Redis Documentation Reference

- **Main spec**: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- **Configuration**: https://redis.io/docs/latest/commands/config-set/
- **Pub/Sub protocol**: https://redis.io/docs/latest/develop/reference/protocol-spec/

---

## 2. Configuration Parameter: notify-keyspace-events

### Parameter Name
`notify-keyspace-events` (case-insensitive)

### Default Value
Empty string `""` (notifications disabled by default)

### Value Format
Non-empty string composed of flag characters. Each character enables specific event types.

### Required Flags
At least one of `K` or `E` must be present for notifications to fire:
- **K**: Enable keyspace events (`__keyspace@<db>__:<key>` channels)
- **E**: Enable keyevent events (`__keyevent@<db>__:<event>` channels)

### Data Type Flags
- **g**: Generic commands (DEL, EXPIRE, RENAME, etc.)
- **$**: String commands (SET, APPEND, INCR, etc.)
- **l**: List commands (LPUSH, RPOP, etc.)
- **s**: Set commands (SADD, SREM, etc.)
- **h**: Hash commands (HSET, HDEL, etc.)
- **z**: Sorted set commands (ZADD, ZREM, etc.)
- **t**: Stream commands (XADD, XDEL, etc.)
- **d**: Module key type events (reserved for Redis modules)

### Special Event Flags
- **x**: Expired events (keys expired via TTL)
- **e**: Evicted events (keys evicted due to maxmemory)
- **m**: Key miss events (access to non-existent key)
- **n**: New key events (new key created)
- **o**: Overwritten events (key value overwritten)
- **c**: Type-changed events (key type changed)

### Alias Flag
- **A**: Shorthand for `g$lshztdxe` (all common events except m, n, o, c)

### Examples
```
"KEA"     → Enable keyspace + keyevent for all common event types
"Kg"      → Enable keyspace events for generic commands only
"K$"      → Enable keyspace events for string commands only
"Egx"     → Enable keyevent events for generic + expired events
""        → Disable all notifications (default)
```

### Implementation Notes
- The config value is stored in `Storage.config` as a string
- When CONFIG SET is called, the string is parsed using `notifications_mod.parseNotificationFlags()`
- The resulting bit flags are stored in `Storage.notification_flags` (atomic u16)
- Commands check `notification_flags` before publishing events

---

## 3. Channel Format Specifications

### Keyspace Notifications
**Format**: `__keyspace@<db>__:<keyname>`
**Message Payload**: Event name (e.g., "set", "del", "expire")

**Example**:
```
PUBLISH __keyspace@0__:mykey del
```
Subscribers to `__keyspace@0__:mykey` receive message `"del"`.

### Keyevent Notifications
**Format**: `__keyevent@<db>__:<eventname>`
**Message Payload**: Key name

**Example**:
```
PUBLISH __keyevent@0__:del mykey
```
Subscribers to `__keyevent@0__:del` receive message `"mykey"`.

### Database Index
- `<db>` is the database index (0-15 by default)
- Zoltraak currently supports DB 0 only (SELECT stub)
- Always use `0` for `<db>` in this iteration

### Pattern Subscriptions
Clients can use PSUBSCRIBE with wildcard patterns:
```
PSUBSCRIBE __key*__:*       → All keyspace + keyevent events
PSUBSCRIBE __keyspace@0__:* → All keyspace events in DB 0
PSUBSCRIBE __keyevent@0__:* → All keyevent events in DB 0
```

---

## 4. Event Names by Command

### String Commands (Flag: `$`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| SET | `set` | `new` (if new key), `overwritten` (if key existed), `expire` (if EX/PX) | |
| SETNX | `set` | `new` (if new key) | Only if key didn't exist |
| SETEX | `set`, `expire` | `new` (if new key) | Two events: set + expire |
| PSETEX | `set`, `expire` | `new` (if new key) | Two events: set + expire |
| GETSET | `set` | `overwritten` (if key existed) | |
| APPEND | `append` | `new` (if new key) | |
| INCR | `incrby` | `new` (if new key) | |
| DECR | `incrby` | `new` (if new key) | Same event name as INCR |
| INCRBY | `incrby` | `new` (if new key) | |
| DECRBY | `incrby` | `new` (if new key) | Same event name as INCR |
| INCRBYFLOAT | `incrbyfloat` | `new` (if new key) | |
| SETRANGE | `setrange` | `new` (if new key) | |
| MSET | `set` | `new` per new key | One event **per key** |
| MSETNX | `set` | `new` per new key | One event **per key** (only if all succeed) |
| GETEX | (no event) | `expire` (if EX/PX), `persist` (if PERSIST) | GET doesn't fire events; GETEX modifies TTL so fires expire/persist |
| GETDEL | `del` | | GET + DEL fires del event |

### Generic Key Commands (Flag: `g`)

| Command | Event Name | Additional Events | Notes |
|---------|------------|-------------------|-------|
| DEL | `del` | | One event **per deleted key** |
| UNLINK | `del` | | Same as DEL (async deletion is internal) |
| EXPIRE | `expire` | `del` (if TTL <= 0) | Negative/zero TTL = immediate delete |
| PEXPIRE | `expire` | `del` (if TTL <= 0) | |
| EXPIREAT | `expire` | `del` (if timestamp in past) | |
| PEXPIREAT | `expire` | `del` (if timestamp in past) | |
| PERSIST | `persist` | | Only if TTL was removed successfully |
| RENAME | `rename_from` (source), `rename_to` (dest) | `del` (if dest existed) | **Two** events, in order: rename_from, then rename_to. If dest key existed, also fire `del` for old dest. |
| RENAMENX | `rename_from` (source), `rename_to` (dest) | | Only if rename succeeded (dest didn't exist) |
| COPY | `copy_to` | `new` (if dest is new), `overwritten` (if dest existed), `del` (if REPLACE and dest existed) | Event on **destination** key only |
| MOVE | `move_from` (source DB), `move_to` (dest DB) | | **Note**: Zoltraak only supports DB 0, so MOVE is stubbed. Events would fire on both DBs. |
| SORT with STORE | `sortstore` | `del` (if result empty and key existed), `new` (if dest is new) | Only if STORE is used |
| MIGRATE | `del` | | Only if source key removed (COPY flag not used) |

### Expiration Events (Flag: `x`)

| Trigger | Event Name | Notes |
|---------|------------|-------|
| Key accessed after TTL expired | `expired` | Lazy expiration |
| Background expiration cycle | `expired` | Active expiration |

**Important**: `expired` events are **not guaranteed** to fire at the exact moment TTL reaches 0. They fire when:
1. Key is accessed and found expired (lazy)
2. Background expiration cycle runs (active)

If a key expires but is never accessed and background cycle doesn't reach it, the event may be significantly delayed.

### New Key Events (Flag: `n`)

| Trigger | Event Name | Notes |
|---------|------------|-------|
| Command creates a new key | `new` | Fires **after** the command event (e.g., `set` then `new`) |

Examples:
- `SET newkey value` → fires `set` then `new`
- `INCR newkey` → fires `incrby` then `new`

### Overwritten Events (Flag: `o`)

| Trigger | Event Name | Notes |
|---------|------------|-------|
| Command overwrites existing key | `overwritten` | Fires **after** the command event |

Example:
- `SET existingkey newvalue` → fires `set` then `overwritten`

### Type Changed Events (Flag: `c`)

| Trigger | Event Name | Notes |
|---------|------------|-------|
| Key type changes (e.g., string → list) | `type_changed` | **Redis 7.4+** feature (not in scope for this iteration) |

---

## 5. Implementation Requirements

### Phase 1: Storage Layer Support (DONE ✓)
Already implemented in `src/storage/notifications.zig`:
- `parseNotificationFlags(config_str: []const u8) u16` ✓
- `shouldNotify(flags: u16, event_flag: NotificationFlag) bool` ✓
- `publishNotification(allocator, pubsub_state, db_index, key, event, flags)` ✓

Already implemented in `src/storage/memory.zig`:
- `notification_flags: std.atomic.Value(u16)` ✓
- `setNotificationFlags(flags: u16)` ✓
- `getNotificationFlags() u16` ✓

Already implemented in `src/commands/config.zig`:
- `CONFIG SET notify-keyspace-events <value>` ✓
- Parses config string and updates `storage.notification_flags` atomically ✓

### Phase 2: Command Handler Integration (THIS ITERATION)

#### Step 1: Add Helper Function to strings.zig
Create a helper function to publish notifications for string commands:

```zig
/// Publish keyspace notification for a string command
fn publishStringNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();

    // Check if string events are enabled
    if (!notifications_mod.shouldNotify(flags, .string)) return;

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
```

#### Step 2: Add Helper Function for Generic Events
Create a helper function for generic key events:

```zig
/// Publish keyspace notification for a generic key command
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

#### Step 3: Integrate Notifications into String Commands
Modify these functions in `src/commands/strings.zig`:

**SET family**:
- `cmdSet()` → After `storage.set()`, publish `"set"` event
- `cmdSetnx()` → After successful set, publish `"set"` event
- `cmdSetex()` → After set, publish `"set"` then `"expire"` events
- `cmdPsetex()` → After set, publish `"set"` then `"expire"` events
- `cmdGetset()` → After set, publish `"set"` event
- `cmdMset()` → After all sets, publish `"set"` event for **each** key
- `cmdMsetnx()` → After successful sets, publish `"set"` event for **each** key

**APPEND**:
- `cmdAppend()` → After append, publish `"append"` event

**INCR family**:
- `cmdIncr()` → After increment, publish `"incrby"` event
- `cmdDecr()` → After decrement, publish `"incrby"` event
- `cmdIncrby()` → After increment, publish `"incrby"` event
- `cmdDecrby()` → After decrement, publish `"incrby"` event
- `cmdIncrbyfloat()` → After increment, publish `"incrbyfloat"` event

**SETRANGE**:
- `cmdSetrange()` → After modification, publish `"setrange"` event

**GETEX**:
- `cmdGetex()` → If EX/PX option used, publish `"expire"` event; if PERSIST used, publish `"persist"` event

**GETDEL**:
- `cmdGetdel()` → After delete, publish `"del"` event (use generic helper)

#### Step 4: Integrate Notifications into Generic Key Commands
Modify these functions in `src/commands/keys.zig`:

**DEL**:
- `cmdDel()` → After each successful deletion, publish `"del"` event for that key

**UNLINK**:
- `cmdUnlink()` → Same as DEL, publish `"del"` event for each key

**EXPIRE family**:
- `cmdExpire()` → After setting TTL, publish `"expire"` event (or `"del"` if TTL <= 0)
- `cmdPexpire()` → Same as EXPIRE
- `cmdExpireat()` → After setting expiry, publish `"expire"` event (or `"del"` if timestamp in past)
- `cmdPexpireat()` → Same as EXPIREAT

**PERSIST**:
- `cmdPersist()` → After removing TTL, publish `"persist"` event (only if successful)

**RENAME**:
- `cmdRename()` → Publish `"rename_from"` for source key, then `"rename_to"` for dest key (and `"del"` if dest existed)

**RENAMENX**:
- `cmdRenamenx()` → Same as RENAME, but only if rename succeeded

**COPY**:
- `cmdCopy()` → Publish `"copy_to"` event on destination key

**SORT with STORE**:
- `cmdSort()` → If STORE option used, publish `"sortstore"` event on destination key

#### Step 5: Notification Ordering
**Critical**: Events must fire in the correct order:
1. Command-specific event (e.g., `"set"`)
2. Special events (e.g., `"new"`, `"overwritten"`, `"expire"`)

**Example** (SET on new key):
```zig
// 1. Set the value
try storage.set(key, value, null);

// 2. Publish "set" event
try publishStringNotification(allocator, storage, pubsub, key, "set");

// 3. If new key, publish "new" event (if flag 'n' enabled)
// (New key detection: check if key existed before SET)
```

**Note**: For this iteration, we focus on command events only. Special events (`new`, `overwritten`) are deferred to a future iteration.

---

## 6. Edge Cases and Error Conditions

### 6.1. No Event When Command Doesn't Modify Key
**Rule**: If a command doesn't actually modify the target key, no event is generated.

**Examples**:
- `DEL nonexistent` → No event (key didn't exist)
- `SETNX existingkey value` → No event (SETNX fails if key exists)
- `PERSIST keywithouttl` → No event (key had no TTL to remove)
- `EXPIRE nonexistent 60` → No event (key doesn't exist)

**Implementation**: Only publish notifications **after successful modification**.

### 6.2. Multiple Events per Command
Some commands generate **multiple** events:

- `SETEX key 60 value` → `"set"` + `"expire"` (two events, in order)
- `RENAME oldkey newkey` → `"rename_from"` (oldkey) + `"rename_to"` (newkey) (and `"del"` if newkey existed)
- `MSET key1 val1 key2 val2` → `"set"` for key1 + `"set"` for key2

**Implementation**: Call `publishNotification()` multiple times, in the documented order.

### 6.3. Empty Collection Deletion
For list/set/hash/zset commands, when a collection becomes empty after a removal operation, a `"del"` event is also fired.

**Example**:
- `LPOP mylist` (last element) → `"lpop"` + `"del"`

**Note**: This applies to list/set/hash/zset commands, which are **not in scope** for this iteration. String and generic key commands don't have this behavior.

### 6.4. Expired Event Timing
`expired` events (flag `x`) are **not guaranteed** to fire at the exact moment TTL reaches 0.

**Reasons**:
1. **Lazy expiration**: Event fires when key is accessed and found expired
2. **Active expiration**: Background cycle runs periodically and removes expired keys

**Implications**:
- If a key expires but is never accessed, the `expired` event may be **significantly delayed**
- If many keys have TTLs, background cycle may not reach all of them immediately
- **Do not rely on `expired` events for precise timing**

**Redis Docs**: "There is no guarantee that clients will receive the expired event at the exact time the key expires. Redis makes a best effort to deliver the event as close to the expiration time as possible."

**Implementation for this iteration**:
- Active expiration is implemented in `src/storage/memory.zig` (background job)
- Lazy expiration happens in `storage.get()` and other access methods
- Both should publish `"expired"` event when they delete an expired key

**Note**: Full integration of `expired` events requires modifying expiration logic in `memory.zig`, which is a future iteration. For now, focus on `expire` events (setting TTL).

### 6.5. Notifications Disabled by Default
`notify-keyspace-events` defaults to empty string `""`, meaning **all notifications are disabled** by default.

**Performance reason**: Notifications consume CPU. Redis disables them to avoid overhead unless explicitly enabled.

**Implementation**: Commands should check `storage.notification_flags` and return early if `0` (or if specific flag not set).

### 6.6. Fire-and-Forget Pub/Sub Behavior
Redis Pub/Sub is **fire-and-forget**:
- If no client is subscribed to a channel, the message is discarded
- If a client disconnects, it loses all messages during disconnection
- **Cannot replay** missed events

**Implementation**: Notifications are published via `pubsub_state.publish()`, which handles this internally. No special handling needed in command handlers.

### 6.7. Cluster-Specific Behavior (Out of Scope)
In Redis Cluster, keyspace notifications are **node-specific**, not cluster-wide:
- Each node generates events only for keys it owns
- Clients must subscribe to **each node** to receive all events

**Zoltraak Status**: Cluster mode is stubbed. All keys are on "node 0". No special handling needed for this iteration.

---

## 7. Testing Requirements

### Unit Tests (src/storage/notifications.zig)
Already exist:
- `parseNotificationFlags` parsing ✓
- `shouldNotify` flag checking ✓
- `publishNotification` channel formatting ✓

### Integration Tests (tests/test_keyspace_notifications_integration.zig)
To be created:

**Test 1: SET command with KEg flags**
1. Start server
2. CONFIG SET notify-keyspace-events "KE$"
3. Client 1: SUBSCRIBE __keyspace@0__:mykey
4. Client 1: SUBSCRIBE __keyevent@0__:set
5. Client 2: SET mykey value
6. Assert: Client 1 receives 2 messages:
   - `["message", "__keyspace@0__:mykey", "set"]`
   - `["message", "__keyevent@0__:set", "mykey"]`

**Test 2: DEL command with KEg flags**
1. CONFIG SET notify-keyspace-events "KEg"
2. SET mykey value (no notification yet)
3. SUBSCRIBE __keyspace@0__:mykey
4. SUBSCRIBE __keyevent@0__:del
5. DEL mykey
6. Assert: 2 messages received (keyspace + keyevent for "del")

**Test 3: EXPIRE command with KEg flags**
1. CONFIG SET notify-keyspace-events "KEg"
2. SET mykey value
3. SUBSCRIBE __keyspace@0__:mykey
4. SUBSCRIBE __keyevent@0__:expire
5. EXPIRE mykey 60
6. Assert: 2 messages received (keyspace + keyevent for "expire")

**Test 4: Notifications disabled by default**
1. Do NOT call CONFIG SET (notifications disabled)
2. SUBSCRIBE __keyspace@0__:mykey
3. SET mykey value
4. Assert: No messages received

**Test 5: MSET fires multiple events**
1. CONFIG SET notify-keyspace-events "KE$"
2. SUBSCRIBE __keyspace@0__:key1
3. SUBSCRIBE __keyspace@0__:key2
4. MSET key1 val1 key2 val2
5. Assert: 2 messages received (one for key1, one for key2)

**Test 6: SETEX fires set + expire events**
1. CONFIG SET notify-keyspace-events "KE$g"
2. SUBSCRIBE __keyevent@0__:set
3. SUBSCRIBE __keyevent@0__:expire
4. SETEX mykey 60 value
5. Assert: 2 messages received ("set" and "expire")

**Test 7: RENAME fires rename_from + rename_to events**
1. CONFIG SET notify-keyspace-events "KEg"
2. SET oldkey value
3. SUBSCRIBE __keyspace@0__:oldkey
4. SUBSCRIBE __keyspace@0__:newkey
5. RENAME oldkey newkey
6. Assert: 2 messages received (rename_from on oldkey, rename_to on newkey)

**Test 8: No event when command fails**
1. CONFIG SET notify-keyspace-events "KEg"
2. SUBSCRIBE __keyspace@0__:mykey
3. DEL mykey (key doesn't exist)
4. Assert: No messages received (DEL of non-existent key doesn't fire event)

**Test 9: Pattern subscription (PSUBSCRIBE)**
1. CONFIG SET notify-keyspace-events "KEg"
2. PSUBSCRIBE __key*__:*
3. SET mykey value
4. DEL mykey
5. Assert: 4 messages received (keyspace + keyevent for SET, keyspace + keyevent for DEL)

**Test 10: Flag filtering (K only, no E)**
1. CONFIG SET notify-keyspace-events "K$" (keyspace only, no keyevent)
2. SUBSCRIBE __keyspace@0__:mykey
3. SUBSCRIBE __keyevent@0__:set (should not receive messages)
4. SET mykey value
5. Assert: Only __keyspace@0__:mykey receives message, __keyevent@0__:set does not

**Test 11: Flag filtering (E only, no K)**
1. CONFIG SET notify-keyspace-events "E$" (keyevent only, no keyspace)
2. SUBSCRIBE __keyspace@0__:mykey (should not receive messages)
3. SUBSCRIBE __keyevent@0__:set
4. SET mykey value
5. Assert: Only __keyevent@0__:set receives message, __keyspace@0__:mykey does not

**Test 12: Selective flag filtering (g vs $)**
1. CONFIG SET notify-keyspace-events "KEg" (generic only, no string)
2. SUBSCRIBE __keyevent@0__:set
3. SUBSCRIBE __keyevent@0__:del
4. SET mykey value ($ flag not enabled)
5. DEL mykey (g flag enabled)
6. Assert: Only __keyevent@0__:del receives message, __keyevent@0__:set does not

### Python Compatibility Test (tests/redis_compat_keyspace_notifications.py)
To be created:

```python
#!/usr/bin/env python3
"""
Redis compatibility test: Keyspace notifications byte-by-byte comparison.
"""
import redis
import subprocess
import time
import threading

def test_keyspace_notifications_redis(port):
    """Test against real Redis."""
    r = redis.Redis(host='127.0.0.1', port=port, decode_responses=True)
    r.config_set('notify-keyspace-events', 'KEA')

    pubsub = r.pubsub()
    pubsub.subscribe('__keyspace@0__:mykey')

    # Wait for subscription to complete
    msg = pubsub.get_message(timeout=1)
    assert msg['type'] == 'subscribe'

    # Execute command in another thread
    def execute():
        time.sleep(0.1)
        r.set('mykey', 'value')

    t = threading.Thread(target=execute)
    t.start()

    # Receive notification
    msg = pubsub.get_message(timeout=2)
    t.join()

    assert msg is not None
    assert msg['type'] == 'message'
    assert msg['channel'] == '__keyspace@0__:mykey'
    assert msg['data'] == 'set'

def test_keyspace_notifications_zoltraak(port):
    """Test against Zoltraak."""
    # Same test as above, but against Zoltraak port
    test_keyspace_notifications_redis(port)

if __name__ == '__main__':
    # Start Redis
    redis_proc = subprocess.Popen(['redis-server', '--port', '6379'])
    time.sleep(1)

    # Start Zoltraak
    zoltraak_proc = subprocess.Popen(['./zig-out/bin/zoltraak', '--port', '6380'])
    time.sleep(1)

    try:
        print("Testing Redis...")
        test_keyspace_notifications_redis(6379)
        print("✓ Redis test passed")

        print("Testing Zoltraak...")
        test_keyspace_notifications_zoltraak(6380)
        print("✓ Zoltraak test passed")

    finally:
        redis_proc.terminate()
        zoltraak_proc.terminate()
        redis_proc.wait()
        zoltraak_proc.wait()
```

---

## 8. Performance Considerations

### CPU Overhead
**Redis Documentation**: "Keyspace notifications are disabled by default because while not very sensible the feature uses some CPU power."

**Implementation**:
- Check `notification_flags` early and return if `0` (no flags enabled)
- Use bitwise operations for flag checking (fast)
- `publishNotification()` only allocates if notifications are enabled

### Memory Overhead
- Each notification requires allocating RESP-formatted message bytes
- Messages are stored in per-subscriber pending queues (max 1024 per subscriber)
- Use arena allocator for temporary allocations where possible

### Atomicity
- `Storage.notification_flags` is atomic (`std.atomic.Value(u16)`)
- CONFIG SET updates flags atomically
- Commands read flags without locking (lock-free read)

---

## 9. Future Iterations

### Out of Scope for This Iteration
The following are deferred to future iterations:

1. **Special events** (`new`, `overwritten`, `type_changed`):
   - Requires tracking "did key exist before" for each command
   - Requires detecting type changes
   - **Future iteration**: Add support for `n`, `o`, `c` flags

2. **List/Set/Hash/Zset notifications** (`l`, `s`, `h`, `z` flags):
   - LPUSH → `"lpush"` event
   - SADD → `"sadd"` event
   - HSET → `"hset"` event
   - ZADD → `"zadd"` event
   - **Future iteration**: Add notifications to all data structure commands

3. **Stream notifications** (`t` flag):
   - XADD → `"xadd"` event
   - **Future iteration**: Add notifications to stream commands

4. **Expiration events** (`x` flag):
   - Integrate `"expired"` event into active/lazy expiration logic
   - **Future iteration**: Modify `memory.zig` expiration code

5. **Eviction events** (`e` flag):
   - Fire `"evicted"` when keys are evicted for maxmemory
   - **Future iteration**: Integrate with eviction policy

6. **Key miss events** (`m` flag):
   - Fire `"miss"` when accessing non-existent key
   - **Future iteration**: Add to `storage.get()` and similar methods

7. **Module events** (`d` flag):
   - Module-specific key type events
   - **Future iteration**: Phase 17 (modules)

### Scope for This Iteration
**String commands**:
- SET, SETNX, SETEX, PSETEX, GETSET, MSET, MSETNX → `"set"` event
- APPEND → `"append"` event
- INCR, DECR, INCRBY, DECRBY → `"incrby"` event
- INCRBYFLOAT → `"incrbyfloat"` event
- SETRANGE → `"setrange"` event
- GETEX → `"expire"` or `"persist"` event
- GETDEL → `"del"` event

**Generic key commands**:
- DEL, UNLINK → `"del"` event
- EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT → `"expire"` event
- PERSIST → `"persist"` event
- RENAME → `"rename_from"` + `"rename_to"` events
- RENAMENX → `"rename_from"` + `"rename_to"` events (if successful)
- COPY → `"copy_to"` event
- SORT with STORE → `"sortstore"` event

---

## 10. Implementation Checklist

### Phase 1: Helper Functions
- [ ] Add `publishStringNotification()` to `src/commands/strings.zig`
- [ ] Add `publishGenericNotification()` to `src/commands/strings.zig` (or `keys.zig`)

### Phase 2: String Commands (src/commands/strings.zig)
- [ ] cmdSet() → publish `"set"` event
- [ ] cmdSetnx() → publish `"set"` event (if successful)
- [ ] cmdSetex() → publish `"set"` + `"expire"` events
- [ ] cmdPsetex() → publish `"set"` + `"expire"` events
- [ ] cmdGetset() → publish `"set"` event
- [ ] cmdMset() → publish `"set"` event per key
- [ ] cmdMsetnx() → publish `"set"` event per key (if successful)
- [ ] cmdAppend() → publish `"append"` event
- [ ] cmdIncr() → publish `"incrby"` event
- [ ] cmdDecr() → publish `"incrby"` event
- [ ] cmdIncrby() → publish `"incrby"` event
- [ ] cmdDecrby() → publish `"incrby"` event
- [ ] cmdIncrbyfloat() → publish `"incrbyfloat"` event
- [ ] cmdSetrange() → publish `"setrange"` event
- [ ] cmdGetex() → publish `"expire"` or `"persist"` event
- [ ] cmdGetdel() → publish `"del"` event

### Phase 3: Generic Key Commands (src/commands/keys.zig)
- [ ] cmdDel() → publish `"del"` event per deleted key
- [ ] cmdUnlink() → publish `"del"` event per deleted key
- [ ] cmdExpire() → publish `"expire"` event (or `"del"` if TTL <= 0)
- [ ] cmdPexpire() → publish `"expire"` event (or `"del"` if TTL <= 0)
- [ ] cmdExpireat() → publish `"expire"` event (or `"del"` if past)
- [ ] cmdPexpireat() → publish `"expire"` event (or `"del"` if past)
- [ ] cmdPersist() → publish `"persist"` event (if successful)
- [ ] cmdRename() → publish `"rename_from"` + `"rename_to"` events
- [ ] cmdRenamenx() → publish `"rename_from"` + `"rename_to"` events (if successful)
- [ ] cmdCopy() → publish `"copy_to"` event
- [ ] cmdSort() with STORE → publish `"sortstore"` event

### Phase 4: Integration Tests
- [ ] Create `tests/test_keyspace_notifications_integration.zig`
- [ ] Test 1: SET with KEg flags
- [ ] Test 2: DEL with KEg flags
- [ ] Test 3: EXPIRE with KEg flags
- [ ] Test 4: Notifications disabled by default
- [ ] Test 5: MSET fires multiple events
- [ ] Test 6: SETEX fires set + expire
- [ ] Test 7: RENAME fires rename_from + rename_to
- [ ] Test 8: No event when command fails
- [ ] Test 9: PSUBSCRIBE pattern matching
- [ ] Test 10: Flag filtering (K only)
- [ ] Test 11: Flag filtering (E only)
- [ ] Test 12: Selective flag filtering (g vs $)

### Phase 5: Python Compatibility Test
- [ ] Create `tests/redis_compat_keyspace_notifications.py`
- [ ] Differential test against real Redis

### Phase 6: Documentation
- [ ] Update README.md with keyspace notifications support
- [ ] Update CLAUDE.md iteration count
- [ ] Update docs/milestones.md

---

## 11. Success Criteria

1. **All 12 integration tests pass** (`zig build test-integration`)
2. **Python compatibility test passes** (byte-by-byte RESP match with Redis)
3. **No memory leaks** (`zig build test` with allocator leak detection)
4. **CONFIG SET notify-keyspace-events updates flags atomically**
5. **Notifications disabled by default** (empty string config)
6. **Events fire in correct order** (command event before special events)
7. **No events for failed commands** (e.g., DEL non-existent key)
8. **Multiple events work** (e.g., SETEX fires "set" + "expire")
9. **Pattern subscriptions work** (PSUBSCRIBE __key*__:*)
10. **Flag filtering works** (K/E, g/$, etc.)

---

## 12. References

- **Redis keyspace notifications**: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- **Redis CONFIG SET**: https://redis.io/docs/latest/commands/config-set/
- **Redis Pub/Sub protocol**: https://redis.io/docs/latest/develop/reference/protocol-spec/
- **Zoltraak notifications module**: `/Users/fn/codespace/zoltraak/src/storage/notifications.zig`
- **Zoltraak config module**: `/Users/fn/codespace/zoltraak/src/storage/config.zig`
- **Zoltraak pubsub module**: `/Users/fn/codespace/zoltraak/src/storage/pubsub.zig`

---

**End of Specification**
