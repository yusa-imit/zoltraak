# Iteration 245: Redis Keyspace Notifications — Summary

## Objective

Enable Redis keyspace notifications for string commands and generic key commands by integrating notification publishing into command handlers.

## Background

**Current State**: `src/storage/notifications.zig` provides complete infrastructure:
- `parseNotificationFlags()` — Parse config string to bit flags ✓
- `shouldNotify()` — Check if specific event type enabled ✓
- `publishNotification()` — Publish to __keyspace and __keyevent channels ✓
- `Storage.notification_flags` — Atomic u16 for runtime flags ✓
- `CONFIG SET notify-keyspace-events` — Updates flags atomically ✓

**Gap**: Command handlers don't publish notifications yet.

## What Are Keyspace Notifications?

Redis feature allowing clients to subscribe to Pub/Sub channels to receive events when keys are modified.

### Two Channel Types

1. **Keyspace notifications**: `__keyspace@<db>__:<key>` → message is event name
   - Example: `__keyspace@0__:mykey` receives `"set"` when `SET mykey value` executes

2. **Keyevent notifications**: `__keyevent@<db>__:<event>` → message is key name
   - Example: `__keyevent@0__:set` receives `"mykey"` when `SET mykey value` executes

### Configuration

`CONFIG SET notify-keyspace-events <flags>`

**Flags**:
- `K` — Enable keyspace events
- `E` — Enable keyevent events
- `g` — Generic commands (DEL, EXPIRE, RENAME, etc.)
- `$` — String commands (SET, APPEND, INCR, etc.)
- `l`, `s`, `h`, `z`, `t` — List, set, hash, zset, stream commands
- `x` — Expired events
- `e` — Evicted events
- `m`, `n`, `o`, `c` — Key miss, new key, overwritten, type changed
- `A` — Alias for `g$lshztdxe`

**Examples**:
- `KEA` — Enable all common event types
- `Kg` — Enable keyspace events for generic commands only
- `""` — Disable all notifications (default)

## Scope for This Iteration

### String Commands (flag `$`)
- SET, SETNX, SETEX, PSETEX, GETSET → `"set"` event
- MSET, MSETNX → `"set"` event **per key**
- APPEND → `"append"` event
- INCR, DECR, INCRBY, DECRBY → `"incrby"` event
- INCRBYFLOAT → `"incrbyfloat"` event
- SETRANGE → `"setrange"` event
- GETEX → `"expire"` or `"persist"` event
- GETDEL → `"del"` event

### Generic Key Commands (flag `g`)
- DEL, UNLINK → `"del"` event **per deleted key**
- EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT → `"expire"` event
- PERSIST → `"persist"` event
- RENAME → `"rename_from"` + `"rename_to"` events (in order)
- RENAMENX → Same as RENAME (if successful)
- COPY → `"copy_to"` event
- SORT with STORE → `"sortstore"` event

### Out of Scope (Future Iterations)
- List/set/hash/zset/stream commands (`l`, `s`, `h`, `z`, `t` flags)
- Special events: `new`, `overwritten`, `type_changed` (`n`, `o`, `c` flags)
- Expiration events: `expired` (`x` flag) — requires modifying expiration logic
- Eviction events: `evicted` (`e` flag) — requires eviction policy integration
- Key miss events: `miss` (`m` flag)

## Implementation Pattern

### 1. Import notifications module
```zig
const notifications_mod = @import("../storage/notifications.zig");
```

### 2. Add pubsub parameter to command function (if missing)
```zig
pub fn cmdSet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub, // Add this parameter
    args: []const RespValue,
) ![]const u8
```

### 3. After successful modification, publish notification
```zig
// Example: cmdSet()
try storage.set(key, value, null);

const flags = storage.getNotificationFlags();
if (flags != 0 and notifications_mod.shouldNotify(flags, .string)) {
    try notifications_mod.publishNotification(
        allocator,
        pubsub,
        0, // DB index (always 0)
        key,
        "set",
        flags,
    );
}
```

### 4. Handle multi-event commands
```zig
// Example: SETEX fires "set" + "expire"
try storage.set(key, value, expires_at);

const flags = storage.getNotificationFlags();
if (flags != 0) {
    if (notifications_mod.shouldNotify(flags, .string)) {
        try notifications_mod.publishNotification(allocator, pubsub, 0, key, "set", flags);
    }
    if (notifications_mod.shouldNotify(flags, .generic)) {
        try notifications_mod.publishNotification(allocator, pubsub, 0, key, "expire", flags);
    }
}
```

### 5. Handle multi-key commands
```zig
// Example: MSET fires "set" per key
for (kv_pairs.items) |kv| {
    try storage.set(kv.key, kv.value, null);
}

const flags = storage.getNotificationFlags();
if (flags != 0 and notifications_mod.shouldNotify(flags, .string)) {
    for (kv_pairs.items) |kv| {
        try notifications_mod.publishNotification(allocator, pubsub, 0, kv.key, "set", flags);
    }
}
```

## Key Design Decisions

### 1. Notifications Fire After Modification
Events are published **after** the command successfully modifies the key, not before.

**Rationale**: Subscribers should receive events only for changes that actually happened.

### 2. No Event for Failed Commands
If a command doesn't modify the key (e.g., `DEL nonexistent`, `SETNX existingkey`), no event is fired.

**Rationale**: Redis spec — "No events are generated when a command doesn't actually modify the target key."

### 3. Event Order for Multi-Event Commands
Commands that generate multiple events fire them in a specific order:
- SETEX: `"set"` → `"expire"`
- RENAME: `"rename_from"` (source) → `"del"` (if dest existed) → `"rename_to"` (dest)

**Rationale**: Matches Redis behavior.

### 4. Atomic Flag Access
`Storage.notification_flags` is `std.atomic.Value(u16)`, allowing lock-free reads.

**Rationale**: Notifications are checked on every command, so lock-free access minimizes overhead.

### 5. Early Return on Disabled Notifications
Commands check `flags == 0` and return early if no notifications are enabled.

**Rationale**: Avoids any overhead when notifications are disabled (default state).

## Testing Strategy

### Unit Tests
Already exist in `src/storage/notifications.zig`:
- `parseNotificationFlags` ✓
- `shouldNotify` ✓
- `publishNotification` ✓

### Integration Tests (New)
`tests/test_keyspace_notifications_integration.zig`:
1. SET with KEg flags → verify keyspace + keyevent messages
2. DEL with KEg flags → verify del event
3. EXPIRE with KEg flags → verify expire event
4. Notifications disabled by default → no messages
5. MSET → multiple "set" events
6. SETEX → "set" + "expire" events
7. RENAME → "rename_from" + "rename_to" events
8. Failed command → no event
9. PSUBSCRIBE pattern matching → wildcard subscriptions work
10. Flag filtering (K only, E only) → selective event delivery
11. Selective flag filtering (g vs $) → command type filtering

### Python Compatibility Test (New)
`tests/redis_compat_keyspace_notifications.py`:
- Differential testing: Compare Zoltraak's RESP output byte-by-byte with real Redis
- Multi-threaded: One thread subscribes, another executes commands
- Verify exact message format, channel names, and event names

## Success Criteria

1. All 12 integration tests pass
2. Python compatibility test passes (byte-by-byte match with Redis)
3. No memory leaks (`zig build test` with leak detection)
4. Notifications disabled by default (empty config string)
5. Events fire in correct order (command event before special events)
6. No events for failed commands
7. Multiple events work (SETEX, RENAME, MSET)
8. Pattern subscriptions work (PSUBSCRIBE)
9. Flag filtering works (K/E, g/$)
10. Performance: Early return when notifications disabled (no overhead)

## Command Modifications Required

### strings.zig
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

### keys.zig
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

## Edge Cases Handled

### 1. DEL Non-Existent Key
```bash
> DEL nonexistent
(integer) 0
# No notification fired (key didn't exist)
```

**Implementation**: Check `storage.exists(key)` before deletion, only notify for existing keys.

### 2. EXPIRE Negative TTL
```bash
> SET mykey value
> EXPIRE mykey -1
(integer) 1
# Fires "del" event, not "expire" (negative TTL = immediate delete)
```

**Implementation**: Check if TTL <= 0, fire `"del"` event instead of `"expire"`.

### 3. RENAME Overwrites Destination
```bash
> SET oldkey value1
> SET newkey value2
> RENAME oldkey newkey
# Fires 3 events:
# 1. "rename_from" on oldkey
# 2. "del" on newkey (old newkey value deleted)
# 3. "rename_to" on newkey
```

**Implementation**: Check `storage.exists(newkey)` before rename, fire `"del"` if true.

### 4. MSET Multiple Keys
```bash
> MSET key1 val1 key2 val2 key3 val3
# Fires 3 separate "set" events (one per key)
```

**Implementation**: Loop over keys after setting, fire one event per key.

### 5. Pattern Subscription
```bash
> PSUBSCRIBE __key*__:*
> SET mykey value
# Receives 2 messages:
# 1. ["pmessage", "__key*__:*", "__keyspace@0__:mykey", "set"]
# 2. ["pmessage", "__key*__:*", "__keyevent@0__:set", "mykey"]
```

**Implementation**: PubSub module already handles pattern matching. No changes needed in command handlers.

### 6. Flag Filtering
```bash
> CONFIG SET notify-keyspace-events K$  # Keyspace only, string commands only
> SUBSCRIBE __keyspace@0__:mykey
> SUBSCRIBE __keyevent@0__:set  # Won't receive (E not enabled)
> DEL mykey  # Won't receive (g not enabled, $ is)
> SET mykey value  # Receives on __keyspace@0__:mykey only
```

**Implementation**: Check `shouldNotify(flags, .string)` and `shouldNotify(flags, .keyspace)` before publishing.

## Performance Impact

### CPU Overhead
- **Disabled (default)**: Zero overhead — early return on `flags == 0`
- **Enabled**: Minimal — atomic flag read + bitwise check + channel name allocation

### Memory Overhead
- Each notification allocates 2 RESP-formatted message strings (keyspace + keyevent)
- Messages stored in per-subscriber pending queues (max 1024 per subscriber)
- Use allocator leak detection in tests to ensure no leaks

### Atomicity
- `Storage.notification_flags` is atomic — lock-free reads
- No mutex contention on notification checks

## Documentation Updates

After implementation:
1. Update README.md with keyspace notifications support
2. Update CLAUDE.md iteration count to 245
3. Update docs/milestones.md with completion status
4. Add keyspace notifications to feature list

## References

- **Spec document**: `.claude/iteration-245-keyspace-notifications-spec.md`
- **Implementation guide**: `.claude/iteration-245-implementation-guide.md`
- **Redis docs**: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- **Redis CONFIG SET**: https://redis.io/docs/latest/commands/config-set/
- **Notifications module**: `/Users/fn/codespace/zoltraak/src/storage/notifications.zig`
- **Config module**: `/Users/fn/codespace/zoltraak/src/storage/config.zig`
- **PubSub module**: `/Users/fn/codespace/zoltraak/src/storage/pubsub.zig`

## Recent Articles (2026)
- [How to Create Redis Keyspace Notifications](https://oneuptime.com/blog/post/2026-01-30-redis-keyspace-notifications/view) (Jan 2026)
- [How to Listen for Key Update Events](https://oneuptime.com/blog/post/2026-03-31-redis-listen-key-update-events/view) (Mar 2026)
- [How to Enable Keyspace Notifications](https://oneuptime.com/blog/post/2026-03-31-redis-how-to-enable-keyspace-notifications-in-redis/view) (Mar 2026)

---

**Ready for implementation!** See implementation guide for code examples.

Sources:
- [Redis keyspace notifications | Docs](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)
- [How to Create Redis Keyspace Notifications](https://oneuptime.com/blog/post/2026-01-30-redis-keyspace-notifications/view)
- [How to Listen for Key Update Events in Redis](https://oneuptime.com/blog/post/2026-03-31-redis-listen-key-update-events/view)
- [How to Enable Keyspace Notifications in Redis](https://oneuptime.com/blog/post/2026-03-31-redis-how-to-enable-keyspace-notifications-in-redis/view)
