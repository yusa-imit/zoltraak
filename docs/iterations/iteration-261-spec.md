# Iteration 261 — CLIENT TRACKING Invalidation Message Delivery

**Phase**: 18.1 (Client-side caching — invalidation delivery)
**Date**: 2026-05-18
**Prerequisite**: Iteration 260 (CLIENT TRACKING core infrastructure complete)

---

## Overview

Implement the invalidation message delivery mechanism for CLIENT TRACKING. This completes the client-side caching feature by delivering RESP3 push messages to clients when tracked keys are modified.

**Current State** (Iteration 260):
- ✅ Tracking table with key → client_set mapping
- ✅ PREFIX tracking for broadcast mode
- ✅ REDIRECT, BCAST, OPTIN, OPTOUT, NOLOOP flags stored per client
- ✅ `trackKeyAccess()` records key accesses
- ✅ `getInvalidationMessages()` generates invalidation list

**Missing**:
- ❌ Integration with write commands to trigger invalidations
- ❌ RESP3 push message delivery to clients
- ❌ Handling of disconnected REDIRECT targets
- ❌ Removal of invalidated keys from tracking table

---

## Redis Specification

### 1. When Invalidations Are Triggered

Invalidation messages are sent when a tracked key is **modified by any write command**, including but not limited to:

**String commands**:
- SET, SETEX, PSETEX, SETNX, MSET, MSETNX, APPEND, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, SETRANGE, GETSET, GETDEL, GETEX

**Key expiration/deletion**:
- DEL, UNLINK, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, PERSIST, RENAME, RENAMENX
- Passive/active expiration (key expires and is deleted)

**Type-specific writes**:
- **List**: LPUSH, RPUSH, LPOP, RPOP, LSET, LTRIM, LINSERT, LREM, BLPOP, BRPOP, BLMOVE, etc.
- **Set**: SADD, SREM, SPOP, SMOVE, etc.
- **Hash**: HSET, HDEL, HINCRBY, HINCRBYFLOAT, HMSET, HSETNX, etc.
- **Sorted Set**: ZADD, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX, etc.
- **Stream**: XADD, XDEL, XTRIM, XCLAIM, XAUTOCLAIM, etc.
- **Bitmap**: SETBIT, BITOP
- **HyperLogLog**: PFADD, PFMERGE
- **Geospatial**: GEOADD
- **JSON**: JSON.SET, JSON.DEL, JSON.NUMINCRBY, JSON.ARRAPPEND, etc.

**When a key is modified**:
1. Look up clients tracking this key in the tracking table
2. For each tracking client, check:
   - NOLOOP: skip if modifier is the tracking client
   - BCAST mode: verify key matches at least one PREFIX
   - REDIRECT: determine target client (self or redirect ID)
3. Generate invalidation message for target client
4. Remove key from tracking table (clients must re-read to re-track)

### 2. RESP3 Push Message Format

Invalidation messages use the RESP3 **Push** type (`>` prefix):

```
>2\r\n
$10\r\n
invalidate\r\n
*1\r\n
$3\r\nkey\r\n
```

**Structure**:
- Push type: `>` followed by element count (2)
- Element 1: Bulk string `"invalidate"`
- Element 2: Array of invalidated keys (can contain multiple keys)

**Example (single key invalidation)**:
```
>2
$10
invalidate
*1
$7
mykey:1
```

**Example (multiple keys invalidation)**:
```
>2
$10
invalidate
*3
$5
user:1
$5
user:2
$5
user:3
```

**Special case (null invalidation — all keys)**:
In broadcast mode with no PREFIX specified, when the tracking table is full and evicts entries, Redis sends a null invalidation:
```
>2
$10
invalidate
*0
```
This signals the client to flush their entire cache.

### 3. Broadcast vs Non-Broadcast Invalidation Rules

#### **Non-Broadcast Mode (Default)**

- Client must read a key to start tracking it
- Invalidation only sent for keys explicitly accessed by this client
- OPTIN: Requires `CLIENT CACHING yes` before read command
- OPTOUT: Tracks all reads unless `CLIENT CACHING no` is used

#### **Broadcast Mode (BCAST flag)**

- Client receives invalidations for all keys matching registered PREFIXes
- Does NOT require reading the key first
- If no PREFIX specified: receives invalidations for ALL keys (high overhead)
- PREFIX matching: key must start with at least one registered prefix

**Example**:
```
CLIENT TRACKING ON BCAST PREFIX user: PREFIX product:
```
- Client receives invalidations for `user:123`, `user:456`, `product:abc`
- Does NOT receive invalidations for `order:789`

### 4. Prefix Matching for Invalidations

**Prefix matching rules** (applies to BCAST mode):
1. Key is compared against all registered prefixes using `std.mem.startsWith()`
2. If ANY prefix matches, invalidation is sent
3. If NO prefix matches, no invalidation
4. If tracking_prefixes is empty in BCAST mode, ALL keys match (broadcast everything)

**Storage**:
- `ClientInfo.tracking_prefixes: std.ArrayList([]const u8)`
- Prefixes are stored as-is (case-sensitive)
- Multiple prefixes supported

**Example**:
- Registered prefixes: `["user:", "session:"]`
- Key `"user:123"` → MATCH (starts with `"user:"`)
- Key `"session:abc"` → MATCH (starts with `"session:"`)
- Key `"product:xyz"` → NO MATCH
- Key `"user"` → NO MATCH (requires exact prefix match)

### 5. REDIRECT Client Message Delivery

**REDIRECT behavior**:
- `tracking_redirect > 0`: Send invalidation to the specified client ID
- `tracking_redirect == -1` or `tracking_redirect == 0`: Send to self (tracking client)

**REDIRECT target validation**:
- Target client must exist at the time of invalidation
- If target client disconnected: message is dropped silently
- RESP3-only feature: REDIRECT target receives `tracking-redir-broken` push if original client disconnects

**Use case**:
- Connection pooling / proxies
- One connection tracks, another receives invalidations
- Useful when the tracking connection is a worker that doesn't maintain state

**Implementation**:
```zig
const target_client_id = if (tracked_info.tracking_redirect > 0)
    @as(u64, @intCast(tracked_info.tracking_redirect))
else
    tracked_id; // Send to self
```

### 6. NOLOOP Behavior

**NOLOOP flag**: Prevents a client from receiving invalidations for keys it modified itself.

**Behavior**:
- If `tracking_noloop == true` AND `modifier_client_id == tracking_client_id`:
  - Do NOT send invalidation message
  - **Still remove key from tracking table** (important!)
- Client must re-read the key to re-enable tracking

**Why this matters**:
- Reduces unnecessary notifications (client knows they wrote the key)
- Still enforces tracking table cleanup (prevents stale tracking)

**Example**:
```
CLIENT TRACKING ON NOLOOP
GET mykey              # Start tracking mykey
SET mykey newvalue     # Modify mykey
                       # → NO invalidation sent (NOLOOP)
                       # → mykey removed from tracking table
GET mykey              # Re-read to re-track
```

### 7. Integration with Command Execution Flow

**Invalidation trigger points**:

1. **After successful write command execution**:
   - Command handler calls `notifyInvalidation(key, client_id)`
   - Invalidation messages generated and queued for delivery

2. **After key expiration** (passive or active):
   - Expiration handler calls `notifyInvalidation(key, -1)` (no modifier client)
   - NOLOOP does not apply (no modifier to skip)

3. **After key eviction** (maxmemory-policy):
   - Eviction handler calls `notifyInvalidation(key, -1)`

**Delivery mechanism**:

Option A: **Immediate push to client output buffer** (simpler, matches Redis behavior)
- Generate invalidation messages in `getInvalidationMessages()`
- For each target client, append RESP3 push message to output buffer
- Messages sent asynchronously (don't block command execution)

Option B: **Queue and deliver on next event loop iteration** (more complex, better concurrency)
- Queue invalidation messages
- Background thread/task delivers messages
- Risk: more complex, potential message ordering issues

**Recommendation**: Use **Option A** for Iteration 261 (immediate push). Option B can be optimized later if needed.

**Command handler pattern**:
```zig
// After modifying key in SET command
try notifyInvalidation(store, client_registry, key, client_id);
```

### 8. Tracking Table Lifecycle

**Key removal from tracking table**:

1. **On invalidation**: Remove key from tracking table after sending messages
   - Clients must re-read to re-track
   - Applies to ALL tracking clients (even if NOLOOP suppressed message)

2. **On key deletion**: Remove key from tracking table immediately
   - `removeKeyFromTracking(key)`

3. **On client disconnect**: Remove client from all tracking table entries
   - `removeClientFromTracking(client_id)`
   - Cleanup prevents memory leaks

4. **On TRACKING OFF**: Remove client from all tracking table entries
   - Same as disconnect

**Key NOT removed**:
- Read operations (GET, MGET, etc.) do not remove keys
- Successful tracking access adds/updates tracking table

---

## Implementation Plan

### Phase 1: Core Invalidation Delivery Function

**File**: `src/commands/client.zig`

Add function to send invalidation messages:

```zig
/// Send invalidation messages to target clients
/// Called after a write command modifies a key
pub fn sendInvalidationMessages(
    registry: *ClientRegistry,
    messages: []InvalidationMessage,
    writer_factory: anytype, // Function to get Writer for client_id
) !void {
    for (messages) |msg| {
        // Get target client info
        const client_info = registry.clients.get(msg.client_id) orelse continue;

        // Get Writer for this client
        const writer = writer_factory(msg.client_id) orelse continue;

        // Build RESP3 push message
        // >2\r\n$10\r\ninvalidate\r\n*1\r\n$N\r\nKEY\r\n
        try writer.writePushInvalidation(&.{msg.key});
    }
}
```

**New Writer method**: `writer.writePushInvalidation(keys: []const []const u8)`

### Phase 2: RESP3 Push Message Writer

**File**: `src/protocol/writer.zig`

Add push message support:

```zig
/// Write RESP3 push invalidation message
/// Format: >2\r\n$10\r\ninvalidate\r\n*N\r\n<keys>
pub fn writePushInvalidation(self: *Writer, keys: []const []const u8) !void {
    // Push type: >2\r\n
    try self.stream.writeAll(">2\r\n");

    // First element: bulk string "invalidate"
    try self.stream.print("${d}\r\ninvalidate\r\n", .{@as(usize, 10)});

    // Second element: array of keys
    try self.stream.print("*{d}\r\n", .{keys.len});
    for (keys) |key| {
        try self.stream.print("${d}\r\n{s}\r\n", .{ key.len, key });
    }
}
```

### Phase 3: Integration with Write Commands

**Pattern**: After any write operation, call invalidation hook.

**Example (SET command)** in `src/commands/strings.zig`:

```zig
pub fn set(/* ... */) !void {
    // ... existing SET logic ...

    // After successful write, trigger invalidation
    defer {
        notifyInvalidation(store, client_registry, key, client_id) catch |err| {
            std.log.warn("Failed to send invalidation for key '{s}': {}", .{ key, err });
        };
    }

    // ... rest of command ...
}
```

**Centralized invalidation function** (new file or in `client.zig`):

```zig
pub fn notifyInvalidation(
    store: *memory.MemoryStore,
    registry: *ClientRegistry,
    key: []const u8,
    modifier_client_id: u64,
) !void {
    // Generate invalidation messages
    const messages = try registry.getInvalidationMessages(
        key,
        modifier_client_id,
        store.allocator,
    );
    defer {
        for (messages) |*msg| msg.deinit(store.allocator);
        store.allocator.free(messages);
    }

    // Send messages to target clients
    try sendInvalidationMessages(registry, messages, getClientWriter);

    // Remove key from tracking table (even if no messages sent)
    registry.removeKeyFromTracking(key);
}
```

### Phase 4: Write Command Coverage

**Commands to instrument** (minimum viable set for Iteration 261):

| Command | File | Trigger Point |
|---------|------|---------------|
| SET | strings.zig | After successful write |
| DEL | keys.zig | After deletion |
| EXPIRE | keys.zig | After expiration update |
| HSET | hashes.zig | After field write |
| LPUSH | lists.zig | After push |
| SADD | sets.zig | After add |
| ZADD | sorted_sets.zig | After add |

**Full coverage** (defer to future iterations if needed):
- All 50+ write commands across all data types
- Passive/active expiration hooks
- Eviction policy hooks

### Phase 5: Error Handling

**Scenarios**:
1. **REDIRECT target disconnected**: Drop message silently
2. **Writer allocation failure**: Log warning, continue
3. **Client output buffer full**: Redis behavior is to close client; consider queueing or dropping
4. **Tracking table lock contention**: Already handled by mutex in `ClientRegistry`

---

## Testing Strategy

### Unit Tests

**File**: `tests/test_client_tracking_invalidation.zig`

1. **Basic invalidation delivery**:
   - Client A tracks key, Client B modifies key
   - Verify Client A receives push message
   - Verify key removed from tracking table

2. **NOLOOP suppression**:
   - Client tracks key, then modifies it with NOLOOP
   - Verify NO push message sent
   - Verify key still removed from tracking table
   - Verify re-read restarts tracking

3. **REDIRECT**:
   - Client A tracks with REDIRECT to Client B
   - Modify key
   - Verify Client B receives push, not Client A

4. **REDIRECT target disconnected**:
   - Client A tracks with REDIRECT to Client B
   - Client B disconnects
   - Modify key
   - Verify no crash, message dropped

5. **Broadcast prefix matching**:
   - Client tracks with BCAST PREFIX "user:"
   - Modify `user:123`, `product:456`
   - Verify only `user:123` invalidation sent

6. **Multiple keys in one invalidation**:
   - Client tracks `key1`, `key2`, `key3`
   - Modify all three in MSET
   - Verify single push with array of 3 keys

7. **OPTIN mode**:
   - Client tracks with OPTIN
   - Read without `CLIENT CACHING yes` → no tracking
   - Modify key → no invalidation
   - Read with `CLIENT CACHING yes` → tracking
   - Modify key → invalidation sent

8. **OPTOUT mode**:
   - Client tracks with OPTOUT
   - Read → tracking
   - Modify → invalidation
   - `CLIENT CACHING no`, read → no tracking
   - Modify → no invalidation

### Integration Tests

**File**: `tests/integration_test.sh` or `tests/test_integration.zig`

1. **End-to-end RESP3 flow**:
   ```bash
   HELLO 3
   CLIENT TRACKING ON
   GET mykey
   # In another connection:
   SET mykey newvalue
   # Verify first connection receives:
   # >2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nmykey\r\n
   ```

2. **Cross-command invalidation**:
   - Track key with GET
   - Invalidate with SET, DEL, EXPIRE, HSET (if hash), LPUSH (if list), etc.
   - Verify invalidation for all write types

3. **Concurrent tracking**:
   - 10 clients tracking same key
   - 1 client modifies key
   - Verify all 10 receive invalidation
   - Verify key removed from tracking table

---

## RESP3 Protocol Example

### Scenario: Client-side cache invalidation

**Client A** (RESP3):
```
< HELLO 3
> %7\r\n...

< CLIENT TRACKING ON
> +OK\r\n

< GET user:123
> $5\r\nhello\r\n
```

**Client B** (any protocol):
```
< SET user:123 goodbye
> +OK\r\n
```

**Client A** receives push message (out-of-band):
```
> >2\r\n
> $10\r\ninvalidate\r\n
> *1\r\n
> $8\r\nuser:123\r\n
```

Client A's cache now knows `user:123` is stale and must re-fetch.

---

## Edge Cases

1. **Key modified by multiple commands in transaction**:
   - MULTI, SET key1, SET key2, EXEC
   - Each SET triggers invalidation independently
   - Clients may receive multiple push messages

2. **Key renamed**:
   - RENAME oldkey newkey
   - Invalidation sent for `oldkey` (tracked clients lose track)
   - No automatic tracking of `newkey`

3. **Key expired (passive)**:
   - Client tries to GET expired key
   - Expiration logic deletes key
   - Invalidation sent with `modifier_client_id = -1` (no NOLOOP)

4. **Tracking table full**:
   - Already handled in Iteration 260 (evict random entry)
   - Optional: send null invalidation `*0` to evicted clients (broadcast mode)

5. **Client disconnects with active tracking**:
   - `removeClientFromTracking()` called on disconnect
   - Cleanup prevents memory leaks

---

## Performance Considerations

1. **Mutex contention**: `ClientRegistry.mutex` protects tracking table
   - Read-write split could improve concurrency (future optimization)
   - Current mutex is acceptable for MVP

2. **Invalidation message batching**:
   - Redis batches multiple key invalidations into one push message
   - Current design sends one message per key (simpler)
   - Future: batch invalidations for same client

3. **Output buffer pressure**:
   - Many invalidations can fill output buffers
   - Consider dropping messages or closing slow clients (Redis behavior)

4. **Memory allocation**:
   - `getInvalidationMessages()` allocates message array
   - Freed immediately after sending
   - No long-lived allocations

---

## Dependencies

**Requires** (already complete):
- Iteration 260: CLIENT TRACKING core infrastructure
- RESP3 support (HELLO 3)
- ClientRegistry tracking table

**Enables** (future work):
- Full client-side caching feature
- Redis compatibility for cache-aware clients
- Performance optimization for read-heavy workloads

---

## Success Criteria

1. ✅ All unit tests pass (8+ scenarios)
2. ✅ Integration tests validate end-to-end RESP3 push delivery
3. ✅ `zig build test` passes with zero failures
4. ✅ No memory leaks detected
5. ✅ Differential testing against real Redis (byte-level RESP3 comparison)
6. ✅ Documentation updated in README.md

---

## References

- [Redis Client-Side Caching](https://redis.io/docs/latest/develop/use/client-side-caching/)
- [RESP3 Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [CLIENT TRACKING Command](https://redis.io/commands/client-tracking/)
- Iteration 260: CLIENT TRACKING Core Infrastructure
- `src/commands/client.zig`: ClientRegistry implementation
- `src/protocol/writer.zig`: RESP3 writer

---

## Implementation Notes

**Estimated complexity**: Medium (4-6 hours)

**Files to modify**:
- `src/protocol/writer.zig` — Add `writePushInvalidation()`
- `src/commands/client.zig` — Add `sendInvalidationMessages()`, `notifyInvalidation()`
- `src/commands/strings.zig` — Instrument SET, MSET, etc.
- `src/commands/keys.zig` — Instrument DEL, EXPIRE, etc.
- `src/commands/hashes.zig` — Instrument HSET, HDEL, etc. (if time permits)
- `tests/test_client_tracking_invalidation.zig` — Unit tests (new file)

**Files to create**:
- `tests/test_client_tracking_invalidation.zig` — Comprehensive unit tests
- `docs/iterations/iteration-261-spec.md` — This document

**Defer to future iterations**:
- Full write command coverage (50+ commands) — prioritize core commands (SET, DEL, EXPIRE, HSET, LPUSH, SADD, ZADD)
- Null invalidation (`*0`) for table eviction
- `tracking-redir-broken` push messages for disconnected REDIRECT targets
- Message batching optimization
- Passive/active expiration hooks
- Eviction policy hooks

---

**End of Specification**
