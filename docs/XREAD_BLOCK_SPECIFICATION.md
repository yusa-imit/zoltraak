# XREAD and XREADGROUP BLOCK Implementation Specification

**Date**: 2026-03-06
**Target**: Zoltraak Redis Implementation
**Status**: Planning Phase
**Priority**: Phase 1.3 Stream Command Gaps (P0)

---

## Executive Summary

This document provides a comprehensive specification for implementing true blocking semantics for the `XREAD BLOCK` and `XREADGROUP BLOCK` commands in Zoltraak. The current implementation parses the BLOCK parameter but treats it as immediate-return (timeout=0 semantics). This specification details the requirements for event-loop based blocking that matches Redis behavior.

---

## 1. XREAD BLOCK Specification

### 1.1 Command Syntax

```
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
```

**Parameter Order**:
- `COUNT` and `BLOCK` options can appear in any order
- `STREAMS` must be the final option before key/ID pairs
- Key count must equal ID count (balanced pairs)

### 1.2 BLOCK Option Semantics

**Timeout Values**:
- `BLOCK 0` — Block indefinitely until data arrives
- `BLOCK n` (n > 0) — Block for n milliseconds, then timeout
- Timeout resolution: milliseconds (unlike other Redis blocking commands which use seconds)
- Server timeout resolution: ~0.1 seconds (100ms), but millisecond precision accepted

**Blocking Behavior**:
1. **Data Already Available**: If any stream has entries > specified ID when command executes, returns immediately (synchronous execution, BLOCK parameter ignored)
2. **No Data Available**: Command blocks the client connection until:
   - New entry arrives on any monitored stream (via `XADD`)
   - Timeout expires (returns nil)
   - Client disconnects (connection cleanup)

**Broadcast Model** (Critical Difference from BLPOP):
- All blocked clients waiting on a stream receive the new data
- Entries are NOT removed from stream (fan-out, not pop)
- Multiple consumers can read same data independently

### 1.3 Return Values

**Success Case** (Data Available):
```
RESP2 Array Reply:
*2                          # 2 streams with data
*2                          # Stream 1
$8                          # Key name length
mystream                    # Key name
*2                          # 2 entries
*2                          # Entry 1
$15
1526984818136-0             # Entry ID
*4                          # 4 field-value pairs
$6
field1
$6
value1
...
```

**Timeout Case** (No Data):
```
RESP2: $-1 (nil bulk string)
RESP3: _   (null)
```

**Structure Guarantees**:
- Result is array of `[key_name, entries_array]` pairs
- Only streams with available data are included in response
- Field-value order matches insertion order from `XADD`
- Entry IDs are in ascending order

### 1.4 Special ID Handling

**ID = "$" (Dollar Sign)**:
- Semantic: "Only messages added AFTER this BLOCK command executes"
- Use Case: First iteration of consumer loop
- Implementation: Capture current stream `last_id` when blocking starts, return entries > `last_id`
- **Critical**: Use `$` only on first call; subsequent calls must use actual last received ID
- **Missing entries risk**: If you always use `$`, entries added between reads are lost

**Example Pattern**:
```bash
# First call - get new entries only
XREAD BLOCK 5000 COUNT 100 STREAMS mystream $

# Returns: [["mystream", [["1526999644174-3", ["foo", "bar"]]]]]

# Subsequent calls - use last ID to avoid gaps
XREAD BLOCK 5000 COUNT 100 STREAMS mystream 1526999644174-3
```

**ID = "+" (Plus Sign - Redis 7.4+)**:
- Semantic: "Get the last entry in stream"
- Use Case: Multi-stream monitoring when only last entry matters
- `COUNT` option ignored when using `+`
- More efficient than `XREVRANGE` for multiple streams

**Incomplete IDs**:
- `XREAD STREAMS mystream 0` → Equivalent to `XREAD STREAMS mystream 0-0`
- Missing sequence part defaults to `0`

### 1.5 Interaction with COUNT Option

**COUNT Semantics**:
- Limits maximum entries returned **per stream**
- Applied independently to each stream
- If no COUNT specified, unlimited entries returned

**Complexity Impact**:
- With COUNT: O(1) per stream if M (returned entries) is constant
- Without COUNT: O(M) per stream where M = actual entries returned

**Example**:
```bash
XREAD COUNT 2 BLOCK 1000 STREAMS stream1 stream2 0-0 0-0
# Returns max 2 entries from stream1 AND max 2 entries from stream2
# Total max = 4 entries (2 per stream)
```

### 1.6 Edge Cases

**Case 1: Multiple Streams - Partial Availability**
```bash
XREAD BLOCK 5000 STREAMS s1 s2 s3 $ $ $
# Blocks until ANY stream gets new data
# Returns only streams with data (may be 1, 2, or 3 streams)
```

**Case 2: Concurrent XADD Performance**
- When N clients are blocked on stream `key`, each `XADD` operation pays O(N) cost to notify all waiters
- Fan-out to all blocked clients happens synchronously
- Consider this for high-concurrency scenarios

**Case 3: Client Disconnect During Block**
- Server must clean up blocked client state
- No response sent (connection already closed)
- Entry list for that client removed from blocking queue

**Case 4: Stream Deleted During Block**
- If stream is deleted via `DEL`, blocked clients should be notified with empty result for that stream
- Implementation-specific: Redis behavior undefined

---

## 2. XREADGROUP BLOCK Specification

### 2.1 Command Syntax

```
XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds]
  [CLAIM min-idle-time] [NOACK] STREAMS key [key ...] id [id ...]
```

**Required Parameters**:
- `GROUP group consumer` — Consumer group and consumer identifier
- `STREAMS key [key ...] id [id ...]` — Stream keys and start IDs

**Optional Parameters**:
- `COUNT count` — Max messages to return
- `BLOCK milliseconds` — Blocking timeout
- `CLAIM min-idle-time` — Auto-claim pending messages (Redis 8.4+)
- `NOACK` — Skip adding to PEL (auto-acknowledge)

### 2.2 BLOCK Option Semantics

**Timeout Values**: Same as `XREAD BLOCK`
- `BLOCK 0` — Infinite wait
- `BLOCK n` — Wait n milliseconds

**Blocking Behavior**:
1. **New Messages (ID = ">")**: Block until new messages available that haven't been delivered to ANY consumer in the group
2. **Pending Messages (ID = "0" or specific ID)**: BLOCK, NOACK, and CLAIM are **IGNORED** — returns immediately with pending entries
3. **Auto-claimed Messages (CLAIM + ID = ">")**: Returns pending entries idle ≥ min-idle-time, THEN blocks for new messages if needed

**Consumer Group Semantics**:
- Each message delivered to ONE consumer only (unlike XREAD broadcast)
- Delivered messages added to Pending Entries List (PEL) unless NOACK
- Must acknowledge via `XACK` to remove from PEL

### 2.3 Return Values

**Success Case** (Standard):
```
RESP2 Array Reply:
*1                          # 1 stream
*2                          # [key, entries]
$8
mystream
*1                          # 1 entry
*2                          # [ID, fields]
$15
1526999644174-0
*4                          # Field-value pairs
$4
name
$5
Alice
...
```

**Success Case with CLAIM** (Additional Metadata):
```
RESP2 Array Reply:
*1
*2
$8
mystream
*1
*4                          # 4 elements per entry with CLAIM
$15
1526999644174-0             # Entry ID
*4                          # Field-value pairs
...
:300000                     # Milliseconds since last delivery
:5                          # Delivery count
```

**Timeout Case**: Same as XREAD (nil/null)

**RESP3 Format**: Map reply instead of nested arrays

### 2.4 Special ID Handling

**ID = ">" (Only for XREADGROUP)**:
- Semantic: "New messages never delivered to any consumer"
- **Without CLAIM**: Returns only brand-new messages
- **With CLAIM min-idle-time**: Returns pending entries idle ≥ min-idle-time, then new messages
- **Blocks if no eligible messages**: Waits for new `XADD` or messages to become claimable

**ID = "0" (Zero)**:
- Semantic: "All pending messages for this consumer"
- Returns entries in consumer's PEL (unacknowledged messages)
- **BLOCK, NOACK, CLAIM ignored** — always returns immediately
- Use for recovery after consumer crash

**ID = Specific ID (e.g., "1526999644174-0")**:
- Returns entries with ID > specified value
- Used to resume from last processed position
- **BLOCK, NOACK, CLAIM ignored** — returns immediately

**Example Recovery Pattern**:
```bash
# After crash - recover pending messages
XREADGROUP GROUP mygroup consumer1 STREAMS mystream 0
# Returns all pending for consumer1

# Process and acknowledge
XACK mystream mygroup 1526999644174-0 1526999644174-1

# Resume normal operation - now blocks
XREADGROUP GROUP mygroup consumer1 BLOCK 2000 STREAMS mystream >
```

### 2.5 NOACK Interaction with BLOCK

**NOACK Semantics**:
- Delivered messages NOT added to PEL
- Equivalent to auto-acknowledging upon read
- No `XACK` required

**Trade-offs**:
- **Pro**: Simpler code, no acknowledgment overhead
- **Con**: Lost messages cannot be recovered, no delivery tracking

**Blocking Behavior**:
- NOACK works normally with BLOCK
- Messages are delivered and immediately considered consumed
- If consumer crashes before processing, message is lost

**Example**:
```bash
XREADGROUP GROUP mygroup consumer1 BLOCK 2000 NOACK STREAMS mystream >
# Blocks for new messages
# Delivered messages NOT added to PEL
# No XACK needed
```

**CLAIM + NOACK Interaction**:
- When using `CLAIM min-idle-time`, auto-claimed pending entries still go to PEL
- `NOACK` only applies to newly read entries, NOT claimed entries
- This is a critical distinction for reliability

### 2.6 Edge Cases

**Case 1: Pending Message Deletion**
```bash
# Entry 1-0 deleted via XDEL
XREADGROUP GROUP mygroup consumer1 STREAMS mystream 0
# Returns: [["mystream", [["1-0", nil]]]]
# PEL retains entry ID, but payload is nil
```

**Case 2: Group Does Not Exist**
```bash
XREADGROUP GROUP nonexistent consumer1 BLOCK 1000 STREAMS mystream >
# Returns: ERR NOGROUP No such consumer group for this key
# Must create group first via XGROUP CREATE
```

**Case 3: Auto-Claim with COUNT Limit**
```bash
# Stream has 20 idle pending + 200 new messages
XREADGROUP GROUP mygroup consumer1 BLOCK 1000 COUNT 100 CLAIM 5000 STREAMS mystream >
# Returns: 20 idle (longest idle first) + 80 new = 100 total
```

**Case 4: Multiple Streams with Different States**
```bash
XREADGROUP GROUP mygroup consumer1 BLOCK 2000 STREAMS s1 s2 s3 > > >
# Blocks until ANY stream has eligible messages
# Returns only streams with data (may be subset)
```

**Case 5: Concurrent Delivery to Same Consumer**
```bash
# Consumer1 gets same message twice (e.g., via XCLAIM then re-read)
# Delivery counter increments
# Last-delivery timestamp updates
# Viewable via XPENDING
```

---

## 3. Key Differences: XREAD vs XREADGROUP BLOCK

| Aspect | XREAD BLOCK | XREADGROUP BLOCK |
|--------|------------|------------------|
| **Message Delivery** | Broadcast (all clients get same data) | Exclusive (one consumer per message) |
| **Consumer Groups** | Not supported | Required (GROUP parameter) |
| **Pending Tracking** | N/A | Messages tracked in PEL |
| **Acknowledgment** | N/A | XACK required (unless NOACK) |
| **Special ID** | `$` = new messages | `>` = new + auto-claimed (with CLAIM) |
| **History Access** | Any ID supported | `0` or specific ID for pending messages |
| **BLOCK Ignored Cases** | Never ignored | Ignored when ID ≠ `>` |
| **XADD Cost** | O(N) to notify N blocked clients | O(N) to notify N blocked consumers |
| **Recovery** | Not applicable | Read pending with ID `0` |

---

## 4. Implementation Requirements for Zoltraak

### 4.1 Current State (Lines 224-234, 412-422 in streams_advanced.zig)

```zig
// XREAD - Current stub implementation
} else if (std.ascii.eqlIgnoreCase(arg, "BLOCK")) {
    i += 1;
    if (i >= args.len) return w.writeError("ERR syntax error");
    const _block_str = switch (args[i]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    // Parse but ignore BLOCK for now (blocking not implemented)
    _ = std.fmt.parseInt(i64, _block_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
}

// XREADGROUP - Same stub pattern
```

**Issues**:
1. BLOCK parameter parsed but ignored
2. No timeout tracking
3. No event-loop integration
4. No client blocking queue
5. Always returns immediately (treats all as timeout=0)

### 4.2 Required Components

**1. Blocking Queue Data Structure**

```zig
// Per-stream blocking queue
const BlockedClient = struct {
    client_id: usize,
    start_id: StreamId,     // ID to compare against
    count: ?usize,          // COUNT limit
    timeout_ms: i64,        // Milliseconds to wait (0 = infinite)
    start_time: i64,        // Unix timestamp when block started
    keys: [][]const u8,     // All keys this client is waiting on
};

// Global blocking registry (in Storage or Server)
blocked_xread_clients: std.StringHashMap(std.ArrayList(BlockedClient)),
blocked_xreadgroup_clients: std.StringHashMap(std.ArrayList(struct {
    client: BlockedClient,
    group: []const u8,
    consumer: []const u8,
    noack: bool,
})),
```

**2. Client Tracking**

```zig
// In Server or Connection struct
const ClientState = struct {
    id: usize,
    is_blocked: bool,
    blocked_on_stream: ?[]const u8,
    blocked_command: ?enum { xread, xreadgroup },
    response_buffer: ?[]const u8,  // Pre-computed response
};
```

**3. Event-Loop Integration**

```zig
// Main server loop needs timeout checking
pub fn serverLoop(server: *Server) !void {
    var timeout = std.time.ns_per_ms * 100;  // 100ms tick for timeout checks

    while (server.running) {
        // Accept connections with timeout
        const events = try server.poll.poll(timeout);

        // Process events...

        // Check for expired blocked clients
        try server.checkBlockedClientTimeouts();
    }
}
```

**4. XADD Notification Hook**

```zig
// After XADD adds entry, notify blocked clients
pub fn xadd(...) !StreamId {
    // ... add entry to stream ...

    const entry_id = // ... new entry ID ...

    // Notify blocked XREAD clients
    try self.notifyBlockedXreadClients(key, entry_id);

    // Notify blocked XREADGROUP clients
    try self.notifyBlockedXreadgroupClients(key, entry_id);

    return entry_id;
}
```

**5. Timeout Checking**

```zig
fn checkBlockedClientTimeouts(self: *Server) !void {
    const now_ms = std.time.milliTimestamp();

    // Check XREAD clients
    var iter = self.storage.blocked_xread_clients.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        const clients = entry.value_ptr;

        var i: usize = 0;
        while (i < clients.items.len) {
            const client = &clients.items[i];

            if (client.timeout_ms == 0) {
                // Infinite wait
                i += 1;
                continue;
            }

            const elapsed = now_ms - client.start_time;
            if (elapsed >= client.timeout_ms) {
                // Timeout expired - send nil response
                try self.sendNilResponse(client.client_id);
                _ = clients.swapRemove(i);
                // Don't increment i (swapRemove moved last item to this position)
            } else {
                i += 1;
            }
        }
    }

    // Similar for XREADGROUP clients...
}
```

**6. Client Disconnect Cleanup**

```zig
fn handleClientDisconnect(self: *Server, client_id: usize) !void {
    // Remove from all blocking queues
    var iter = self.storage.blocked_xread_clients.iterator();
    while (iter.next()) |entry| {
        const clients = entry.value_ptr;
        var i: usize = 0;
        while (i < clients.items.len) {
            if (clients.items[i].client_id == client_id) {
                _ = clients.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    // Similar for XREADGROUP...
}
```

### 4.3 Command Handler Changes

**XREAD with Blocking**:

```zig
pub fn cmdXread(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, client_id: usize) !?[]const u8 {
    // ... parse COUNT, BLOCK, STREAMS ...

    var block_ms: ?i64 = null;
    if (/* BLOCK option found */) {
        block_ms = std.fmt.parseInt(i64, block_str, 10) catch {
            return error.InvalidInteger;
        };
    }

    // Check if data already available
    const has_data = try storage.checkStreamsHaveData(keys, ids, count);

    if (has_data or block_ms == null) {
        // Immediate return path
        return try buildXreadResponse(allocator, storage, keys, ids, count);
    }

    // No data and blocking requested - enqueue client
    try storage.enqueueBlockedXreadClient(.{
        .client_id = client_id,
        .keys = try allocator.dupe([]const u8, keys),
        .start_ids = try allocator.dupe([]const u8, ids),
        .count = count,
        .timeout_ms = block_ms.?,
        .start_time = std.time.milliTimestamp(),
    });

    // Return null to indicate blocking (no response yet)
    return null;
}
```

**XREADGROUP with Blocking**:

```zig
pub fn cmdXreadgroup(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, client_id: usize) !?[]const u8 {
    // ... parse GROUP, consumer, COUNT, BLOCK, NOACK, STREAMS ...

    // Check if ID is ">" (only then blocking applies)
    const is_new_messages = std.mem.eql(u8, id_str, ">");

    if (!is_new_messages) {
        // BLOCK/NOACK/CLAIM ignored for pending message reads
        return try buildXreadgroupResponse(allocator, storage, ...);
    }

    // ... similar blocking logic as XREAD ...
}
```

### 4.4 Storage API Extensions

**New Methods Needed**:

```zig
// Storage struct additions
pub fn enqueueBlockedXreadClient(self: *Storage, client: BlockedClient) !void;
pub fn enqueueBlockedXreadgroupClient(self: *Storage, client: BlockedXreadgroupClient) !void;
pub fn notifyBlockedXreadClients(self: *Storage, key: []const u8, new_entry_id: StreamId) !void;
pub fn notifyBlockedXreadgroupClients(self: *Storage, key: []const u8, new_entry_id: StreamId) !void;
pub fn removeBlockedClient(self: *Storage, client_id: usize) void;
pub fn checkStreamsHaveData(self: *Storage, keys: [][]const u8, start_ids: [][]const u8, count: ?usize) !bool;
```

### 4.5 Complexity Analysis

**XADD with N Blocked Clients**:
- Time: O(N) to notify all waiting clients
- Space: O(N) for blocking queue

**BLOCK Timeout Checking**:
- Time: O(M) where M = total blocked clients across all streams
- Frequency: Every ~100ms (configurable tick)

**Memory per Blocked Client**:
- ~200 bytes (client metadata + key copies)
- Max blocked clients: configurable limit (e.g., 10,000)

---

## 5. Testing Strategy

### 5.1 Unit Tests

**Test 1: XREAD BLOCK with Immediate Data**
```zig
test "XREAD BLOCK returns immediately if data available" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    _ = try storage.xadd("stream1", "*", &[_][]const u8{"f1", "v1"}, null);

    const result = try cmdXread(testing.allocator, &storage, /* BLOCK 1000 STREAMS stream1 0-0 */, 1);
    defer testing.allocator.free(result.?);

    // Should return data immediately, not block
    try testing.expect(result != null);
    try testing.expect(std.mem.indexOf(u8, result.?, "stream1") != null);
}
```

**Test 2: XREAD BLOCK with Timeout**
```zig
test "XREAD BLOCK returns nil on timeout" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    _ = try storage.xadd("stream1", "*", &[_][]const u8{"f1", "v1"}, null);

    const result = try cmdXread(testing.allocator, &storage, /* BLOCK 100 STREAMS stream1 $ */, 1);

    // Should return null (blocking)
    try testing.expect(result == null);

    // Wait 150ms
    std.time.sleep(150 * std.time.ns_per_ms);

    // Check timeout handler sent nil
    const client = storage.getBlockedClient(1);
    try testing.expect(client == null);  // Removed from queue
}
```

**Test 3: XREADGROUP BLOCK with ID = 0 (Should Not Block)**
```zig
test "XREADGROUP BLOCK ignored when ID is not >" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    _ = try storage.xadd("stream1", "*", &[_][]const u8{"f1", "v1"}, null);
    try storage.xgroupCreate("stream1", "group1", "0");

    const result = try cmdXreadgroup(testing.allocator, &storage, /* BLOCK 1000 STREAMS stream1 0 */, 1);
    defer testing.allocator.free(result.?);

    // Should return immediately (BLOCK ignored)
    try testing.expect(result != null);
}
```

**Test 4: XREAD $ Special ID**
```zig
test "XREAD $ only returns entries added after block" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    _ = try storage.xadd("stream1", "*", &[_][]const u8{"old", "data"}, null);

    const result = try cmdXread(testing.allocator, &storage, /* BLOCK 100 STREAMS stream1 $ */, 1);

    // Should block (returns null)
    try testing.expect(result == null);

    // Add new entry
    _ = try storage.xadd("stream1", "*", &[_][]const u8{"new", "data"}, null);

    // Client should be notified with new entry only
    const client_response = storage.getClientResponse(1);
    try testing.expect(std.mem.indexOf(u8, client_response, "new") != null);
    try testing.expect(std.mem.indexOf(u8, client_response, "old") == null);
}
```

### 5.2 Integration Tests

**Test 1: End-to-End XREAD BLOCK Flow**
```bash
#!/bin/bash
# Start zoltraak server
./zig-out/bin/zoltraak &
SERVER_PID=$!
sleep 1

# Terminal 1: Block waiting for data
redis-cli XREAD BLOCK 5000 STREAMS mystream $ &
CLIENT_PID=$!

# Terminal 2: Add data after 1 second
sleep 1
redis-cli XADD mystream * field1 value1

# Check client received data
wait $CLIENT_PID
if [ $? -eq 0 ]; then
    echo "PASS: Client received data"
else
    echo "FAIL: Client did not receive data"
fi

kill $SERVER_PID
```

**Test 2: XREAD BLOCK Timeout**
```bash
# Block with short timeout, no data
START=$(date +%s%3N)
redis-cli XREAD BLOCK 500 STREAMS mystream $
END=$(date +%s%3N)
ELAPSED=$((END - START))

if [ $ELAPSED -ge 500 ] && [ $ELAPSED -le 600 ]; then
    echo "PASS: Timeout within expected range (${ELAPSED}ms)"
else
    echo "FAIL: Timeout incorrect (${ELAPSED}ms)"
fi
```

**Test 3: XREADGROUP BLOCK with Consumer Groups**
```bash
redis-cli XADD stream1 * data first
redis-cli XGROUP CREATE stream1 group1 0

# Consumer 1 blocks
redis-cli XREADGROUP GROUP group1 consumer1 BLOCK 5000 STREAMS stream1 \> &
C1_PID=$!

# Consumer 2 blocks
redis-cli XREADGROUP GROUP group1 consumer2 BLOCK 5000 STREAMS stream1 \> &
C2_PID=$!

sleep 1

# Add data - only ONE consumer should receive it
redis-cli XADD stream1 * data second

wait $C1_PID
C1_EXIT=$?
wait $C2_PID
C2_EXIT=$?

# One should get data (exit 0), one should timeout (exit 1)
if [ $C1_EXIT -eq 0 ] && [ $C2_EXIT -ne 0 ]; then
    echo "PASS: Exclusive delivery to consumer1"
elif [ $C2_EXIT -eq 0 ] && [ $C1_EXIT -ne 0 ]; then
    echo "PASS: Exclusive delivery to consumer2"
else
    echo "FAIL: Both or neither consumer received data"
fi
```

### 5.3 Performance Tests

**Test 1: Fan-out Scalability**
```bash
# 100 clients blocking on same stream
for i in {1..100}; do
    redis-cli XREAD BLOCK 10000 STREAMS mystream $ &
done

# Add one entry
redis-cli XADD mystream * data test

# All clients should receive within 100ms
wait
```

**Test 2: Timeout Precision**
```bash
# Measure actual timeout vs requested
for timeout in 100 500 1000 2000; do
    START=$(date +%s%3N)
    redis-cli XREAD BLOCK $timeout STREAMS mystream $
    END=$(date +%s%3N)
    ELAPSED=$((END - START))
    ERROR=$((ELAPSED - timeout))
    echo "Requested: ${timeout}ms, Actual: ${ELAPSED}ms, Error: ${ERROR}ms"
done
```

---

## 6. Error Conditions

**ERR Syntax Errors**:
```
XREAD BLOCK abc STREAMS mystream $
# ERR value is not an integer or out of range

XREAD BLOCK STREAMS mystream $
# ERR syntax error (missing timeout)

XREAD STREAMS mystream
# ERR Unbalanced XREAD list of streams
```

**ERR NOGROUP** (XREADGROUP only):
```
XREADGROUP GROUP nonexistent consumer1 BLOCK 1000 STREAMS mystream >
# ERR NOGROUP No such consumer group for this key
```

**WRONGTYPE**:
```
SET mykey "string"
XREAD BLOCK 1000 STREAMS mykey $
# WRONGTYPE Operation against a key holding the wrong kind of value
```

---

## 7. Implementation Phases

### Phase 1: Infrastructure (Week 1)
- [ ] Add `BlockedClient` data structures to Storage
- [ ] Implement blocking queue (per-stream HashMap)
- [ ] Add timeout checking mechanism to server loop
- [ ] Implement client disconnect cleanup

### Phase 2: XREAD BLOCK (Week 2)
- [ ] Modify `cmdXread` to handle BLOCK parameter
- [ ] Implement `enqueueBlockedXreadClient`
- [ ] Implement `notifyBlockedXreadClients` in XADD
- [ ] Implement `checkStreamsHaveData` for immediate return path
- [ ] Unit tests for XREAD BLOCK

### Phase 3: XREADGROUP BLOCK (Week 3)
- [ ] Modify `cmdXreadgroup` to handle BLOCK parameter
- [ ] Implement BLOCK/NOACK/CLAIM interaction rules
- [ ] Handle ID = ">" vs other IDs (BLOCK only applies to ">")
- [ ] Implement exclusive delivery (one consumer per message)
- [ ] Unit tests for XREADGROUP BLOCK

### Phase 4: Integration & Testing (Week 4)
- [ ] End-to-end integration tests
- [ ] Performance benchmarks (fan-out, timeout precision)
- [ ] redis-benchmark compatibility tests
- [ ] Differential testing against real Redis
- [ ] Memory leak checks for blocking queue

### Phase 5: Documentation & Cleanup
- [ ] Update CLAUDE.md with XREAD/XREADGROUP BLOCK status
- [ ] Add inline documentation for blocking queue APIs
- [ ] Create examples in README.md
- [ ] Delete this specification document

---

## 8. References

### Official Redis Documentation
- [XREAD Command](https://redis.io/docs/latest/commands/xread/)
- [XREADGROUP Command](https://redis.io/docs/latest/commands/xreadgroup/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis Streams Introduction](https://redis.io/docs/latest/develop/data-types/streams/)

### Redis Source Code
- `src/t_stream.c` — Stream data structure and XADD implementation
- `src/blocked.c` — Generic blocking queue implementation
- `src/networking.c` — Client notification and response handling

### Known Issues (GitHub)
- [XREAD BLOCK=0 doesn't block](https://github.com/redis/jedis/issues/2277) — Java client issue
- [XREAD COUNT doesn't work with BLOCK](https://github.com/redis/redis/issues/5543) — Resolved in Redis 5.0.5
- [XREADGROUP block longer than timeout](https://github.com/redis/redis/issues/12998) — Edge case with very long timeouts
- [xreadgroup not blocking when timestamp ID is used](https://github.com/redis/redis/issues/7056) — Confirmed BLOCK ignored for non-">" IDs

---

## 9. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Memory leak** from blocking queue | High | Rigorous testing with `std.testing.allocator`, ensure cleanup on timeout/disconnect |
| **Deadlock** in server loop | High | Use non-blocking poll with timeout, separate blocking queue from main event loop |
| **Timeout precision issues** | Medium | Document that server uses ~100ms resolution, client expectations should account for this |
| **XADD performance degradation** | Medium | Limit max blocked clients per stream, O(N) fan-out is acceptable for typical N < 100 |
| **Client disconnect race conditions** | Medium | Atomic operations for blocking queue updates, careful locking around client state |
| **RESP protocol buffering** | Low | Pre-allocate response buffers for blocked clients, avoid per-wakeup allocations |

---

## 10. Success Criteria

**Functional**:
- [ ] `XREAD BLOCK 0` blocks indefinitely until data arrives
- [ ] `XREAD BLOCK n` times out after n milliseconds ± 100ms
- [ ] `XREAD BLOCK` returns immediately if data already available
- [ ] `XREADGROUP BLOCK` ignores BLOCK when ID ≠ ">"
- [ ] Multiple clients can block on same stream (broadcast for XREAD, exclusive for XREADGROUP)
- [ ] Client disconnect properly cleans up blocking queue

**Performance**:
- [ ] Fan-out to N blocked clients completes in O(N) time
- [ ] Timeout precision within ±10% of requested value
- [ ] No memory leaks over 1M blocking operations
- [ ] XADD latency increase < 10% with 100 blocked clients

**Compatibility**:
- [ ] 100% compatibility with Redis XREAD BLOCK behavior
- [ ] 100% compatibility with Redis XREADGROUP BLOCK behavior
- [ ] redis-cli blocking commands work correctly
- [ ] redis-benchmark XREAD/XREADGROUP tests pass

---

## Appendix A: Zig Code Snippets

### A.1 Blocking Queue Data Structure

```zig
// src/storage/blocking.zig
const std = @import("std");
const StreamId = @import("memory.zig").Value.StreamId;

pub const BlockedClient = struct {
    client_id: usize,
    keys: [][]const u8,          // Owned copies of key names
    start_ids: []StreamId,       // Parsed start IDs for each key
    count: ?usize,
    timeout_ms: i64,             // 0 = infinite
    start_time: i64,             // Milliseconds since epoch
    allocator: std.mem.Allocator,

    pub fn deinit(self: *BlockedClient) void {
        for (self.keys) |key| {
            self.allocator.free(key);
        }
        self.allocator.free(self.keys);
        self.allocator.free(self.start_ids);
    }
};

pub const BlockedXreadgroupClient = struct {
    client: BlockedClient,
    group: []const u8,           // Owned copy
    consumer: []const u8,        // Owned copy
    noack: bool,
    claim_min_idle: ?i64,        // For CLAIM option

    pub fn deinit(self: *BlockedXreadgroupClient) void {
        self.client.deinit();
        self.client.allocator.free(self.group);
        self.client.allocator.free(self.consumer);
    }
};

pub const BlockingQueue = struct {
    xread_clients: std.StringHashMap(std.ArrayList(BlockedClient)),
    xreadgroup_clients: std.StringHashMap(std.ArrayList(BlockedXreadgroupClient)),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) BlockingQueue {
        return .{
            .xread_clients = std.StringHashMap(std.ArrayList(BlockedClient)).init(allocator),
            .xreadgroup_clients = std.StringHashMap(std.ArrayList(BlockedXreadgroupClient)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BlockingQueue) void {
        var iter = self.xread_clients.iterator();
        while (iter.next()) |entry| {
            for (entry.value_ptr.items) |*client| {
                client.deinit();
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.xread_clients.deinit();

        var iter2 = self.xreadgroup_clients.iterator();
        while (iter2.next()) |entry| {
            for (entry.value_ptr.items) |*client| {
                client.deinit();
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.xreadgroup_clients.deinit();
    }

    pub fn enqueueXreadClient(self: *BlockingQueue, key: []const u8, client: BlockedClient) !void {
        const gop = try self.xread_clients.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList(BlockedClient){};
        }
        try gop.value_ptr.append(self.allocator, client);
    }

    pub fn getXreadClients(self: *BlockingQueue, key: []const u8) ?[]BlockedClient {
        const list = self.xread_clients.get(key) orelse return null;
        return list.items;
    }

    pub fn removeClient(self: *BlockingQueue, client_id: usize) void {
        // Remove from XREAD queues
        var iter = self.xread_clients.iterator();
        while (iter.next()) |entry| {
            const clients = entry.value_ptr;
            var i: usize = 0;
            while (i < clients.items.len) {
                if (clients.items[i].client_id == client_id) {
                    var removed = clients.swapRemove(i);
                    removed.deinit();
                } else {
                    i += 1;
                }
            }
        }

        // Remove from XREADGROUP queues
        var iter2 = self.xreadgroup_clients.iterator();
        while (iter2.next()) |entry| {
            const clients = entry.value_ptr;
            var i: usize = 0;
            while (i < clients.items.len) {
                if (clients.items[i].client.client_id == client_id) {
                    var removed = clients.swapRemove(i);
                    removed.deinit();
                } else {
                    i += 1;
                }
            }
        }
    }
};
```

### A.2 XADD Notification Hook

```zig
// In src/storage/memory.zig, at end of xadd function
pub fn xadd(
    self: *Storage,
    key: []const u8,
    id_str: []const u8,
    fields: []const []const u8,
    expires_at: ?i64,
) !StreamId {
    // ... existing implementation ...

    // Notify blocked clients after adding entry
    try self.notifyBlockedXreadClients(key, entry_id);
    try self.notifyBlockedXreadgroupClients(key, entry_id);

    return entry_id;
}

fn notifyBlockedXreadClients(self: *Storage, key: []const u8, new_id: StreamId) !void {
    const clients = self.blocking_queue.getXreadClients(key) orelse return;

    for (clients, 0..) |*client, i| {
        // Check if new entry is > client's start ID
        const should_notify = blk: {
            for (client.keys, client.start_ids) |client_key, start_id| {
                if (std.mem.eql(u8, client_key, key)) {
                    // Special case: $ means use last_id at block time
                    if (StreamId.compare(new_id, start_id) > 0) {
                        break :blk true;
                    }
                }
            }
            break :blk false;
        };

        if (should_notify) {
            // Build response for this client
            const response = try self.buildXreadResponse(
                client.client.allocator,
                client.keys,
                client.start_ids,
                client.count,
            );

            // Send to client via server callback
            try self.sendResponseToClient(client.client_id, response);

            // Mark for removal
            // (actual removal happens after iteration)
        }
    }

    // Remove notified clients from queue
    // (implementation detail: mark and sweep pattern)
}
```

---

## Appendix B: Timeout Precision Analysis

**Redis Server Timeout Resolution**: ~0.1 seconds (100ms)

**Zoltraak Target**: ±10% precision for timeouts ≥ 100ms

**Measurement Method**:
```bash
#!/bin/bash
# Timeout precision test
for timeout in 100 200 500 1000 2000 5000; do
    echo "Testing BLOCK $timeout ms:"

    for trial in {1..10}; do
        start=$(date +%s%3N)
        redis-cli XREAD BLOCK $timeout STREAMS test$ > /dev/null
        end=$(date +%s%3N)
        elapsed=$((end - start))
        error=$((elapsed - timeout))
        error_pct=$(awk "BEGIN {print ($error / $timeout) * 100}")

        echo "  Trial $trial: ${elapsed}ms (error: ${error}ms, ${error_pct}%)"
    done
done
```

**Expected Results**:
- BLOCK 100: 100-120ms (±20ms acceptable due to system scheduling)
- BLOCK 500: 500-550ms (±10%)
- BLOCK 1000+: ≤5% error

---

## Appendix C: Memory Layout

**Per-Stream Blocking Queue** (worst case):

```
Stream "mystream" with 100 blocked clients:
  - Key name: 8 bytes (pointer to string)
  - ArrayList metadata: 24 bytes
  - BlockedClient array: 100 * 200 bytes = 20KB
    - client_id: 8 bytes
    - keys: 8 bytes (pointer) + 8 bytes (string) = 16 bytes
    - start_ids: 8 bytes (pointer) + 16 bytes (StreamId) = 24 bytes
    - count: 9 bytes (optional)
    - timeout_ms: 8 bytes
    - start_time: 8 bytes
    - allocator: 8 bytes
    - Alignment padding: ~100 bytes per struct
  Total: ~20KB per stream with 100 clients
```

**Global Limit**: 10,000 concurrent blocked clients = ~200KB memory overhead

---

**END OF SPECIFICATION**

Generated: 2026-03-06
Author: Claude Code (Redis Specification Analyzer)
Version: 1.0
