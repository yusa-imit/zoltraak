# Redis Specification: XINFO CONSUMERS and XINFO GROUPS

## Document Purpose
This specification documents the Redis XINFO CONSUMERS and XINFO GROUPS commands for implementation verification in Zoltraak. Both commands are **already implemented** in `src/commands/streams.zig` and `src/storage/memory.zig`. This document serves as a reference for validation and testing.

---

## 1. XINFO CONSUMERS

### 1.1 Syntax
```
XINFO CONSUMERS key groupname
```

### 1.2 Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `key` | string | Yes | The stream key name |
| `groupname` | string | Yes | The consumer group name |

### 1.3 Command Properties

- **Time Complexity**: O(N) where N is the number of consumers in the group
- **Available Since**: Redis 5.0.0
- **ACL Categories**: `@read`, `@stream`, `@slow`
- **Command Flags**: `readonly`

### 1.4 Return Type

**RESP2 and RESP3**: Array reply containing consumer information

### 1.5 Response Format

Returns an array where each element represents one consumer. Each consumer is represented as a **flat array** of key-value pairs:

```
*<number-of-consumers>\r\n
  *8\r\n                    # Each consumer has 8 elements (4 fields × 2)
    $4\r\nname\r\n
    $<len>\r\n<consumer-name>\r\n
    $7\r\npending\r\n
    :<pending-count>\r\n
    $4\r\nidle\r\n
    :<idle-ms>\r\n
    $8\r\ninactive\r\n
    :<inactive-ms>\r\n
```

#### 1.5.1 Consumer Fields

| Field | Type | Description | Since |
|-------|------|-------------|-------|
| `name` | bulk string | Consumer's unique name | 5.0.0 |
| `pending` | integer | Number of entries in PEL (Pending Entry List) - messages delivered but not yet acknowledged | 5.0.0 |
| `idle` | integer | Milliseconds since last **attempted** interaction (XREADGROUP, XCLAIM, XAUTOCLAIM) | 5.0.0 (meaning changed in 7.2.0) |
| `inactive` | integer | Milliseconds since last **successful** interaction | 7.2.0 |

#### 1.5.2 idle vs inactive (Redis 7.2.0 Breaking Change)

- **Before Redis 7.2.0**: `idle` represented time since last successful interaction
- **Redis 7.2.0+**:
  - `idle`: Time since last **attempted** interaction (including failed operations)
  - `inactive`: Time since last **successful** interaction (new field)

**Zoltraak Implementation**: Tracks both `last_attempted_time` and `last_successful_time` in the `Consumer` struct.

### 1.6 Example Response

```
*2\r\n
  *8\r\n
    $4\r\nname\r\n
    $5\r\nAlice\r\n
    $7\r\npending\r\n
    :1\r\n
    $4\r\nidle\r\n
    :9104628\r\n
    $8\r\ninactive\r\n
    :18104698\r\n
  *8\r\n
    $4\r\nname\r\n
    $3\r\nBob\r\n
    $7\r\npending\r\n
    :1\r\n
    $4\r\nidle\r\n
    :83841983\r\n
    $8\r\ninactive\r\n
    :993841998\r\n
```

### 1.7 Error Conditions

| Error | Condition | Response |
|-------|-----------|----------|
| `WRONGTYPE` | Key exists but is not a stream | `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n` |
| `NOGROUP` | Consumer group does not exist | `-NOGROUP No such consumer group for this key\r\n` |
| `ERR` | Wrong number of arguments | `-ERR wrong number of arguments for 'xinfo consumers' command\r\n` |

**Note**: In Redis 7.0+, if the key doesn't exist, XINFO CONSUMERS returns `NOGROUP` error. Zoltraak implements this behavior.

### 1.8 Edge Cases

| Scenario | Expected Behavior |
|----------|-------------------|
| Group exists but has no consumers | Returns empty array: `*0\r\n` |
| Consumer has no pending messages | `pending` field is `0` |
| Consumer just created (never interacted) | `idle` and `inactive` are milliseconds since creation |
| Key does not exist | Returns `NOGROUP` error (same as non-existent group) |
| Expired key | Returns `NOGROUP` error |

### 1.9 Zoltraak Implementation Notes

- **File**: `src/storage/memory.zig:9539` - `xinfoConsumers()`
- **Command Handler**: `src/commands/streams.zig:984` - routed from `cmdXinfoStream()`
- **Consumer Timing**: Calculates `idle` and `inactive` using `std.time.milliTimestamp()`
- **Pending Count**: Counts entries in `consumer.pending.items.len`

---

## 2. XINFO GROUPS

### 2.1 Syntax
```
XINFO GROUPS key
```

### 2.2 Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `key` | string | Yes | The stream key name |

### 2.3 Command Properties

- **Time Complexity**: O(N) where N is the number of consumer groups
- **Available Since**: Redis 5.0.0
- **ACL Categories**: `@read`, `@stream`, `@slow`
- **Command Flags**: `readonly`
- **Arity**: 3

### 2.4 Return Type

**RESP2 and RESP3**: Array reply containing group information

### 2.5 Response Format

Returns an array where each element represents one consumer group. Each group is represented as a **flat array** of key-value pairs:

```
*<number-of-groups>\r\n
  *12\r\n                   # Each group has 12 elements (6 fields × 2)
    $4\r\nname\r\n
    $<len>\r\n<group-name>\r\n
    $9\r\nconsumers\r\n
    :<consumer-count>\r\n
    $7\r\npending\r\n
    :<pending-count>\r\n
    $17\r\nlast-delivered-id\r\n
    $<len>\r\n<id>\r\n
    $12\r\nentries-read\r\n
    :<entries-read>\r\n
    $3\r\nlag\r\n
    :<lag>\r\n  OR  $-1\r\n  # Integer or null bulk string
```

#### 2.5.1 Group Fields

| Field | Type | Description | Since |
|-------|------|-------------|-------|
| `name` | bulk string | Consumer group's unique name | 5.0.0 |
| `consumers` | integer | Number of consumers in the group | 5.0.0 |
| `pending` | integer | Length of group's PEL (Pending Entry List) | 5.0.0 |
| `last-delivered-id` | bulk string | ID of last entry delivered to group's consumers | 5.0.0 |
| `entries-read` | integer | Logical "read counter" of last entry delivered | 7.0.0 |
| `lag` | integer or null | Number of entries waiting to be delivered; null when unavailable | 7.0.0 |

### 2.6 Lag Calculation

**Formula**: `lag = stream.entries_added - group.entries_read`

#### 2.6.1 When Lag is NULL

The `lag` field is returned as a **null bulk string** (`$-1\r\n`) in two scenarios:

1. **Arbitrary Consumer Group Creation**: When a group is created or repositioned using `XGROUP CREATE` or `XGROUP SETID` with an arbitrary ID (not `0-0`, `>`, or `$`)

2. **Deleted Entries**: When one or more entries between the group's `last-delivered-id` and the stream's last entry have been deleted via `XDEL` or trimming operations

**Recovery**: Lag becomes available automatically during regular operation once the consumer group processes all messages up to the stream's last entry.

**Zoltraak Implementation**: Uses `group.arbitrary_start` boolean flag to determine if lag should be null.

### 2.7 Example Response

```
*2\r\n
  *12\r\n
    $4\r\nname\r\n
    $7\r\nmygroup\r\n
    $9\r\nconsumers\r\n
    :2\r\n
    $7\r\npending\r\n
    :2\r\n
    $17\r\nlast-delivered-id\r\n
    $14\r\n1638126030001-0\r\n
    $12\r\nentries-read\r\n
    :2\r\n
    $3\r\nlag\r\n
    :0\r\n
  *12\r\n
    $4\r\nname\r\n
    $16\r\nsome-other-group\r\n
    $9\r\nconsumers\r\n
    :1\r\n
    $7\r\npending\r\n
    :0\r\n
    $17\r\nlast-delivered-id\r\n
    $14\r\n1638126028070-0\r\n
    $12\r\nentries-read\r\n
    :1\r\n
    $3\r\nlag\r\n
    :1\r\n
```

### 2.8 Error Conditions

| Error | Condition | Response |
|-------|-----------|----------|
| `WRONGTYPE` | Key exists but is not a stream | `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n` |
| `ERR` | Wrong number of arguments | `-ERR wrong number of arguments for 'xinfo groups' command\r\n` |

**Note**: Unlike XINFO CONSUMERS, XINFO GROUPS does **not** return an error for non-existent keys. It returns an empty array.

### 2.9 Edge Cases

| Scenario | Expected Behavior |
|----------|-------------------|
| Key does not exist | Returns empty array: `*0\r\n` |
| Stream exists but has no groups | Returns empty array: `*0\r\n` |
| Expired key | Returns empty array: `*0\r\n` |
| Group with no consumers | `consumers` field is `0` |
| Group with no pending messages | `pending` field is `0` |
| Newly created group (no reads) | `last-delivered-id` is the ID specified during creation (or `0-0`) |
| Arbitrary start (XGROUP CREATE with ID) | `lag` is null bulk string: `$-1\r\n` |
| Lag is negative (shouldn't happen) | Implementation should prevent this; Redis would show `0` |

### 2.10 Zoltraak Implementation Notes

- **File**: `src/storage/memory.zig:9600` - `xinfoGroups()`
- **Command Handler**: `src/commands/streams.zig:1007` - routed from `cmdXinfoStream()`
- **Lag Calculation**:
  - If `group.arbitrary_start == true`: Returns null bulk string
  - Otherwise: `stream.entries_added - group.entries_read`
- **Empty Key Handling**: Returns `*0\r\n` instead of error (differs from XINFO CONSUMERS)

---

## 3. Integration with Existing Implementation

### 3.1 Command Routing

Both commands are routed through `cmdXinfoStream()` in `src/commands/streams.zig:970`:

```zig
pub fn cmdXinfoStream(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    // Parse subcommand (args[1])
    if (std.mem.eql(u8, subcommand, "CONSUMERS")) {
        // Route to storage.xinfoConsumers()
    } else if (std.mem.eql(u8, subcommand, "GROUPS")) {
        // Route to storage.xinfoGroups()
    } else if (std.mem.eql(u8, subcommand, "STREAM")) {
        // Existing XINFO STREAM implementation
    }
}
```

### 3.2 Storage Layer Methods

| Method | Signature | Returns |
|--------|-----------|---------|
| `xinfoConsumers` | `(self: *Storage, allocator: Allocator, key: []const u8, group_name: []const u8) ![]const u8` | RESP-encoded consumer array or error |
| `xinfoGroups` | `(self: *Storage, allocator: Allocator, key: []const u8) ![]const u8` | RESP-encoded group array or error |

Both methods return **pre-formatted RESP strings**, which the command handler returns directly to the client.

### 3.3 Data Structures

From `src/storage/memory.zig`:

```zig
const Consumer = struct {
    name: []const u8,
    pending: std.ArrayList(PendingEntry),
    last_attempted_time: i64,    // For 'idle' field
    last_successful_time: i64,   // For 'inactive' field
};

const ConsumerGroup = struct {
    name: []const u8,
    last_delivered_id: StreamId,
    consumers: std.StringHashMap(*Consumer),
    pending: std.ArrayList(PendingEntry),
    entries_read: u64,
    arbitrary_start: bool,       // For lag null determination
};
```

---

## 4. Testing Requirements

### 4.1 XINFO CONSUMERS Tests

- **Basic functionality**: List consumers with all fields
- **Empty group**: Group exists but has no consumers
- **Non-existent group**: NOGROUP error
- **Non-existent key**: NOGROUP error
- **Wrong type**: WRONGTYPE error on non-stream key
- **Expired key**: NOGROUP error
- **Timing fields**: Verify `idle` and `inactive` are reasonable timestamps
- **Pending count**: Verify matches actual PEL size

### 4.2 XINFO GROUPS Tests

- **Basic functionality**: List groups with all fields
- **Empty stream**: Stream exists but has no groups
- **Non-existent key**: Empty array (not error)
- **Wrong type**: WRONGTYPE error on non-stream key
- **Expired key**: Empty array
- **Lag calculation**: Verify `stream.entries_added - group.entries_read`
- **Lag null**: Verify null for `arbitrary_start == true`
- **Multiple groups**: Verify all groups returned

### 4.3 Integration Tests

- **XINFO CONSUMERS + XREADGROUP**: Verify `idle` and `inactive` update correctly
- **XINFO GROUPS + XGROUP CREATE**: Verify lag is null for arbitrary IDs
- **XINFO GROUPS + XADD**: Verify lag increases when new entries added
- **Cross-command validation**: XINFO STREAM vs XINFO GROUPS field consistency

---

## 5. Redis Compatibility Checklist

| Feature | Redis Spec | Zoltraak Status |
|---------|-----------|-----------------|
| **XINFO CONSUMERS syntax** | `key groupname` | ✅ Implemented |
| **XINFO GROUPS syntax** | `key` | ✅ Implemented |
| **Consumer fields (4)** | name, pending, idle, inactive | ✅ Implemented |
| **Group fields (6)** | name, consumers, pending, last-delivered-id, entries-read, lag | ✅ Implemented |
| **idle = last attempted** | Redis 7.2.0+ behavior | ✅ Implemented |
| **inactive = last successful** | Redis 7.2.0+ field | ✅ Implemented |
| **Lag null for arbitrary start** | Group created with arbitrary ID | ✅ Implemented (`arbitrary_start` flag) |
| **NOGROUP error** | XINFO CONSUMERS on missing group | ✅ Implemented |
| **Empty array on missing key** | XINFO GROUPS behavior | ✅ Implemented |
| **WRONGTYPE error** | Both commands on non-stream | ✅ Implemented |
| **RESP2 flat arrays** | Key-value pairs in arrays | ✅ Implemented |
| **Timing precision** | Milliseconds for idle/inactive | ✅ Implemented |

---

## 6. Reference Links

- [XINFO CONSUMERS Official Docs](https://redis.io/commands/xinfo-consumers/)
- [XINFO GROUPS Official Docs](https://redis.io/commands/xinfo-groups/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis Streams Introduction](https://redis.io/docs/data-types/streams/)
- [Consumer Groups Tutorial](https://redis.io/docs/data-types/streams-tutorial/#consumer-groups)

---

## 7. Implementation Status

**Status**: ✅ **FULLY IMPLEMENTED**

Both commands were implemented as part of the Redis Streams subsystem. The implementation:

1. **Command routing**: `src/commands/streams.zig:984-1026`
2. **Storage methods**: `src/storage/memory.zig:9539-9670`
3. **Tests**: `src/storage/memory.zig:11488-11587` (4 unit tests)
4. **Integration**: Integrated with XINFO STREAM router

**Next Steps**:
- Validate with integration tests against real Redis
- Add RESP3 native map support (currently uses RESP2 flat arrays in both protocols)
- Verify timing field accuracy under concurrent load

---

## 8. Known Deviations from Redis

1. **RESP3 Maps**: Zoltraak currently returns RESP2-style flat arrays in both RESP2 and RESP3. Redis 7.0+ uses native RESP3 maps (`%`) in RESP3 mode. This is a cosmetic difference; functionally equivalent.

2. **Timing Precision**: Redis uses internal monotonic clocks; Zoltraak uses `std.time.milliTimestamp()`. This may cause minor discrepancies in `idle`/`inactive` values during system clock adjustments.

3. **Arbitrary Start Detection**: Zoltraak uses a boolean `arbitrary_start` flag. Redis infers this from comparing the group's starting ID against the stream's min/max entries. Both approaches are correct but implementation differs.

---

**Document Version**: 1.0
**Last Updated**: 2026-05-08
**Zoltraak Version**: v0.1.0+
**Target Redis Version**: 7.2.0
