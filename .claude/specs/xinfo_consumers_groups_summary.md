# XINFO CONSUMERS and XINFO GROUPS - Implementation Summary

## Status: ✅ FULLY IMPLEMENTED

Both commands are already implemented in Zoltraak as part of Iteration 167 (Streams Advanced).

---

## Quick Reference

### XINFO CONSUMERS

**Syntax**: `XINFO CONSUMERS key groupname`

**Returns**: Array of consumers with fields:
- `name` (string): Consumer name
- `pending` (int): Number of pending messages
- `idle` (int): Milliseconds since last attempted interaction
- `inactive` (int): Milliseconds since last successful interaction

**Errors**:
- `NOGROUP`: Group or key doesn't exist
- `WRONGTYPE`: Key is not a stream

**Response Format** (RESP2):
```
*<num-consumers>\r\n
  *8\r\n
    $4\r\nname\r\n$<len>\r\n<name>\r\n
    $7\r\npending\r\n:<count>\r\n
    $4\r\nidle\r\n:<ms>\r\n
    $8\r\ninactive\r\n:<ms>\r\n
```

---

### XINFO GROUPS

**Syntax**: `XINFO GROUPS key`

**Returns**: Array of consumer groups with fields:
- `name` (string): Group name
- `consumers` (int): Number of consumers in group
- `pending` (int): Number of pending messages
- `last-delivered-id` (string): Last delivered stream ID
- `entries-read` (int): Number of entries read by group
- `lag` (int or null): Entries waiting to be delivered

**Errors**:
- `WRONGTYPE`: Key is not a stream
- No error for missing key (returns empty array)

**Response Format** (RESP2):
```
*<num-groups>\r\n
  *12\r\n
    $4\r\nname\r\n$<len>\r\n<name>\r\n
    $9\r\nconsumers\r\n:<count>\r\n
    $7\r\npending\r\n:<count>\r\n
    $17\r\nlast-delivered-id\r\n$<len>\r\n<id>\r\n
    $12\r\nentries-read\r\n:<count>\r\n
    $3\r\nlag\r\n:<lag>\r\n OR $-1\r\n
```

---

## Key Differences Between Commands

| Aspect | XINFO CONSUMERS | XINFO GROUPS |
|--------|-----------------|--------------|
| **Missing key** | `NOGROUP` error | Empty array `*0\r\n` |
| **Missing group** | `NOGROUP` error | N/A |
| **Parameters** | key + groupname | key only |
| **Fields per item** | 4 (8 RESP elements) | 6 (12 RESP elements) |
| **Complexity** | O(N) consumers | O(N) groups |

---

## Critical Implementation Details

### Timing Fields (Redis 7.2.0+ Behavior)

- **idle**: Time since last **attempted** interaction (includes failed XREADGROUP)
- **inactive**: Time since last **successful** interaction (message delivery)

**Zoltraak Implementation**:
```zig
const Consumer = struct {
    last_attempted_time: i64,   // Updated on every XREADGROUP/XCLAIM/XAUTOCLAIM
    last_successful_time: i64,  // Updated only when messages delivered
};
```

### Lag Calculation

**Formula**: `lag = stream.entries_added - group.entries_read`

**NULL Cases**:
1. Group created with arbitrary ID (not `0-0`, `>`, or `$`)
2. Entries deleted between group position and stream end

**Zoltraak Implementation**:
```zig
if (group.arbitrary_start) {
    // Return null bulk string: $-1\r\n
} else {
    const lag = stream.entries_added - group.entries_read;
    // Return integer: :<lag>\r\n
}
```

---

## Implementation Files

| Component | File | Lines |
|-----------|------|-------|
| **Command Router** | `src/commands/streams.zig` | 984-1026 |
| **CONSUMERS Storage** | `src/storage/memory.zig` | 9539-9596 |
| **GROUPS Storage** | `src/storage/memory.zig` | 9600-9670 |
| **Unit Tests** | `src/storage/memory.zig` | 11488-11587 |

---

## Testing Checklist

### XINFO CONSUMERS
- ✅ List consumers with all 4 fields
- ✅ Empty group returns `*0\r\n`
- ✅ Missing group returns `NOGROUP`
- ✅ Missing key returns `NOGROUP`
- ✅ Non-stream key returns `WRONGTYPE`
- ⏳ Timing fields accuracy (integration test needed)

### XINFO GROUPS
- ✅ List groups with all 6 fields
- ✅ Empty stream returns `*0\r\n`
- ✅ Missing key returns `*0\r\n` (not error)
- ✅ Non-stream key returns `WRONGTYPE`
- ✅ Lag calculation for normal groups
- ✅ Lag null for arbitrary start groups
- ⏳ Lag null after XDEL (integration test needed)

---

## Edge Cases

### XINFO CONSUMERS

| Scenario | Expected | Implemented |
|----------|----------|-------------|
| Consumer never read | `idle`/`inactive` = time since creation | ✅ Yes |
| Consumer with 0 pending | `pending: 0` | ✅ Yes |
| Expired key | `NOGROUP` error | ✅ Yes |

### XINFO GROUPS

| Scenario | Expected | Implemented |
|----------|----------|-------------|
| Group with 0 consumers | `consumers: 0` | ✅ Yes |
| Newly created group | `entries-read: 0`, `pending: 0` | ✅ Yes |
| Lag is negative | Should not happen (bug if it does) | ⚠️ Not validated |

---

## Known Deviations from Redis

1. **RESP3 Format**: Zoltraak returns RESP2 flat arrays in both RESP2 and RESP3 modes. Redis 7.0+ uses RESP3 maps (`%`) in RESP3 mode. Functionally equivalent, cosmetically different.

2. **Timing Precision**: Zoltraak uses `std.time.milliTimestamp()` vs Redis's monotonic clock. Minor discrepancies possible during system clock adjustments.

3. **Arbitrary Start Detection**: Zoltraak uses explicit `arbitrary_start` flag. Redis infers from ID comparison. Both correct, different approach.

---

## Next Actions

Since these commands are **already implemented**, the focus should be on:

1. **Integration Testing**: Add tests in `tests/test_integration.zig` to verify:
   - Timing field updates during XREADGROUP
   - Lag calculation after XADD
   - Lag null behavior after XDEL
   - RESP3 mode responses (currently returns RESP2 format)

2. **Redis Compatibility Validation**: Use `redis-compatibility-validator` to compare byte-for-byte with real Redis 7.2.0

3. **RESP3 Native Maps** (Future): Consider implementing native RESP3 map format (`%` type) for these commands when in RESP3 mode

---

## Example Usage

### Test XINFO CONSUMERS
```bash
redis-cli XGROUP CREATE mystream mygroup 0 MKSTREAM
redis-cli XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >
redis-cli XINFO CONSUMERS mystream mygroup
```

**Expected Output**:
```
1) 1) "name"
   2) "consumer1"
   3) "pending"
   4) (integer) 1
   5) "idle"
   6) (integer) 123
   7) "inactive"
   8) (integer) 123
```

### Test XINFO GROUPS
```bash
redis-cli XADD mystream * field value
redis-cli XGROUP CREATE mystream group1 0
redis-cli XGROUP CREATE mystream group2 1234567890-0
redis-cli XINFO GROUPS mystream
```

**Expected Output**:
```
1)  1) "name"
    2) "group1"
    3) "consumers"
    4) (integer) 0
    5) "pending"
    6) (integer) 0
    7) "last-delivered-id"
    8) "0-0"
    9) "entries-read"
   10) (integer) 0
   11) "lag"
   12) (integer) 1

2)  1) "name"
    2) "group2"
    3) "consumers"
    4) (integer) 0
    5) "pending"
    6) (integer) 0
    7) "last-delivered-id"
    8) "1234567890-0"
    9) "entries-read"
   10) (integer) 0
   11) "lag"
   12) (nil)  # Null because arbitrary start
```

---

## References

- [Redis XINFO CONSUMERS Docs](https://redis.io/commands/xinfo-consumers/)
- [Redis XINFO GROUPS Docs](https://redis.io/commands/xinfo-groups/)
- [Zoltraak Streams Implementation](https://github.com/yourusername/zoltraak/blob/main/src/commands/streams.zig)

---

**Document Version**: 1.0
**Date**: 2026-05-08
**Status**: Reference for validation, not implementation
