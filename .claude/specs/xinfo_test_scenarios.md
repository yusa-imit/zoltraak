# XINFO CONSUMERS & XINFO GROUPS - Test Scenarios

## Purpose
This document provides comprehensive test scenarios for validating XINFO CONSUMERS and XINFO GROUPS implementation. Use these scenarios for integration testing and Redis compatibility validation.

---

## Test Category 1: XINFO CONSUMERS Basic Functionality

### Scenario 1.1: List Consumers with All Fields

**Setup**:
```redis
XADD mystream * field1 value1
XGROUP CREATE mystream mygroup 0
XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >
XREADGROUP GROUP mygroup consumer2 COUNT 1 STREAMS mystream >
```

**Command**:
```redis
XINFO CONSUMERS mystream mygroup
```

**Expected Output** (conceptual):
```
1) 1) "name"
   2) "consumer1"
   3) "pending"
   4) (integer) 1
   5) "idle"
   6) (integer) <milliseconds>
   7) "inactive"
   8) (integer) <milliseconds>
2) 1) "name"
   2) "consumer2"
   3) "pending"
   4) (integer) 0
   5) "idle"
   6) (integer) <milliseconds>
   7) "inactive"
   8) (integer) <milliseconds>
```

**Validation**:
- ✅ Returns array of 2 consumers
- ✅ Each consumer has exactly 4 fields
- ✅ `pending` count matches actual pending entries
- ✅ `idle` and `inactive` are positive integers

---

### Scenario 1.2: Empty Consumer Group

**Setup**:
```redis
XADD mystream * field1 value1
XGROUP CREATE mystream emptygroup 0
```

**Command**:
```redis
XINFO CONSUMERS mystream emptygroup
```

**Expected Output**:
```
(empty array)
```

**RESP Wire Format**:
```
*0\r\n
```

**Validation**:
- ✅ Returns empty array (not error)
- ✅ RESP encoding is `*0\r\n`

---

### Scenario 1.3: Non-existent Consumer Group

**Setup**:
```redis
XADD mystream * field1 value1
```

**Command**:
```redis
XINFO CONSUMERS mystream nosuchgroup
```

**Expected Output**:
```
(error) NOGROUP No such consumer group for this key
```

**RESP Wire Format**:
```
-NOGROUP No such consumer group for this key\r\n
```

**Validation**:
- ✅ Returns NOGROUP error
- ✅ Error message matches Redis exactly

---

### Scenario 1.4: Non-existent Stream Key

**Setup**:
```redis
# No setup - key doesn't exist
```

**Command**:
```redis
XINFO CONSUMERS nosuchstream mygroup
```

**Expected Output**:
```
(error) NOGROUP No such consumer group for this key
```

**Validation**:
- ✅ Returns NOGROUP error (not WRONGTYPE)
- ✅ Behaves identically to missing group

---

### Scenario 1.5: Wrong Key Type

**Setup**:
```redis
SET stringkey "value"
```

**Command**:
```redis
XINFO CONSUMERS stringkey mygroup
```

**Expected Output**:
```
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

**RESP Wire Format**:
```
-WRONGTYPE Operation against a key holding the wrong kind of value\r\n
```

**Validation**:
- ✅ Returns WRONGTYPE error
- ✅ Error message matches Redis exactly

---

### Scenario 1.6: Consumer Timing Fields

**Setup**:
```redis
XADD mystream * field1 value1
XADD mystream * field2 value2
XGROUP CREATE mystream mygroup 0
```

**Commands** (with delays):
```redis
# T0: Create consumer, read 1 message
XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
# Wait 100ms
# T1: Attempt read (no new messages)
XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
# Wait 200ms
# T2: Check timing
XINFO CONSUMERS mystream mygroup
```

**Expected Behavior**:
- At T0: `idle` ≈ 0, `inactive` ≈ 0 (just created)
- At T1: `idle` ≈ 100ms (since last attempt), `inactive` ≈ 100ms (since last success)
- At T2: `idle` ≈ 200ms (since T1 attempt), `inactive` ≈ 300ms (since T0 success)

**Validation**:
- ✅ `idle` updates on every XREADGROUP call
- ✅ `inactive` updates only when messages delivered
- ✅ Both values increase monotonically over time

---

## Test Category 2: XINFO GROUPS Basic Functionality

### Scenario 2.1: List Groups with All Fields

**Setup**:
```redis
XADD mystream * field1 value1
XADD mystream * field2 value2
XADD mystream * field3 value3
XGROUP CREATE mystream group1 0
XGROUP CREATE mystream group2 $
XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream >
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Expected Output** (conceptual):
```
1)  1) "name"
    2) "group1"
    3) "consumers"
    4) (integer) 1
    5) "pending"
    6) (integer) 1
    7) "last-delivered-id"
    8) "<stream-id>"
    9) "entries-read"
   10) (integer) 1
   11) "lag"
   12) (integer) 2

2)  1) "name"
    2) "group2"
    3) "consumers"
    4) (integer) 0
    5) "pending"
    6) (integer) 0
    7) "last-delivered-id"
    8) "<last-stream-id>"
    9) "entries-read"
   10) (integer) 0
   11) "lag"
   12) (integer) 0
```

**Validation**:
- ✅ Returns array of 2 groups
- ✅ Each group has exactly 6 fields
- ✅ `group1` has 1 consumer, 1 pending, lag = 2 (3 total - 1 read)
- ✅ `group2` created at end (`$`), lag = 0

---

### Scenario 2.2: Lag Calculation

**Setup**:
```redis
XADD mystream * a 1
XADD mystream * b 2
XADD mystream * c 3
XADD mystream * d 4
XADD mystream * e 5
XGROUP CREATE mystream mygroup 0
XREADGROUP GROUP mygroup consumer COUNT 2 STREAMS mystream >
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Expected Output** (relevant fields):
```
...
"entries-read"
(integer) 2
"lag"
(integer) 3   # 5 total entries - 2 read = 3 unread
```

**Validation**:
- ✅ `lag = stream.entries_added - group.entries_read`
- ✅ `lag = 5 - 2 = 3`

**Follow-up**:
```redis
XADD mystream * f 6
XINFO GROUPS mystream
```

**Expected**:
```
"lag"
(integer) 4   # 6 total - 2 read = 4 unread
```

**Validation**:
- ✅ Lag increases when new entries added

---

### Scenario 2.3: Lag NULL for Arbitrary Start

**Setup**:
```redis
XADD mystream 1000-0 field1 value1
XADD mystream 2000-0 field2 value2
XADD mystream 3000-0 field3 value3
XGROUP CREATE mystream mygroup 1500-0
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Expected Output**:
```
...
"lag"
(nil)
```

**RESP Wire Format**:
```
...
$3\r\nlag\r\n
$-1\r\n
```

**Validation**:
- ✅ Lag is null bulk string (`$-1\r\n`)
- ✅ Arbitrary ID (not `0-0`, `>`, or `$`) triggers null lag

---

### Scenario 2.4: Lag NULL After XDEL (Advanced)

**Setup**:
```redis
XADD mystream 1000-0 a 1
XADD mystream 2000-0 b 2
XADD mystream 3000-0 c 3
XGROUP CREATE mystream mygroup 0
XREADGROUP GROUP mygroup consumer COUNT 1 STREAMS mystream >
XDEL mystream 2000-0
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Expected Output**:
```
...
"last-delivered-id"
"1000-0"
"lag"
(nil)   # Deleted entry between last-delivered and stream end
```

**Validation**:
- ✅ Lag becomes null after entry deleted in gap
- ✅ Lag recovers after reading past the gap

**Note**: This test requires XDEL implementation and proper arbitrary_start flag management.

---

### Scenario 2.5: Empty Stream

**Setup**:
```redis
XADD mystream * field1 value1
# No groups created
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Expected Output**:
```
(empty array)
```

**RESP Wire Format**:
```
*0\r\n
```

**Validation**:
- ✅ Returns empty array
- ✅ No error (differs from XINFO CONSUMERS)

---

### Scenario 2.6: Non-existent Stream

**Setup**:
```redis
# No stream created
```

**Command**:
```redis
XINFO GROUPS nosuchstream
```

**Expected Output**:
```
(empty array)
```

**RESP Wire Format**:
```
*0\r\n
```

**Validation**:
- ✅ Returns empty array (not NOGROUP error)
- ✅ Differs from XINFO CONSUMERS behavior

---

### Scenario 2.7: Wrong Key Type

**Setup**:
```redis
SET stringkey "value"
```

**Command**:
```redis
XINFO GROUPS stringkey
```

**Expected Output**:
```
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

**Validation**:
- ✅ Returns WRONGTYPE error
- ✅ Same as XINFO CONSUMERS behavior

---

## Test Category 3: Cross-Command Integration

### Scenario 3.1: XINFO CONSUMERS + XACK

**Setup**:
```redis
XADD mystream * a 1
XGROUP CREATE mystream mygroup 0
XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
```

**Before XACK**:
```redis
XINFO CONSUMERS mystream mygroup
```

**Expected**:
```
...
"pending"
(integer) 1
```

**Execute**:
```redis
XACK mystream mygroup <entry-id>
```

**After XACK**:
```redis
XINFO CONSUMERS mystream mygroup
```

**Expected**:
```
...
"pending"
(integer) 0
```

**Validation**:
- ✅ Pending count decreases after XACK
- ✅ Consumer still exists with 0 pending

---

### Scenario 3.2: XINFO GROUPS + XREADGROUP

**Setup**:
```redis
XADD mystream * a 1
XADD mystream * b 2
XGROUP CREATE mystream mygroup 0
```

**Before Read**:
```redis
XINFO GROUPS mystream
```

**Expected**:
```
...
"consumers"
(integer) 0
"pending"
(integer) 0
"entries-read"
(integer) 0
"lag"
(integer) 2
```

**Execute**:
```redis
XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
```

**After Read**:
```redis
XINFO GROUPS mystream
```

**Expected**:
```
...
"consumers"
(integer) 1
"pending"
(integer) 1
"entries-read"
(integer) 1
"lag"
(integer) 1
```

**Validation**:
- ✅ Consumer count increases
- ✅ Pending increases
- ✅ Entries-read increases
- ✅ Lag decreases

---

### Scenario 3.3: XINFO STREAM vs XINFO GROUPS Consistency

**Setup**:
```redis
XADD mystream * a 1
XADD mystream * b 2
XGROUP CREATE mystream mygroup 0
XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
```

**Commands**:
```redis
XINFO STREAM mystream
XINFO GROUPS mystream
```

**Validation**:
- ✅ `XINFO STREAM length` matches total entries
- ✅ `XINFO GROUPS lag` + `entries-read` = stream length
- ✅ `XINFO STREAM entries-added` = stream length

---

## Test Category 4: RESP Protocol Validation

### Scenario 4.1: RESP2 Wire Format

**Setup**:
```redis
XADD s * a 1
XGROUP CREATE s g 0
XREADGROUP GROUP g c COUNT 1 STREAMS s >
```

**Command**:
```redis
XINFO CONSUMERS s g
```

**Expected Wire Format** (hexdump):
```
*1\r\n              # Array of 1 consumer
*8\r\n              # Consumer has 8 elements
$4\r\nname\r\n
$1\r\nc\r\n
$7\r\npending\r\n
:1\r\n
$4\r\nidle\r\n
:<ms>\r\n
$8\r\ninactive\r\n
:<ms>\r\n
```

**Validation**:
- ✅ Byte-by-byte match with Redis RESP2
- ✅ No extra whitespace or formatting

---

### Scenario 4.2: RESP3 Mode (Future Enhancement)

**Setup**:
```redis
HELLO 3
XADD s * a 1
XGROUP CREATE s g 0
XREADGROUP GROUP g c COUNT 1 STREAMS s >
```

**Command**:
```redis
XINFO CONSUMERS s g
```

**Expected Wire Format** (RESP3 with maps):
```
*1\r\n              # Array of 1 consumer
%4\r\n              # Map with 4 entries
$4\r\nname\r\n
$1\r\nc\r\n
$7\r\npending\r\n
:1\r\n
$4\r\nidle\r\n
:<ms>\r\n
$8\r\ninactive\r\n
:<ms>\r\n
```

**Current Zoltraak**: Returns RESP2 flat array even in RESP3 mode.

**Validation**:
- ⚠️ Currently returns flat array (not map)
- ⚠️ Functionally equivalent, cosmetically different

---

## Test Category 5: Edge Cases and Error Conditions

### Scenario 5.1: Consumer with Zero Pending

**Setup**:
```redis
XADD mystream * a 1
XGROUP CREATE mygroup mygroup 0
XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
XACK mystream mygroup <entry-id>
```

**Command**:
```redis
XINFO CONSUMERS mystream mygroup
```

**Expected**:
```
...
"name"
"alice"
"pending"
(integer) 0
```

**Validation**:
- ✅ Consumer still listed with 0 pending
- ✅ Timing fields still present

---

### Scenario 5.2: Group with Zero Consumers

**Setup**:
```redis
XADD mystream * a 1
XGROUP CREATE mystream mygroup 0
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Expected**:
```
...
"consumers"
(integer) 0
```

**Validation**:
- ✅ Group listed with 0 consumers
- ✅ All other fields still present

---

### Scenario 5.3: Expired Stream Key

**Setup**:
```redis
XADD mystream * a 1
XGROUP CREATE mystream mygroup 0
EXPIRE mystream 1
# Wait 2 seconds
```

**Commands**:
```redis
XINFO CONSUMERS mystream mygroup
XINFO GROUPS mystream
```

**Expected**:
```
# XINFO CONSUMERS
(error) NOGROUP No such consumer group for this key

# XINFO GROUPS
(empty array)
```

**Validation**:
- ✅ XINFO CONSUMERS returns NOGROUP after expiry
- ✅ XINFO GROUPS returns empty array after expiry

---

### Scenario 5.4: Wrong Number of Arguments

**Commands**:
```redis
XINFO CONSUMERS
XINFO CONSUMERS mystream
XINFO CONSUMERS mystream mygroup extra
XINFO GROUPS
XINFO GROUPS mystream extra
```

**Expected** (all):
```
(error) ERR wrong number of arguments for 'xinfo <subcommand>' command
```

**Validation**:
- ✅ Argument count validation
- ✅ Helpful error messages

---

## Test Category 6: Performance and Stress Testing

### Scenario 6.1: Many Consumers

**Setup**:
```bash
for i in {1..100}; do
  redis-cli XREADGROUP GROUP mygroup "consumer$i" COUNT 1 STREAMS mystream >
done
```

**Command**:
```redis
XINFO CONSUMERS mystream mygroup
```

**Validation**:
- ✅ Returns all 100 consumers
- ✅ Response time < 100ms
- ✅ No memory leaks

---

### Scenario 6.2: Many Groups

**Setup**:
```bash
for i in {1..100}; do
  redis-cli XGROUP CREATE mystream "group$i" 0 MKSTREAM
done
```

**Command**:
```redis
XINFO GROUPS mystream
```

**Validation**:
- ✅ Returns all 100 groups
- ✅ Response time < 100ms
- ✅ Lag calculations correct for all groups

---

### Scenario 6.3: Large Pending Lists

**Setup**:
```bash
# Add 1000 entries
for i in {1..1000}; do
  redis-cli XADD mystream '*' field$i value$i
done
# Read all without ACK
redis-cli XREADGROUP GROUP mygroup alice COUNT 1000 STREAMS mystream >
```

**Command**:
```redis
XINFO CONSUMERS mystream mygroup
```

**Expected**:
```
...
"pending"
(integer) 1000
```

**Validation**:
- ✅ Correct pending count
- ✅ Response time < 50ms
- ✅ No memory leaks during iteration

---

## Test Execution Checklist

### Unit Tests (Existing)
- ✅ xinfoConsumers returns consumer list with timing fields
- ✅ xinfoConsumers returns NOGROUP for missing group
- ✅ xinfoGroups returns group list with lag
- ✅ xinfoGroups lag is null for arbitrary start
- ✅ xinfoGroups returns empty array for missing key

### Integration Tests (To Add)
- ⏳ Category 1: XINFO CONSUMERS (6 scenarios)
- ⏳ Category 2: XINFO GROUPS (7 scenarios)
- ⏳ Category 3: Cross-command (3 scenarios)
- ⏳ Category 4: RESP protocol (2 scenarios)
- ⏳ Category 5: Edge cases (4 scenarios)
- ⏳ Category 6: Performance (3 scenarios)

### Redis Compatibility Tests
- ⏳ Byte-by-byte RESP comparison with Redis 7.2.0
- ⏳ Timing field accuracy validation
- ⏳ Lag calculation differential testing
- ⏳ Error message exact match validation

---

## Automated Test Script Template

```bash
#!/bin/bash
# XINFO CONSUMERS & GROUPS Integration Test Suite

set -e

# Start Zoltraak server
./zig-out/bin/zoltraak &
ZOLTRAAK_PID=$!
trap "kill $ZOLTRAAK_PID" EXIT

sleep 1

# Test 1.1: List consumers with all fields
redis-cli FLUSHALL
redis-cli XADD mystream '*' field1 value1
redis-cli XGROUP CREATE mystream mygroup 0
redis-cli XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream '>'
OUTPUT=$(redis-cli XINFO CONSUMERS mystream mygroup)
echo "$OUTPUT" | grep -q "name" || { echo "FAIL 1.1"; exit 1; }
echo "$OUTPUT" | grep -q "pending" || { echo "FAIL 1.1"; exit 1; }
echo "$OUTPUT" | grep -q "idle" || { echo "FAIL 1.1"; exit 1; }
echo "$OUTPUT" | grep -q "inactive" || { echo "FAIL 1.1"; exit 1; }
echo "PASS 1.1"

# Test 2.1: List groups with all fields
redis-cli FLUSHALL
redis-cli XADD mystream '*' a 1
redis-cli XGROUP CREATE mystream group1 0
OUTPUT=$(redis-cli XINFO GROUPS mystream)
echo "$OUTPUT" | grep -q "name" || { echo "FAIL 2.1"; exit 1; }
echo "$OUTPUT" | grep -q "consumers" || { echo "FAIL 2.1"; exit 1; }
echo "$OUTPUT" | grep -q "lag" || { echo "FAIL 2.1"; exit 1; }
echo "PASS 2.1"

# Add more tests...

echo "All tests passed!"
```

---

**Document Version**: 1.0
**Last Updated**: 2026-05-08
**Test Count**: 25 scenarios across 6 categories
