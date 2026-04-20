# BF.INSERT — Test Examples & Expected Outputs

This document provides concrete test cases with expected RESP protocol responses.

---

## Test Category 1: Basic Creation and Insertion

### Test 1.1: Auto-Create with Defaults
```redis
Command: BF.INSERT myfilter ITEMS item1 item2 item3
Expected: *3\r\n:1\r\n:1\r\n:1\r\n
Meaning: Array of 3 elements, all 1 (new items)

Response decoded:
[1, 1, 1]
```

### Test 1.2: Auto-Create with Custom CAPACITY
```redis
Command: BF.INSERT customfilter CAPACITY 50000 ITEMS item1 item2
Expected: *2\r\n:1\r\n:1\r\n
Meaning: Array of 2 elements, both 1

Response decoded:
[1, 1]

# Filter created with:
# - capacity: 50000 (not 100)
# - error_rate: 0.01 (default)
# - expansion: 2 (default)
# - scaling: enabled (default)
```

### Test 1.3: Auto-Create with Custom ERROR
```redis
Command: BF.INSERT precisefilter ERROR 0.001 ITEMS item1 item2
Expected: *2\r\n:1\r\n:1\r\n

# Filter created with:
# - capacity: 100 (default)
# - error_rate: 0.001 (not 0.01)
# - expansion: 2 (default)
# - scaling: enabled (default)
```

### Test 1.4: Auto-Create with All Custom Parameters
```redis
Command: BF.INSERT fullcontrol CAPACITY 20000 ERROR 0.0001 EXPANSION 4 ITEMS a b c
Expected: *3\r\n:1\r\n:1\r\n:1\r\n

# Filter created with:
# - capacity: 20000
# - error_rate: 0.0001
# - expansion: 4
# - scaling: enabled (default)
```

### Test 1.5: Auto-Create with NONSCALING
```redis
Command: BF.INSERT staticfilter CAPACITY 100 NONSCALING ITEMS item1
Expected: *1\r\n:1\r\n

# Filter created with:
# - capacity: 100
# - error_rate: 0.01 (default)
# - nonscaling: true (no sub-filter expansion)
```

---

## Test Category 2: Insert into Existing Filter

### Test 2.1: Insert into Existing Filter (Creation Params Ignored)
```redis
# First create a filter
Command: BF.INSERT existing ERROR 0.001 ITEMS original_item
Expected: *1\r\n:1\r\n

# Now insert with DIFFERENT parameters (they should be ignored)
Command: BF.INSERT existing CAPACITY 999 ERROR 0.1 EXPANSION 10 ITEMS new_item
Expected: *1\r\n:0\r\n
Meaning: new_item was treated as duplicate

# The filter still has capacity=1000 (from creation), not 999
# And error=0.001 (from creation), not 0.1
```

### Test 2.2: Duplicate Items in Same Batch
```redis
Command: BF.INSERT myfilter ITEMS item1 item1 item2
Expected: *3\r\n:1\r\n:0\r\n:1\r\n
Decoded: [1, 0, 1]
Meaning: First item1 is new (1), second item1 is duplicate (0), item2 is new (1)
```

### Test 2.3: Previously Inserted Items
```redis
# First insert
Command: BF.INSERT myfilter ITEMS alice bob
Expected: *2\r\n:1\r\n:1\r\n

# Second insert with mix of old and new
Command: BF.INSERT myfilter ITEMS alice charlie bob dave
Expected: *4\r\n:0\r\n:1\r\n:0\r\n:1\r\n
Decoded: [0, 1, 0, 1]
Meaning: alice (duplicate), charlie (new), bob (duplicate), dave (new)
```

---

## Test Category 3: NOCREATE Flag

### Test 3.1: NOCREATE with Existing Filter
```redis
# Create filter first
Command: BF.INSERT myfilter ITEMS seed
Expected: *1\r\n:1\r\n

# Now use NOCREATE on existing filter (works)
Command: BF.INSERT myfilter NOCREATE ITEMS item1 item2
Expected: *2\r\n:1\r\n:1\r\n
Decoded: [1, 1]
```

### Test 3.2: NOCREATE with Nonexistent Filter
```redis
Command: BF.INSERT newfilter NOCREATE ITEMS item1
Expected: -ERR no such key\r\n
Meaning: Error response; filter not created
```

### Test 3.3: NOCREATE with CAPACITY (Mutual Exclusion)
```redis
Command: BF.INSERT anyfilter NOCREATE CAPACITY 1000 ITEMS item1
Expected: -ERR NOCREATE cannot be used with CAPACITY or ERROR\r\n
```

### Test 3.4: NOCREATE with ERROR (Mutual Exclusion)
```redis
Command: BF.INSERT anyfilter NOCREATE ERROR 0.001 ITEMS item1
Expected: -ERR NOCREATE cannot be used with CAPACITY or ERROR\r\n
```

---

## Test Category 4: NONSCALING and Overflow

### Test 4.1: NONSCALING Filter with Capacity Exceeded
```redis
# Create small non-scaling filter
Command: BF.INSERT smallfilter CAPACITY 3 NONSCALING ITEMS a b c d e
Expected: *5\r\n:1\r\n:1\r\n:1\r\n-ERR filter is full\r\n-ERR filter is full\r\n
Decoded: [1, 1, 1, "ERR filter is full", "ERR filter is full"]
Meaning: a, b, c inserted; d, e rejected (partial success, not rolled back)
```

### Test 4.2: NONSCALING Filter Without Overflow
```redis
Command: BF.INSERT smallfilter CAPACITY 3 NONSCALING ITEMS a b c
Expected: *3\r\n:1\r\n:1\r\n:1\r\n
Decoded: [1, 1, 1]
Meaning: Exactly at capacity, all succeed
```

### Test 4.3: Scaling Filter (No Overflow)
```redis
# Default is scaling (not NONSCALING)
Command: BF.INSERT scalingfilter CAPACITY 3 ITEMS a b c d e f
Expected: *6\r\n:1\r\n:1\r\n:1\r\n:1\r\n:1\r\n:1\r\n
Decoded: [1, 1, 1, 1, 1, 1]
Meaning: All succeed; new sub-filter created when threshold exceeded
```

---

## Test Category 5: Return Values (Mixed Results)

### Test 5.1: All New Items
```redis
Command: BF.INSERT myfilter ITEMS fresh1 fresh2 fresh3
Expected: *3\r\n:1\r\n:1\r\n:1\r\n
```

### Test 5.2: All Duplicates
```redis
# Insert items first
Command: BF.INSERT myfilter ITEMS item1 item2
Expected: *2\r\n:1\r\n:1\r\n

# Re-insert same items
Command: BF.INSERT myfilter ITEMS item1 item2
Expected: *2\r\n:0\r\n:0\r\n
Decoded: [0, 0]
```

### Test 5.3: Mixed New and Duplicate
```redis
Command: BF.INSERT myfilter ITEMS item1 item2
Expected: *2\r\n:1\r\n:1\r\n

Command: BF.INSERT myfilter ITEMS item1 item3 item2 item4
Expected: *4\r\n:0\r\n:1\r\n:0\r\n:1\r\n
Decoded: [0, 1, 0, 1]
Meaning: item1 (dup), item3 (new), item2 (dup), item4 (new)
```

---

## Test Category 6: Error Cases

### Test 6.1: Invalid CAPACITY (Zero)
```redis
Command: BF.INSERT filter CAPACITY 0 ITEMS item
Expected: -ERR capacity must be greater than 0\r\n
```

### Test 6.2: Invalid CAPACITY (Negative)
```redis
Command: BF.INSERT filter CAPACITY -100 ITEMS item
Expected: -ERR capacity must be greater than 0\r\n
```

### Test 6.3: Invalid ERROR (Outside Range)
```redis
Command: BF.INSERT filter ERROR 0.0 ITEMS item
Expected: -ERR error rate must be between 0 and 1\r\n

Command: BF.INSERT filter ERROR 1.5 ITEMS item
Expected: -ERR error rate must be between 0 and 1\r\n
```

### Test 6.4: Invalid ERROR (Not Float)
```redis
Command: BF.INSERT filter ERROR abc ITEMS item
Expected: -ERR error rate must be a valid float\r\n
```

### Test 6.5: Invalid EXPANSION (Zero)
```redis
Command: BF.INSERT filter EXPANSION 0 ITEMS item
Expected: -ERR expansion must be greater than 0\r\n
```

### Test 6.6: Missing ITEMS Keyword
```redis
Command: BF.INSERT filter CAPACITY 100 item1 item2
Expected: -ERR ITEMS expected\r\n
```

### Test 6.7: No Items Provided
```redis
Command: BF.INSERT filter ITEMS
Expected: -ERR wrong number of arguments for 'bf.insert' command\r\n
```

### Test 6.8: WRONGTYPE Error (Key is String)
```redis
# Create a string key
Command: SET mykey "hello"
Expected: +OK\r\n

# Try to use BF.INSERT on it
Command: BF.INSERT mykey ITEMS item
Expected: -WRONGTYPE Operation against a key holding the wrong kind of value\r\n
```

---

## Test Category 7: Binary-Safe Items

### Test 7.1: Items with Null Bytes
```redis
Command: BF.INSERT binaryfilter ITEMS "hello\x00world" "test\x00data"
Expected: *2\r\n:1\r\n:1\r\n
Meaning: Binary strings handled correctly
```

### Test 7.2: Items with Special Characters
```redis
Command: BF.INSERT specialfilter ITEMS "item\r\nwith\r\nnewlines" "item\twith\ttabs"
Expected: *2\r\n:1\r\n:1\r\n
```

### Test 7.3: Empty String Item
```redis
Command: BF.INSERT filter ITEMS "" "nonempty"
Expected: *2\r\n:1\r\n:1\r\n
Meaning: Empty string is a valid (unusual) item
```

---

## Test Category 8: RESP3 Protocol Support

### Test 8.1: RESP3 Returns Booleans (Not Integers)
```
# Client sends HELLO 3 first to switch to RESP3
Command: HELLO 3
Expected: %7\r\n ... (map response)

Command: BF.INSERT myfilter ITEMS item1 item2
Expected: *2\r\n#t\r\n#t\r\n
Decoded: [true, true]  (booleans, not 1, 1)
```

### Test 8.2: RESP3 Duplicates as False
```
# After items already inserted
Command: BF.INSERT myfilter ITEMS item1 item3
Expected: *2\r\n#f\r\n#t\r\n
Decoded: [false, true]  (booleans)
```

---

## Test Category 9: Concurrent/Stress Tests

### Test 9.1: Large Batch (1000 items)
```redis
Command: BF.INSERT bigfilter ITEMS item1 item2 ... item1000
Expected: *1000\r\n:1\r\n:1\r\n ... (all 1s)
Meaning: All 1000 items inserted successfully
```

### Test 9.2: Many Sequential Calls
```redis
# Call BF.INSERT 100 times on same filter
Command: BF.INSERT filter ITEMS item1 (repeated 100x)
Expected: First call: [1], subsequent 99 calls: [0]
Meaning: First insert is new, subsequent are duplicates
```

### Test 9.3: Parameter Parsing with Mixed Case
```redis
Command: BF.INSERT filter capacity 100 error 0.01 EXPANSION 2 NOCREATE items item1
# Parser should be case-insensitive for keywords
Expected: Works (or correct error if NOCREATE prevents creation)
```

---

## Test Category 10: Edge Cases

### Test 10.1: Key Already Exists (Wrong Type)
```redis
Command: LPUSH mykey element
Expected: :1\r\n

Command: BF.INSERT mykey ITEMS item
Expected: -WRONGTYPE Operation against a key holding the wrong kind of value\r\n
```

### Test 10.2: Expired Key
```redis
# Create key with expiry
Command: BF.INSERT tempkey ITEMS item1
Expected: *1\r\n:1\r\n
Command: EXPIRE tempkey 0  (expire immediately)
Expected: :1\r\n

# Now key is expired; should create new filter
Command: BF.INSERT tempkey ITEMS item2
Expected: *1\r\n:1\r\n  (new filter created)
```

### Test 10.3: Multiple Parameters Same Type
```redis
Command: BF.INSERT filter CAPACITY 1000 CAPACITY 2000 ITEMS item
# Behavior: Last CAPACITY wins, or error (implementation choice)
# Redis typically allows override; later value wins
```

### Test 10.4: Single Item Batch
```redis
Command: BF.INSERT singlefilter ITEMS oneitem
Expected: *1\r\n:1\r\n
Meaning: Array with one element
```

---

## Implementation Validation Checklist

When implementing BF.INSERT, verify these test cases pass:

**Creation Tests**:
- [ ] Auto-create with defaults
- [ ] Auto-create with CAPACITY
- [ ] Auto-create with ERROR
- [ ] Auto-create with EXPANSION
- [ ] Auto-create with NONSCALING
- [ ] Ignore params when filter exists

**NOCREATE Tests**:
- [ ] NOCREATE on existing filter
- [ ] NOCREATE on nonexistent filter (error)
- [ ] NOCREATE + CAPACITY mutual exclusion
- [ ] NOCREATE + ERROR mutual exclusion

**Return Value Tests**:
- [ ] All new items return [1, 1, 1, ...]
- [ ] All duplicates return [0, 0, 0, ...]
- [ ] Mixed return [1, 0, 1, ...]

**NONSCALING Tests**:
- [ ] No overflow: all succeed
- [ ] With overflow: partial success with error strings
- [ ] Partial success is NOT rolled back

**Error Tests**:
- [ ] Invalid CAPACITY
- [ ] Invalid ERROR
- [ ] Invalid EXPANSION
- [ ] WRONGTYPE error
- [ ] Missing ITEMS
- [ ] Empty ITEMS list

**RESP3 Tests**:
- [ ] Booleans returned (true/false) instead of 1/0
- [ ] Errors still as strings

**Stress Tests**:
- [ ] Large batches (1000+ items)
- [ ] Binary-safe items (null bytes)
- [ ] Concurrent operations

---

## Redis Compatibility Matrix

| Aspect | Expected | Implementation Notes |
|--------|----------|----------------------|
| Return format | Array of integers (RESP2) or booleans (RESP3) | Use protocol version |
| Error strings | Exact match with Redis | Use canonical error messages |
| Overflow behavior | Partial success, not rolled back | Commit each item independently |
| Creation params | Ignored if filter exists | Don't use them if key exists |
| NOCREATE logic | Error if key missing | Check exists before insert |

