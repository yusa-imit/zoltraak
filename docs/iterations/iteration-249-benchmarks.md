# Iteration 249 — Performance Benchmarking Requirements

**Status**: Deferred to Stabilization session (Session #145 or next 5th session)
**Reason**: Test Execution Policy restricts benchmarking to Stabilization sessions only

---

## 1. Notification Overhead Benchmark

**Target**: 0-10% overhead when notifications enabled

### Test Methodology

1. **Baseline (notifications disabled)**:
   ```bash
   redis-benchmark -t set,get,lpush,hset,zadd -n 1000000 -q
   ```
   Record throughput for each operation.

2. **With notifications (CONFIG SET notify-keyspace-events "KEA")**:
   ```bash
   redis-benchmark -t set,get,lpush,hset,zadd -n 1000000 -q
   ```
   Record throughput for each operation.

3. **Calculate overhead**:
   ```
   Overhead = ((Baseline - WithNotifications) / Baseline) * 100%
   ```

4. **Expected Results**:
   - GET operations: 0-2% overhead (read-only, minimal notification work)
   - SET operations: 5-10% overhead (writes trigger notifications)
   - LPUSH/HSET/ZADD: 5-10% overhead
   - Overall average: ≤ 10%

### Test Configurations

| Config | Flags | Description |
|--------|-------|-------------|
| Disabled | "" | No notifications |
| Minimal | "Kg" | Generic events only |
| Typical | "KEx" | Generic + expired |
| Full | "KEA" | All events |

Run benchmarks for each configuration to measure overhead scaling.

---

## 2. Pub/Sub Delivery Performance

**Target**: No regression in message delivery throughput

### Test Methodology

1. **Baseline RESP2**:
   - 10,000 subscribers on single channel
   - Publish 100,000 messages
   - Measure: msgs/sec delivered

2. **RESP3 push messages**:
   - 10,000 RESP3 subscribers on single channel
   - Publish 100,000 messages
   - Measure: msgs/sec delivered

3. **Mixed mode**:
   - 5,000 RESP2 + 5,000 RESP3 subscribers
   - Publish 100,000 messages
   - Measure: msgs/sec delivered

4. **Expected Results**:
   - RESP3 should be ≥ RESP2 (simpler format)
   - Mixed mode should have no regression
   - Target: ≥ 100,000 msgs/sec/subscriber

---

## 3. Memory Overhead

**Target**: No significant memory increase

### Test Methodology

1. **Before** (RESP2 only):
   ```bash
   redis-cli INFO memory | grep used_memory_human
   ```

2. **After** (with RESP3 support):
   - 10,000 active RESP3 subscribers
   - Each subscribed to 10 channels
   - Check memory usage

3. **Expected Results**:
   - Per-subscriber overhead: ≤ 1 KB (version field: 1 byte)
   - Total overhead for 10,000 subscribers: ≤ 10 MB
   - No memory leaks after 1M operations

---

## 4. Notification Event Performance

**Target**: Event firing adds minimal latency

### Test Methodology

Use `DEBUG SLEEP` to simulate workload and measure latency:

1. **Latency test script**:
   ```python
   import redis
   import time

   r = redis.Redis()
   r.config_set('notify-keyspace-events', '')

   start = time.time()
   for i in range(100000):
       r.set(f'key:{i}', 'value')
   baseline = time.time() - start

   r.config_set('notify-keyspace-events', 'K$')

   start = time.time()
   for i in range(100000):
       r.set(f'key:{i}', 'value')
   with_notifications = time.time() - start

   print(f'Baseline: {baseline:.2f}s')
   print(f'With notifications: {with_notifications:.2f}s')
   print(f'Overhead: {((with_notifications - baseline) / baseline * 100):.1f}%')
   ```

2. **Expected Results**:
   - p50 latency: ≤ 5% increase
   - p99 latency: ≤ 10% increase
   - p999 latency: ≤ 15% increase

---

## 5. Expired/Evicted Event Performance

**Target**: Background expiration/eviction maintains throughput

### Test Methodology

1. **Set up expiring keys**:
   ```bash
   for i in {1..100000}; do
     redis-cli SETEX "expire:$i" 10 "value"
   done
   ```

2. **Monitor expired events**:
   ```bash
   redis-cli --csv PSUBSCRIBE '__keyevent@0__:expired' &
   ```

3. **Measure**:
   - Count expired events per second
   - Monitor CPU usage during expiration
   - Check SET/GET throughput during expiration

4. **Expected Results**:
   - Expiration events: 10,000+/sec
   - CPU overhead: ≤ 10%
   - SET/GET throughput: ≥ 90% of baseline

---

## 6. Differential Testing

**Target**: Byte-for-byte compatibility with Redis

### Test Methodology

Run `redis-compatibility-validator` agent with:

1. **RESP2 message format**:
   - SUBSCRIBE, PUBLISH, message delivery
   - All notification event types
   - Compare RESP bytes with real Redis

2. **RESP3 push format**:
   - HELLO 3, SUBSCRIBE, PUBLISH
   - Verify push message format matches Redis 7.0+

3. **Expected Results**:
   - 100% byte compatibility for RESP2
   - 100% byte compatibility for RESP3 push
   - All event types match Redis behavior

---

## 7. Stress Tests

**Target**: Stability under load

### Test Scenarios

1. **Many subscribers**:
   - 10,000 subscribers on 1 channel
   - Publish 1M messages
   - No crashes, no memory leaks

2. **Many channels**:
   - 100 subscribers on 10,000 unique channels
   - Publish 1M total messages
   - Verify channel isolation

3. **Pattern matching**:
   - 1,000 PSUBSCRIBE patterns
   - Publish 100k messages to matching channels
   - Measure pattern matching overhead

4. **Long-running**:
   - 24-hour test with constant load
   - Monitor memory growth
   - Check for leaks or degradation

---

## Execution Instructions

### For Stabilization Sessions

```bash
# Check session counter
COUNTER=$(cat .claude/session-counter)
if [ $((COUNTER % 5)) -eq 0 ]; then
  echo "STABILIZATION SESSION - Running benchmarks"

  # 1. Baseline benchmark
  ./scripts/bench-notifications.sh baseline

  # 2. Full benchmark
  ./scripts/bench-notifications.sh full

  # 3. Generate report
  ./scripts/bench-report.sh > docs/benchmarks/iteration-249-$(date +%Y%m%d).md

  # 4. Check for regressions
  ./scripts/bench-compare.sh
fi
```

### Success Criteria

✅ All benchmarks pass with ≤ 10% overhead
✅ No memory leaks detected
✅ RESP3 format matches Redis byte-for-byte
✅ No performance regression vs baseline

---

**Next Steps**: Execute benchmarks in next Stabilization session (Session #145 or 150).
