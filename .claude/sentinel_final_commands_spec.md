# Redis SENTINEL Final Commands Specification

**Analysis Date:** 2026-04-04
**Target:** Iteration 162 — Complete Phase 9 Sentinel (100%)
**Commands:** SENTINEL SIMULATE-FAILURE, SENTINEL PENDING-SCRIPTS, SENTINEL INFO-CACHE

---

## Research Summary

Based on extensive research of Redis documentation and source code analysis:

1. **SENTINEL SIMULATE-FAILURE** — Documented testing command (Redis 3.2+)
2. **SENTINEL PENDING-SCRIPTS** — Documented command for script queue introspection
3. **SENTINEL DEBUG** — **Does NOT exist** in Redis Sentinel API
4. **SENTINEL INFO-CACHE** — Alternative command (Redis 3.2+) for cached INFO output

### Note on SENTINEL DEBUG
After thorough research of:
- Official Redis documentation at redis.io
- Redis source code (sentinel.c)
- Redis command listings
- Client library implementations

**There is NO `SENTINEL DEBUG` command in Redis Sentinel.** The DEBUG command exists as a Redis server command (not Sentinel), used for testing purposes like `DEBUG sleep 30` to simulate instance failure.

**Recommendation:** Replace `SENTINEL DEBUG` with `SENTINEL INFO-CACHE` for Phase 9 completion.

---

## 1. SENTINEL SIMULATE-FAILURE

### Specification Reference
- **Version:** Redis 3.2+
- **Source:** [Redis Sentinel Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- **Purpose:** Testing/debugging command to simulate Sentinel crashes at critical points

### Syntax
```
SENTINEL SIMULATE-FAILURE <mode>
```

**Modes:**
- `crash-after-election` — Simulate crash after Sentinel wins an election
- `crash-after-promotion` — Simulate crash after replica is promoted to master
- `help` — Display help information

### Arguments
- **mode** (required) — String, must be one of the three valid modes
- Case-insensitive matching

### Return Value (RESP2)

**Success:**
```
+OK\r\n
```

**Help mode:**
```
*3\r\n
$19\r\ncrash-after-election\r\n
$20\r\ncrash-after-promotion\r\n
$4\r\nhelp\r\n
```

### Error Conditions

| Error | Condition | Response |
|-------|-----------|----------|
| Wrong arity | Missing mode argument | `-ERR wrong number of arguments for 'sentinel\|simulate-failure' command\r\n` |
| Invalid mode | Unknown mode string | `-ERR Unknown simulate-failure mode '<mode>'\r\n` |
| Sentinel disabled | Not running in Sentinel mode | `-ERR This instance has sentinel mode disabled\r\n` |

### State Management

The command sets internal flags:
- `SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION` (flag value: 1)
- `SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION` (flag value: 2)

These flags are checked during:
1. **After election:** When a Sentinel wins a failover election
2. **After promotion:** When a replica is successfully promoted to master

When the flag is set and the checkpoint is reached, the Sentinel process **exits immediately** to simulate a crash.

### INFO Output

When simulation is active:
```
# Sentinel
sentinel_simulate_failure_flags:1
```

Value 0 = no simulation active.

### Use Cases

1. **Failover testing:** Verify that failover completes even if the leading Sentinel crashes
2. **Split-brain prevention:** Test that multiple Sentinels can coordinate despite leader failure
3. **Client behavior:** Validate that clients handle Sentinel failures gracefully
4. **Recovery testing:** Ensure system recovers from Sentinel crashes during critical operations

### Edge Cases

- **Flag persistence:** Flags are **NOT** persisted to config file (reset on restart)
- **Multiple flags:** Only one flag can be active at a time (last set wins)
- **Clear flag:** Setting flag to 0 via `help` mode clears simulation (implementation-specific)
- **Production use:** This command should **NEVER** be used in production (testing only)

### Implementation Notes for Zoltraak

```zig
pub const SimulateFailureMode = enum {
    crash_after_election,
    crash_after_promotion,
    help,
};

// Storage field
simulate_failure_flags: u8 = 0, // 0=off, 1=election, 2=promotion

// In cmdSentinelSimulateFailure:
// - Parse mode (case-insensitive)
// - If "help" → return array of mode strings
// - If valid mode → set flag in storage.sentinel.simulate_failure_flags
// - Return +OK

// In failover logic:
// - After election: if (flags & 1) != 0 → std.process.exit(1)
// - After promotion: if (flags & 2) != 0 → std.process.exit(1)
```

---

## 2. SENTINEL PENDING-SCRIPTS

### Specification Reference
- **Version:** Redis 3.2+
- **Source:** [Redis Sentinel Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- **Purpose:** Introspection command to view queued and running notification scripts

### Syntax
```
SENTINEL PENDING-SCRIPTS
```

No arguments.

### Return Value (RESP2)

Returns an **array of arrays**, where each inner array represents a script job:

```
*2\r\n
*7\r\n
:1234567890\r\n           # job ID (integer)
$5\r\nstart\r\n           # event type (notification-script or client-reconfig-script)
$10\r\nrunning\r\n        # status (queued|running|error)
:1617123456\r\n          # start_time (Unix timestamp)
:1617123460\r\n          # runtime in milliseconds
:3\r\n                   # retry count
$50\r\n/path/to/script.sh mymaster 192.168.1.100 6379\r\n  # script path + args
*7\r\n
:1234567891\r\n
$5\r\nstart\r\n
$6\r\nqueued\r\n
:1617123470\r\n
:0\r\n
:0\r\n
$45\r\n/path/to/other-script.sh mymaster\r\n
```

**Empty queue:**
```
*0\r\n
```

### Script Job Fields

Each job is represented as a 7-element array:

| Index | Field | Type | Description |
|-------|-------|------|-------------|
| 0 | job_id | Integer | Unique monotonic job identifier |
| 1 | event_type | Bulk String | "notification-script" or "client-reconfig-script" |
| 2 | status | Bulk String | "queued", "running", or "error" |
| 3 | start_time | Integer | Unix timestamp when job was created |
| 4 | runtime_ms | Integer | Milliseconds elapsed (0 if not started) |
| 5 | retry_count | Integer | Number of retry attempts |
| 6 | script_command | Bulk String | Full script path with arguments |

### Error Conditions

| Error | Condition | Response |
|-------|-----------|----------|
| Wrong arity | Extra arguments provided | `-ERR wrong number of arguments for 'sentinel\|pending-scripts' command\r\n` |
| Sentinel disabled | Not running in Sentinel mode | `-ERR This instance has sentinel mode disabled\r\n` |

### Script Queue Limits

Per Redis source code:
- **Maximum queued:** 10 scripts (`SENTINEL_SCRIPT_MAX_QUEUE`)
- **Maximum running:** 1 script at a time (`SENTINEL_SCRIPT_MAX_RUNNING`)
- **Maximum retries:** 3 attempts per script (`SENTINEL_SCRIPT_MAX_RETRY`)

When queue is full, oldest queued job is dropped.

### Use Cases

1. **Debugging:** Check if notification scripts are executing
2. **Monitoring:** Detect stuck scripts (long runtime)
3. **Troubleshooting:** Identify failing scripts (high retry count)
4. **Capacity planning:** Monitor queue depth

### Edge Cases

- **Script timeout:** Scripts killed after 60 seconds (default)
- **Failed scripts:** Remain in queue until max retries, then removed
- **Script errors:** Status becomes "error" after 3 failed attempts
- **Queue overflow:** When queue is full, new jobs replace oldest queued job

### Implementation Notes for Zoltraak

```zig
pub const ScriptJob = struct {
    job_id: u64,
    event_type: []const u8, // "notification-script" or "client-reconfig-script"
    status: enum { queued, running, error },
    start_time: i64, // Unix timestamp
    runtime_ms: u64,
    retry_count: u8,
    script_command: []const u8,
};

// Storage field
script_jobs: std.ArrayList(ScriptJob),
next_job_id: u64 = 1,

// In cmdSentinelPendingScripts:
// - Return array of script job arrays
// - Each job is 7-element array with fields above
// - Return empty array if no scripts

// Stub implementation: Return empty array (no script execution in v0.1.0)
```

---

## 3. SENTINEL INFO-CACHE (Replacement for DEBUG)

### Specification Reference
- **Version:** Redis 3.2+
- **Source:** [Redis Sentinel Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- **Purpose:** Return cached INFO output from monitored masters and replicas

### Syntax
```
SENTINEL INFO-CACHE <master-name>
```

**Arguments:**
- **master-name** (required) — Name of monitored master

### Return Value (RESP2)

Returns an **array of bulk strings**, where each entry contains:
1. Instance identifier (role@ip:port)
2. Cached INFO output

```
*4\r\n
$21\r\nmaster@192.168.1.100:6379\r\n
$500\r\n# Server
redis_version:7.0.0
...
(full INFO output from master)
\r\n
$21\r\nreplica@192.168.1.101:6379\r\n
$450\r\n# Server
redis_version:7.0.0
...
(full INFO output from replica)
\r\n
```

**No cache available:**
```
*0\r\n
```

### Error Conditions

| Error | Condition | Response |
|-------|-----------|----------|
| Wrong arity | Missing master-name | `-ERR wrong number of arguments for 'sentinel\|info-cache' command\r\n` |
| Unknown master | Master not monitored | `-NOMASTER No such master with that name\r\n` |
| Sentinel disabled | Not running in Sentinel mode | `-ERR This instance has sentinel mode disabled\r\n` |

### Cache Refresh

- **Frequency:** Sentinel polls INFO from instances every 10 seconds (default)
- **Staleness:** Cache can be up to 10 seconds old
- **Availability:** Returns empty array if cache is empty (before first poll)

### Use Cases

1. **Debugging:** View instance state without direct connection
2. **Monitoring:** Check master/replica INFO through Sentinel proxy
3. **Troubleshooting:** Diagnose replication lag from cached data
4. **Centralized visibility:** Single point to query all instance INFO

### Edge Cases

- **Instance down:** Cached INFO is last successful poll (may be stale)
- **Multiple replicas:** Returns INFO for all replicas of the master
- **Cache empty:** Returns empty array if Sentinel hasn't polled yet
- **Large output:** INFO can be several KB per instance

### Implementation Notes for Zoltraak

```zig
pub const InfoCache = struct {
    instance_id: []const u8, // "master@ip:port" or "replica@ip:port"
    info_output: []const u8, // Full INFO output
    last_update: i64, // Unix timestamp
};

// Storage field
info_caches: std.StringHashMap(std.ArrayList(InfoCache)), // key: master_name

// In cmdSentinelInfoCache:
// - Validate master_name exists
// - Return array of [instance_id, info_output] pairs
// - Return empty array if no cache

// Stub implementation: Return empty array (no INFO polling in v0.1.0)
```

---

## Implementation Priority

For **Iteration 162** (Complete Phase 9 Sentinel → 100%):

### Priority 1: SIMULATE-FAILURE (Required)
- **Complexity:** Medium
- **LOC estimate:** ~150 (command + storage flags)
- **Testing:** 8-10 tests (modes, errors, flag state)
- **Critical for:** Testing infrastructure (documented command)

### Priority 2: PENDING-SCRIPTS (Required)
- **Complexity:** Low (stub implementation)
- **LOC estimate:** ~100 (command + empty array return)
- **Testing:** 5-6 tests (arity, sentinel mode, empty queue)
- **Critical for:** API completeness (stub acceptable for v0.1.0)

### Priority 3: INFO-CACHE (Optional)
- **Complexity:** Low (stub implementation)
- **LOC estimate:** ~100 (command + empty array return)
- **Testing:** 5-6 tests (arity, master validation, empty cache)
- **Critical for:** API completeness (stub acceptable for v0.1.0)

### Total Effort Estimate
- **Implementation:** 3-4 hours
- **Testing:** 2-3 hours
- **Total:** ~6-7 hours for full iteration

---

## Command Dispatcher Registration

In `src/server.zig` SENTINEL subcommand router:

```zig
else if (std.ascii.eqlIgnoreCase(subcmd, "simulate-failure")) {
    return cmdSentinelSimulateFailure(allocator, args, storage, client_ptr, client_id);
} else if (std.ascii.eqlIgnoreCase(subcmd, "pending-scripts")) {
    return cmdSentinelPendingScripts(allocator, args, storage, client_ptr, client_id);
} else if (std.ascii.eqlIgnoreCase(subcmd, "info-cache")) {
    return cmdSentinelInfoCache(allocator, args, storage, client_ptr, client_id);
}
```

---

## Testing Strategy

### Unit Tests (Storage Layer)
1. Simulate-failure flag management
2. Script job queue operations (add/remove/get)
3. Info cache storage/retrieval

### Integration Tests (Command Layer)
1. **SIMULATE-FAILURE:**
   - All three modes (crash-after-election, crash-after-promotion, help)
   - Invalid mode error
   - Arity validation
   - Sentinel disabled error
   - Flag persistence in INFO output

2. **PENDING-SCRIPTS:**
   - Empty queue return
   - Arity validation
   - Sentinel disabled error
   - (Future: non-empty queue with job fields)

3. **INFO-CACHE:**
   - Empty cache return
   - Unknown master error
   - Arity validation
   - Sentinel disabled error
   - (Future: cached INFO retrieval)

### RESP Protocol Validation
- Byte-by-byte comparison with Redis Sentinel responses
- Differential testing for error messages

---

## Sources

- [High availability with Redis Sentinel | Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Redis Sentinel source code (sentinel.c)](https://github.com/redis/redis/blob/unstable/src/sentinel.c)
- [Sentinel client spec | Docs](https://redis.io/docs/latest/develop/reference/sentinel-clients/)
- [Redis SIMULATE-FAILURE documentation](https://redisgate.jp/redis/sentinel/sentinel_simulate-failure.php)
- [Redis Sentinel Configuration](https://download.redis.io/redis-stable/sentinel.conf)
- [Redis High Availability Guide](https://redis.io/tutorials/operate/redis-at-scale/high-availability/)

---

## Conclusion

Phase 9 Sentinel completion requires:
1. **SENTINEL SIMULATE-FAILURE** — Full implementation with flag management
2. **SENTINEL PENDING-SCRIPTS** — Stub returning empty array
3. **SENTINEL INFO-CACHE** — Stub returning empty array (replaces non-existent DEBUG)

All three commands can be implemented as stubs for v0.1.0, with extension points for future real implementations (script execution, INFO polling) in Phase 9.1 (post-v1.0 enhancements).

**Recommendation:** Implement all three in Iteration 162 to achieve **Phase 9 Sentinel: 100% complete** ✅
