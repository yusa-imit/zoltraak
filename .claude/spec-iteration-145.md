# Iteration 145 Specification: CLUSTER SAVECONFIG & BUMPEPOCH

**Phase**: 8.11 — Cluster Config Epoch Management
**Date**: 2026-03-29
**Status**: Planning complete, ready for implementation

---

## Overview

This iteration implements two critical cluster administration commands for managing cluster configuration persistence and epoch versioning:

1. **CLUSTER SAVECONFIG** — Forces immediate persistence of cluster configuration to disk
2. **CLUSTER BUMPEPOCH** — Manually advances the configuration epoch without consensus

These commands complete the cluster configuration management foundation, bringing Phase 8 to 70% completion.

---

## 1. CLUSTER SAVECONFIG

### Redis Specification

**Reference**: https://redis.io/commands/cluster-saveconfig/

#### Syntax
```
CLUSTER SAVECONFIG
```

#### Command Details
- **Available since**: Redis 3.0.0
- **Time complexity**: O(1)
- **ACL categories**: `@admin`, `@slow`, `@dangerous`
- **Command flags**: admin, stale, no_async_loading
- **Arity**: 2 (command + 0 arguments)

#### Description

Forces a node to save the `nodes.conf` configuration file to disk. The command calls `fsync(2)` to ensure the configuration is flushed to disk before returning.

**Use cases**:
1. **Disaster recovery**: Regenerate a lost or deleted `nodes.conf` file
2. **Explicit persistence**: Ensure configuration changes are immediately persisted
3. **Safety guarantee**: While most CLUSTER commands auto-schedule persistence, this guarantees immediate write

**Important**: This is primarily for disaster recovery. Redis nodes typically auto-persist configuration after changes.

#### Return Values

**RESP2/RESP3**:
- **Success**: Simple string reply `OK`
- **Error**: Error reply if operation fails (e.g., disk full, permission denied)

#### Error Conditions

Potential errors:
- **Disk full**: Cannot write `nodes.conf`
- **Permission denied**: No write access to cluster config directory
- **I/O error**: fsync failure

### nodes.conf File Format

The `nodes.conf` file stores cluster configuration in plain text format with one line per node:

```
<node-id> <ip>:<port@cluster-port> <flags> <master-id|-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-ranges>
```

**Fields**:
1. `node-id`: 40-character hex string (unique node identifier)
2. `ip:port@cluster-port`: Node address (e.g., `127.0.0.1:7000@17000`)
3. `flags`: Comma-separated (e.g., `myself,master` or `slave` or `fail,master`)
4. `master-id`: Master's node ID if replica, `-` if master
5. `ping-sent`: Unix milliseconds of last PING sent (0 if none)
6. `pong-recv`: Unix milliseconds of last PONG received
7. `config-epoch`: Configuration epoch for this node
8. `link-state`: `connected` or `disconnected`
9. `slot-ranges`: Space-separated slot assignments (e.g., `0-5460 5461-10922`)

**Special lines**:
- `vars currentEpoch <epoch> lastVoteEpoch <epoch>`: Cluster-wide epoch state
- `vars bumpepoch <epoch>`: Manual epoch bump tracking (optional)

**Example**:
```
07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460
vars currentEpoch 6 lastVoteEpoch 0
```

### Implementation Requirements

#### Storage Layer (`src/storage/cluster.zig`)

**Method signature**:
```zig
/// Save cluster configuration to nodes.conf file
/// Calls fsync to ensure durability before returning
/// Returns error if file write or fsync fails
pub fn saveConfig(self: *ClusterState, config_path: []const u8) !void
```

**Implementation steps**:
1. Open/create `nodes.conf` file for writing
2. Write header comment (optional): `# Cluster configuration file, created by zoltraak`
3. Iterate over `self.nodes` and write each node's line:
   - Format: `<node-id> <ip>:<port@cluster-port> <flags> <master-id|-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-ranges>`
   - Compress slot ranges (e.g., `0 1 2 3` → `0-3`)
4. Write vars line: `vars currentEpoch <epoch> lastVoteEpoch 0`
5. Call `fsync(fd)` to flush to disk
6. Close file

**Edge cases**:
- Empty cluster (only myself): Write single line + vars
- No slots assigned: Write node line with empty slot-ranges
- Replica nodes: Use master's node ID, not `-`

**Error handling**:
- Propagate file I/O errors (disk full, permission denied)
- Clean up partial writes on error (delete incomplete file)

#### Command Layer (`src/commands/cluster.zig`)

**Handler signature**:
```zig
pub fn cmdClusterSaveConfig(
    args: [][]const u8,
    storage: *Storage,
    client_id: u64,
) ![]const u8
```

**Implementation**:
1. Validate arity: `args.len == 1` (just "SAVECONFIG")
2. Check cluster enabled: `if (!storage.cluster_state.enabled) return error.ClusterDisabled`
3. Call `storage.cluster_state.saveConfig()` with config path
4. Return `+OK\r\n` on success
5. Return error on failure (e.g., `-ERR Could not save config: <reason>\r\n`)

**Config path**:
- Default: `./nodes.conf` (relative to server working directory)
- Future: Allow configuration via `cluster-config-file` config option

**Integration**:
- Register in `strings.zig` CLUSTER dispatcher: `"saveconfig" => cmdClusterSaveConfig`
- Add to ACL write commands list (modifies disk state)
- Add to skip_redirect_cmds list (SAVECONFIG is local-only, no slot routing)

---

## 2. CLUSTER BUMPEPOCH

### Redis Specification

**Reference**: https://redis.io/commands/cluster-bumpepoch/

#### Syntax
```
CLUSTER BUMPEPOCH
```

#### Command Details
- **Available since**: Redis 3.0.0
- **Time complexity**: O(1)
- **ACL categories**: `@admin`, `@slow`, `@dangerous`
- **Command flags**: admin, stale, no_async_loading
- **Arity**: 2 (command + 0 arguments)

#### Description

Triggers an increment to the cluster's configuration epoch from the connected node. The epoch will be incremented if:
1. The node's `config_epoch` is zero, **OR**
2. The node's `config_epoch` is less than the cluster's greatest epoch (`current_epoch`)

**CRITICAL WARNING**: Config epoch management is performed internally by the cluster via consensus. `CLUSTER BUMPEPOCH` attempts to increment the epoch **WITHOUT** getting consensus, which may violate the "last failover wins" rule. **Use with extreme caution.**

**Legitimate use cases**:
- Forcing a specific node to take ownership during manual resharding
- Recovering from epoch conflicts after network partitions
- Testing and debugging cluster epoch logic

**Dangerous scenarios**:
- Can break slot ownership consistency if misused
- May cause split-brain if multiple nodes bump epoch independently
- Can override legitimate failover decisions

#### Return Values

**RESP2/RESP3**:
- **`BUMPED <epoch>`**: Epoch was successfully incremented to `<epoch>` (bulk string reply)
- **`STILL <epoch>`**: Node already has the greatest epoch in the cluster, no increment occurred (bulk string reply)

**Return format**:
```
$BUMPED 5\r\n  (if incremented to 5)
$STILL 8\r\n   (if already at highest epoch 8)
```

#### Epoch Advancement Rules

The command increments `config_epoch` to `current_epoch + 1` if:
```
(my_config_epoch == 0) OR (my_config_epoch < current_epoch)
```

After increment:
- `self.myself.config_epoch = self.current_epoch + 1`
- `self.current_epoch = self.myself.config_epoch` (update cluster-wide epoch)

**Example scenarios**:

| Before | After | Return |
|--------|-------|--------|
| `config_epoch=0, current_epoch=5` | `config_epoch=6, current_epoch=6` | `BUMPED 6` |
| `config_epoch=3, current_epoch=7` | `config_epoch=8, current_epoch=8` | `BUMPED 8` |
| `config_epoch=10, current_epoch=10` | No change | `STILL 10` |
| `config_epoch=12, current_epoch=10` | No change | `STILL 12` |

### Implementation Requirements

#### Storage Layer (`src/storage/cluster.zig`)

**Method signature**:
```zig
/// Attempt to bump the configuration epoch
/// Returns the new epoch if bumped, or the current epoch if already highest
/// Returns true if epoch was incremented, false if already at highest
pub fn bumpEpoch(self: *ClusterState) !struct { bumped: bool, epoch: u64 }
```

**Implementation logic**:
```zig
pub fn bumpEpoch(self: *ClusterState) !struct { bumped: bool, epoch: u64 } {
    const myself = self.myself orelse return error.ClusterNotInitialized;

    // Check if bump is needed
    if (myself.config_epoch == 0 or myself.config_epoch < self.current_epoch) {
        // Increment to current_epoch + 1
        const new_epoch = self.current_epoch + 1;
        myself.config_epoch = new_epoch;
        self.current_epoch = new_epoch;
        return .{ .bumped = true, .epoch = new_epoch };
    } else {
        // Already at or above current_epoch
        return .{ .bumped = false, .epoch = myself.config_epoch };
    }
}
```

**Side effects**:
- Updates `myself.config_epoch`
- Updates `self.current_epoch` (cluster-wide max)
- Should trigger `saveConfig()` to persist new epoch

**Thread safety**: Not required (single-threaded Redis model)

#### Command Layer (`src/commands/cluster.zig`)

**Handler signature**:
```zig
pub fn cmdClusterBumpEpoch(
    args: [][]const u8,
    storage: *Storage,
    client_id: u64,
) ![]const u8
```

**Implementation**:
1. Validate arity: `args.len == 1` (just "BUMPEPOCH")
2. Check cluster enabled: `if (!storage.cluster_state.enabled) return error.ClusterDisabled`
3. Call `storage.cluster_state.bumpEpoch()`
4. Format response:
   - If `bumped == true`: `$BUMPED <epoch>\r\n`
   - If `bumped == false`: `$STILL <epoch>\r\n`
5. Auto-save config: Call `saveConfig()` to persist new epoch

**Response formatting**:
```zig
var buf: [128]u8 = undefined;
const response = if (result.bumped)
    try std.fmt.bufPrint(&buf, "$BUMPED {d}\r\n", .{result.epoch})
else
    try std.fmt.bufPrint(&buf, "$STILL {d}\r\n", .{result.epoch});
return storage.allocator.dupe(u8, response);
```

**Integration**:
- Register in `strings.zig` CLUSTER dispatcher: `"bumpepoch" => cmdClusterBumpEpoch`
- Add to ACL write commands list (modifies cluster state)
- Add to skip_redirect_cmds list (local-only operation)

---

## 3. Configuration Epoch Semantics

### What is a Configuration Epoch?

**Configuration epochs** (`configEpoch`) are 64-bit unsigned integers that function as version numbers for a node's hash slot assignments. They implement a distributed consensus mechanism similar to Raft "terms."

**Key properties**:
1. **Per-node versioning**: Each master has its own `config_epoch`
2. **Monotonically increasing**: New epochs are always higher than previous
3. **Uniqueness guarantee**: No two masters should have the same epoch (conflict resolution via lexicographic node ID)
4. **Cluster-wide tracking**: `current_epoch` tracks the highest epoch seen by any node

### When Should It Be Incremented?

#### Automatic Increments (Normal Operation)

1. **Replica promotion during failover**:
   - When a replica wins an election to replace a failed master
   - New master gets `config_epoch = max(all_masters_epochs) + 1`
   - Guarantees new master has higher epoch than all existing masters

2. **Slot migration completion**:
   - When `CLUSTER SETSLOT <slot> NODE <node-id>` finalizes a slot import
   - Target node gets `config_epoch = current_epoch + 1`
   - Ensures imported slot ownership has higher epoch than previous owner

3. **Manual failover with TAKEOVER**:
   - `CLUSTER FAILOVER TAKEOVER` bypasses majority vote
   - Replica immediately promotes with new highest epoch

#### Manual Increments (Administrative)

4. **CLUSTER BUMPEPOCH command**:
   - Explicit manual increment without consensus
   - Used for recovery scenarios or forcing slot ownership
   - **Dangerous**: Can violate "last failover wins" rule

### How It's Used in Cluster Consensus

#### "Last Failover Wins" Rule

Configuration epochs implement conflict resolution for slot ownership disputes:

**Rule 1: Initial assignment**
```
If slot is unassigned (NULL):
  → Assign to first node claiming it
```

**Rule 2: Higher epoch wins**
```
If slot is assigned to node A with config_epoch=3:
AND node B claims slot with config_epoch=5:
  → Reassign slot to node B (higher epoch wins)
```

**Liveness property**:
> "Eventually all nodes agree that the owner of a slot is the node with the greatest `configEpoch` among nodes advertising it."

#### Epoch Usage in Failover Elections

**Master voting constraints**:
1. Masters vote only **once per epoch** (tracked via `lastVoteEpoch`)
2. Refuse votes for epochs ≤ `lastVoteEpoch`
3. Replica's master must be in FAIL state
4. Auth request `currentEpoch` must be > master's `currentEpoch`

**Example: Epoch prevents double-voting**
```
Master lastVoteEpoch = 5

Scenario:
1. Replica R1 requests vote for epoch 6 → Master votes OK
2. Replica R2 requests vote for epoch 6 → Master refuses (already voted for epoch 6)
3. Next election with epoch 7 → Master can vote again
```

### Relationship to Slot Ownership Changes

| Event | Epoch Change | Reason |
|-------|-------------|--------|
| **Failover (auto)** | `new_master.config_epoch = current_epoch + 1` | Ensures failover winner has highest epoch |
| **Slot migration (SETSLOT NODE)** | `target.config_epoch = current_epoch + 1` | Finalizes slot ownership with new epoch |
| **Epoch conflict** | Lower node ID increments | Resolves two nodes with same epoch |
| **Manual bump (BUMPEPOCH)** | `config_epoch = current_epoch + 1` | Forces epoch increase for administrative reasons |

### currentEpoch vs configEpoch

| Field | Scope | Purpose | Updates |
|-------|-------|---------|---------|
| **currentEpoch** | Cluster-wide | Tracks highest epoch seen by any node | Updated when receiving gossip with higher epoch |
| **configEpoch** | Per-node | Version number for this node's slot assignments | Updated during failover, migration, or manual bump |

**Relationship**:
- `current_epoch` is always ≥ any `config_epoch` in the cluster
- After failover: `new_master.config_epoch` becomes new `current_epoch`
- `current_epoch` propagates via gossip (PING/PONG messages)

### Persistence Requirements

Both `current_epoch` and `config_epoch` **must** be persisted to disk before continuing operations:

**From Redis Cluster Spec**:
> "Every time the `configEpoch` changes for some known node, it is permanently stored in the nodes.conf file by all the nodes that receive this information. The same also happens for the `currentEpoch` value. These two variables are guaranteed to be saved and `fsync-ed` to disk when updated before a node continues its operations."

**Implementation requirement**:
- After `bumpEpoch()`, immediately call `saveConfig()`
- After receiving higher epoch via gossip, persist to `nodes.conf`
- Use `fsync()` to guarantee durability

---

## 4. Implementation Architecture

### File Organization

**No new files needed**. All changes in existing files:

1. **src/storage/cluster.zig**:
   - `saveConfig()` — serialize cluster state to nodes.conf
   - `bumpEpoch()` — increment config epoch with validation

2. **src/commands/cluster.zig**:
   - `cmdClusterSaveConfig()` — SAVECONFIG handler
   - `cmdClusterBumpEpoch()` — BUMPEPOCH handler

3. **src/server.zig** (registration):
   - Add `"saveconfig"` and `"bumpepoch"` to CLUSTER dispatcher

### Storage Layer API

```zig
// src/storage/cluster.zig

/// Save cluster configuration to nodes.conf file
/// Format: one line per node + vars line for currentEpoch
/// Calls fsync before returning to guarantee durability
pub fn saveConfig(self: *ClusterState, config_path: []const u8) !void {
    // Implementation in storage layer
}

/// Attempt to bump configuration epoch
/// Increments if config_epoch == 0 OR config_epoch < current_epoch
/// Returns whether epoch was incremented and the resulting epoch value
pub fn bumpEpoch(self: *ClusterState) !struct { bumped: bool, epoch: u64 } {
    // Implementation in storage layer
}
```

### Command Layer API

```zig
// src/commands/cluster.zig

/// CLUSTER SAVECONFIG — Force config persistence to disk
pub fn cmdClusterSaveConfig(
    args: [][]const u8,
    storage: *Storage,
    client_id: u64,
) ![]const u8 {
    // Validate arity, call saveConfig(), return OK or error
}

/// CLUSTER BUMPEPOCH — Manually advance configuration epoch
pub fn cmdClusterBumpEpoch(
    args: [][]const u8,
    storage: *Storage,
    client_id: u64,
) ![]const u8 {
    // Validate arity, call bumpEpoch(), format BUMPED/STILL response
}
```

### Integration Points

**Command registration** (src/server.zig or src/commands/strings.zig):
```zig
// Inside CLUSTER subcommand dispatcher
if (std.mem.eql(u8, subcommand, "saveconfig")) {
    return cluster_mod.cmdClusterSaveConfig(args[1..], storage, client_id);
}
if (std.mem.eql(u8, subcommand, "bumpepoch")) {
    return cluster_mod.cmdClusterBumpEpoch(args[1..], storage, client_id);
}
```

**ACL permissions**:
- Add `"saveconfig"` and `"bumpepoch"` to write_commands list (both modify cluster state)
- Both require `@admin`, `@slow`, `@dangerous` categories

**Cluster redirect skip**:
- Add both to `skip_redirect_cmds` list (local-only operations, no slot routing)

---

## 5. Test Requirements

### Unit Tests (src/storage/cluster.zig)

**Test: saveConfig writes correct format**
```zig
test "saveConfig writes nodes.conf with correct format" {
    // Setup: Create cluster with 3 nodes (2 masters, 1 replica)
    // Action: Call saveConfig()
    // Assert: File exists, contains 3 node lines + vars line
    // Assert: Slot ranges are compressed (e.g., "0-100 200-300")
    // Assert: Replica has master_id, master has "-"
}
```

**Test: saveConfig calls fsync**
```zig
test "saveConfig ensures durability with fsync" {
    // Setup: Mock file system operations
    // Action: Call saveConfig()
    // Assert: fsync() was called before file close
    // Note: May be difficult to test directly, verify in code review
}
```

**Test: saveConfig handles I/O errors**
```zig
test "saveConfig returns error on disk full" {
    // Setup: Mock file write to fail with ENOSPC
    // Action: Call saveConfig()
    // Assert: Returns error.OutOfSpace
    // Assert: Partial file is cleaned up (deleted)
}
```

**Test: bumpEpoch increments from zero**
```zig
test "bumpEpoch increments when config_epoch is zero" {
    // Setup: myself.config_epoch = 0, current_epoch = 5
    // Action: Call bumpEpoch()
    // Assert: result.bumped == true
    // Assert: result.epoch == 6 (current_epoch + 1)
    // Assert: myself.config_epoch == 6
    // Assert: current_epoch == 6
}
```

**Test: bumpEpoch increments when behind**
```zig
test "bumpEpoch increments when config_epoch < current_epoch" {
    // Setup: myself.config_epoch = 3, current_epoch = 8
    // Action: Call bumpEpoch()
    // Assert: result.bumped == true
    // Assert: result.epoch == 9
    // Assert: myself.config_epoch == 9
    // Assert: current_epoch == 9
}
```

**Test: bumpEpoch no-op when already highest**
```zig
test "bumpEpoch returns STILL when already at highest" {
    // Setup: myself.config_epoch = 10, current_epoch = 10
    // Action: Call bumpEpoch()
    // Assert: result.bumped == false
    // Assert: result.epoch == 10
    // Assert: No state changes
}
```

**Test: bumpEpoch when ahead of current_epoch**
```zig
test "bumpEpoch returns STILL when config_epoch > current_epoch" {
    // Setup: myself.config_epoch = 12, current_epoch = 10
    // Action: Call bumpEpoch()
    // Assert: result.bumped == false
    // Assert: result.epoch == 12
    // Assert: No state changes
}
```

### Command Tests (src/commands/cluster.zig)

**Test: CLUSTER SAVECONFIG returns OK**
```zig
test "CLUSTER SAVECONFIG returns OK on success" {
    // Setup: Initialize cluster state
    // Action: Call cmdClusterSaveConfig()
    // Assert: Response == "+OK\r\n"
    // Assert: nodes.conf file was created
}
```

**Test: CLUSTER SAVECONFIG validates arity**
```zig
test "CLUSTER SAVECONFIG rejects extra arguments" {
    // Action: Call with ["SAVECONFIG", "extra"]
    // Assert: Returns error (wrong number of arguments)
}
```

**Test: CLUSTER SAVECONFIG requires cluster enabled**
```zig
test "CLUSTER SAVECONFIG fails when cluster disabled" {
    // Setup: storage.cluster_state.enabled = false
    // Action: Call cmdClusterSaveConfig()
    // Assert: Returns error "-ERR This instance has cluster support disabled\r\n"
}
```

**Test: CLUSTER BUMPEPOCH returns BUMPED**
```zig
test "CLUSTER BUMPEPOCH returns BUMPED when incremented" {
    // Setup: config_epoch = 0, current_epoch = 5
    // Action: Call cmdClusterBumpEpoch()
    // Assert: Response == "$BUMPED 6\r\n"
}
```

**Test: CLUSTER BUMPEPOCH returns STILL**
```zig
test "CLUSTER BUMPEPOCH returns STILL when no increment" {
    // Setup: config_epoch = 10, current_epoch = 10
    // Action: Call cmdClusterBumpEpoch()
    // Assert: Response == "$STILL 10\r\n"
}
```

**Test: CLUSTER BUMPEPOCH validates arity**
```zig
test "CLUSTER BUMPEPOCH rejects extra arguments" {
    // Action: Call with ["BUMPEPOCH", "extra"]
    // Assert: Returns error (wrong number of arguments)
}
```

**Test: CLUSTER BUMPEPOCH auto-saves config**
```zig
test "CLUSTER BUMPEPOCH persists new epoch to disk" {
    // Setup: config_epoch = 0, current_epoch = 5
    // Action: Call cmdClusterBumpEpoch()
    // Assert: nodes.conf file is updated with new epoch
    // Assert: vars line has "currentEpoch 6"
}
```

### Integration Tests (tests/test_integration.zig)

**Test: Full SAVECONFIG cycle**
```redis
CLUSTER ADDSLOTS 0 1 2 3 4
CLUSTER SAVECONFIG
# Restart server
CLUSTER SLOTS
# Assert: Slots 0-4 are still assigned after restart
```

**Test: BUMPEPOCH increments epoch**
```redis
CLUSTER INFO  # Note initial config_epoch
CLUSTER BUMPEPOCH  # Should return BUMPED
CLUSTER INFO  # Assert config_epoch increased by 1
```

**Test: BUMPEPOCH idempotence**
```redis
CLUSTER BUMPEPOCH  # BUMPED <N>
CLUSTER BUMPEPOCH  # STILL <N> (no further increment)
```

**Test: Config persistence after BUMPEPOCH**
```redis
CLUSTER BUMPEPOCH
# Kill server (SIGKILL)
# Restart
CLUSTER INFO  # Assert config_epoch matches pre-crash value
```

---

## 6. Error Handling

### CLUSTER SAVECONFIG Errors

| Error | RESP Format | Trigger |
|-------|------------|---------|
| Cluster disabled | `-ERR This instance has cluster support disabled\r\n` | `cluster_state.enabled == false` |
| Wrong arity | `-ERR wrong number of arguments for 'cluster\|saveconfig' command\r\n` | `args.len != 1` |
| Disk full | `-ERR Could not save config: No space left on device\r\n` | File write fails with ENOSPC |
| Permission denied | `-ERR Could not save config: Permission denied\r\n` | File write fails with EACCES |
| I/O error | `-ERR Could not save config: <error>\r\n` | Generic file I/O error |

### CLUSTER BUMPEPOCH Errors

| Error | RESP Format | Trigger |
|-------|------------|---------|
| Cluster disabled | `-ERR This instance has cluster support disabled\r\n` | `cluster_state.enabled == false` |
| Wrong arity | `-ERR wrong number of arguments for 'cluster\|bumpepoch' command\r\n` | `args.len != 1` |
| Not initialized | `-ERR Cluster not initialized\r\n` | `myself == null` |

---

## 7. Redis Compatibility Notes

### Differences from Redis

1. **Config file path**:
   - Redis: Configurable via `cluster-config-file` option (default: `nodes-{port}.conf`)
   - Zoltraak: Hardcoded to `./nodes.conf` in initial implementation
   - Future: Add `cluster-config-file` config option

2. **Epoch conflict resolution**:
   - Redis: Automatic via lexicographic node ID comparison (if two nodes have same epoch)
   - Zoltraak: Not yet implemented (deferred to Phase 8.12)

3. **Auto-persistence triggers**:
   - Redis: Auto-saves after most CLUSTER commands (ADDSLOTS, SETSLOT, etc.)
   - Zoltraak: Currently no auto-save (manual via SAVECONFIG only)
   - Future: Add auto-save hooks to ADDSLOTS, SETSLOT, REPLICATE, etc.

4. **lastVoteEpoch**:
   - Redis: Persisted in vars line for failover vote tracking
   - Zoltraak: Not yet tracked (deferred until full election implementation)

### Compatibility Target

**Goal**: Byte-for-byte compatible RESP responses for SAVECONFIG and BUMPEPOCH commands.

**Validation**:
- Compare `redis-cli CLUSTER SAVECONFIG` vs `zoltraak-cli CLUSTER SAVECONFIG`
- Compare `redis-cli CLUSTER BUMPEPOCH` vs `zoltraak-cli CLUSTER BUMPEPOCH`
- Verify nodes.conf format matches Redis exactly (field order, spacing)

---

## 8. Implementation Checklist

### Phase 2: Tests First + Implementation

**unit-test-writer**:
- [ ] 7 storage layer tests (saveConfig × 3, bumpEpoch × 4)
- [ ] 7 command layer tests (SAVECONFIG × 3, BUMPEPOCH × 4)
- [ ] 4 integration tests (full cycle, persistence, idempotence, crash recovery)

**zig-implementor**:
- [ ] Implement `ClusterState.saveConfig()` with fsync
- [ ] Implement `ClusterState.bumpEpoch()` with validation logic
- [ ] Implement `cmdClusterSaveConfig()` with error handling
- [ ] Implement `cmdClusterBumpEpoch()` with BUMPED/STILL formatting
- [ ] Register both commands in CLUSTER dispatcher
- [ ] Add to ACL write_commands and skip_redirect_cmds lists

### Phase 3: Code Quality Review

**zig-quality-reviewer**:
- [ ] Verify fsync is called before returning from saveConfig()
- [ ] Check file cleanup on error (delete partial writes)
- [ ] Verify memory safety (no leaks in file operations)
- [ ] Check error propagation (file I/O errors handled correctly)

**code-reviewer**:
- [ ] Verify nodes.conf format matches Redis exactly
- [ ] Check epoch increment logic matches Redis semantics
- [ ] Review auto-save trigger (after BUMPEPOCH)
- [ ] Verify RESP response formats (BUMPED/STILL)

### Phase 4: Integration Testing

**integration-test-orchestrator**:
- [ ] Test SAVECONFIG writes valid nodes.conf
- [ ] Test BUMPEPOCH increments correctly
- [ ] Test config persistence across server restart
- [ ] Test error handling (disk full, permission denied)

### Phase 5: Validation

**redis-compatibility-validator**:
- [ ] Byte-by-byte RESP comparison vs Redis 7.x
- [ ] Verify nodes.conf format matches Redis (field order, slot compression)
- [ ] Test epoch semantics (BUMPED vs STILL conditions)

**performance-validator**:
- [ ] Benchmark SAVECONFIG latency (should be <10ms for small clusters)
- [ ] Verify fsync overhead is acceptable

---

## 9. Success Criteria

**Functional**:
- [ ] CLUSTER SAVECONFIG writes nodes.conf to disk
- [ ] nodes.conf format matches Redis exactly (parseable by Redis)
- [ ] CLUSTER BUMPEPOCH increments epoch correctly
- [ ] BUMPEPOCH returns BUMPED or STILL based on conditions
- [ ] Config persists across server restart

**Quality**:
- [ ] All 18 tests pass (7 storage + 7 command + 4 integration)
- [ ] Zero memory leaks detected by `zig build test`
- [ ] No critical or important issues from code reviews

**Compatibility**:
- [ ] RESP responses match Redis byte-for-byte
- [ ] nodes.conf file format compatible with Redis (can start Redis with zoltraak's nodes.conf)

**Documentation**:
- [ ] Update `docs/milestones.md`: Phase 8 Cluster 65% → 70%
- [ ] Update `CLAUDE.md`: Add SAVECONFIG/BUMPEPOCH to completed commands

---

## 10. Future Work (Phase 8.12+)

**Auto-persistence hooks**:
- Add `saveConfig()` calls after ADDSLOTS, DELSLOTS, SETSLOT, REPLICATE
- Configurable auto-save interval (e.g., every 300s + on change)

**Config file management**:
- Add `cluster-config-file` config option for custom path
- Support `nodes-{port}.conf` naming convention

**Epoch conflict resolution**:
- Implement automatic conflict detection and resolution
- Use lexicographic node ID comparison when two nodes have same epoch

**Failover integration**:
- Persist `lastVoteEpoch` in nodes.conf
- Update `current_epoch` during election process

**Config reload**:
- Implement `CLUSTER RESET` command to reload from nodes.conf
- Support manual edit of nodes.conf and hot reload

---

## 11. References

**Redis Documentation**:
- CLUSTER SAVECONFIG: https://redis.io/commands/cluster-saveconfig/
- CLUSTER BUMPEPOCH: https://redis.io/commands/cluster-bumpepoch/
- Redis Cluster Specification: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/

**Related Commands**:
- CLUSTER INFO (Iteration 136)
- CLUSTER NODES (Iteration 136)
- CLUSTER SLOTS (Iteration 136)
- CLUSTER ADDSLOTS (Iteration 137)
- CLUSTER SETSLOT (Iteration 138)

**Implementation Files**:
- Storage: `/Users/fn/codespace/zoltraak/src/storage/cluster.zig`
- Commands: `/Users/fn/codespace/zoltraak/src/commands/cluster.zig`
- Server: `/Users/fn/codespace/zoltraak/src/server.zig`
- Tests: `/Users/fn/codespace/zoltraak/tests/test_integration.zig`

---

## 12. Implementation Notes

### nodes.conf Format Details

**Slot range compression**:
```
Uncompressed: 0 1 2 3 5 6 7 10
Compressed:   0-3 5-7 10
```

**Algorithm**:
1. Sort slots
2. Group consecutive slots into ranges
3. Format as `<start>-<end>` for ranges, single number for isolated slots

**Field separators**: Single space between fields

**Node flags**: Comma-separated, no spaces (e.g., `myself,master` not `myself, master`)

### fsync Semantics

**Purpose**: Guarantee durability before returning OK

**Implementation**:
```zig
const file = try std.fs.cwd().createFile(config_path, .{});
defer file.close();

// Write all content
try file.writeAll(content);

// Force flush to disk
try file.sync(); // Calls fsync(2) on Unix, FlushFileBuffers on Windows
```

**Error handling**: If fsync fails, delete partial file and return error

### Epoch Increment Logic

**Pseudocode**:
```
if (my_config_epoch == 0 OR my_config_epoch < current_epoch) {
    new_epoch = current_epoch + 1;
    my_config_epoch = new_epoch;
    current_epoch = new_epoch;
    saveConfig(); // Persist immediately
    return BUMPED(new_epoch);
} else {
    return STILL(my_config_epoch);
}
```

**Thread safety**: Not required (single-threaded server model)

---

## Summary

**Iteration 145** implements cluster configuration management via SAVECONFIG and BUMPEPOCH commands, completing Phase 8.11 and bringing cluster support to **70% completion**.

**Key deliverables**:
1. `ClusterState.saveConfig()` — Serialize cluster state to nodes.conf with fsync
2. `ClusterState.bumpEpoch()` — Increment configuration epoch with validation
3. `CLUSTER SAVECONFIG` command — Force config persistence
4. `CLUSTER BUMPEPOCH` command — Manual epoch advancement
5. Full test coverage (18 tests)
6. Redis-compatible nodes.conf format

**Next iteration** (Phase 8.12): CLUSTER COUNTKEYSINSLOT, CLUSTER GETKEYSINSLOT, CLUSTER KEYSLOT — slot key introspection commands.
