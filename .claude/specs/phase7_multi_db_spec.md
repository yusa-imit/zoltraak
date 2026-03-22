# Phase 7: Multi-Database Support — Redis Specification

**Compiled:** 2026-03-22
**Target:** Zoltraak v1.0 Phase 7
**Redis Compatibility Target:** Redis 7.x+

---

## Table of Contents

1. [Overview](#overview)
2. [Database Configuration](#database-configuration)
3. [SELECT Command](#select-command)
4. [SWAPDB Command](#swapdb-command)
5. [MOVE Command](#move-command)
6. [COPY Command with DB Option](#copy-command-with-db-option)
7. [FLUSHDB Behavior](#flushdb-behavior)
8. [DBSIZE Behavior](#dbsize-behavior)
9. [INFO Keyspace Format](#info-keyspace-format)
10. [RESET Command Database Behavior](#reset-command-database-behavior)
11. [Per-Connection State Management](#per-connection-state-management)
12. [Error Messages](#error-messages)
13. [Cluster Mode Restrictions](#cluster-mode-restrictions)
14. [Implementation Considerations](#implementation-considerations)
15. [Sources](#sources)

---

## Overview

Redis supports multiple logical databases within a single instance, numbered from 0 to a configurable maximum (default 16, max 16384). Each database is a completely separate keyspace with independent keys, but they share:
- The same server process and memory
- The same configuration parameters
- The same RDB/AOF persistence files (all databases persisted together)
- The same replication stream

**Key principle:** Databases are a form of namespacing, not isolation. They are NOT separate Redis instances.

**Important limitation:** Redis Cluster does NOT support multiple databases — only database 0 is available in cluster mode.

---

## Database Configuration

### Configuration Parameter: `databases`

**File:** `redis.conf`

```conf
databases 16
```

**Default value:** 16
**Minimum value:** 1
**Maximum value:** 16384
**Modifiable at runtime:** NO (requires server restart)

### How to Configure

1. **redis.conf file:**
   ```conf
   databases 32
   ```

2. **Command line:**
   ```bash
   redis-server --databases 32
   ```

3. **Not modifiable via CONFIG SET** — this parameter is read-only after startup

### Database Numbering

- Databases are numbered from **0** to **N-1** where N is the `databases` config value
- Default: 0-15 (16 databases)
- All new client connections start on **database 0**

---

## SELECT Command

### Syntax

```
SELECT index
```

### Parameters

- **index** (integer): Zero-based database index (0 to `databases-1`)

### Return Value

**Simple string reply:** `+OK\r\n`

### Time Complexity

**O(1)** — constant time

### Behavior

- Switches the current connection to the specified database
- Database selection is a **per-connection property** — each client connection has its own selected database
- The selected database persists for the lifetime of the connection
- New connections always start at database 0
- Clients must **re-select** the database after reconnection

### Error Conditions

| Condition | Error Message | Return Value |
|-----------|---------------|--------------|
| Negative index | `ERR DB index is out of range` | Error reply |
| Index >= `databases` config | `ERR DB index is out of range` | Error reply |
| Invalid argument (not an integer) | `ERR invalid DB index` | Error reply |
| Wrong number of arguments | `ERR wrong number of arguments for 'select' command` | Error reply |
| Cluster mode (index != 0) | `ERR SELECT is not allowed in cluster mode` | Error reply |

### Examples

```redis
SELECT 0
# +OK

SELECT 5
# +OK

SELECT -1
# -ERR DB index is out of range

SELECT 999
# -ERR DB index is out of range

SELECT abc
# -ERR invalid DB index

SELECT
# -ERR wrong number of arguments for 'select' command
```

### Cluster Mode

**NOT SUPPORTED** — Redis Cluster only supports database 0.

Attempting `SELECT 1` or any non-zero index in cluster mode returns:
```
-ERR SELECT is not allowed in cluster mode
```

### Version Information

**Available since:** Redis 1.0.0

### Related Commands

- `FLUSHDB` — flush current database
- `DBSIZE` — count keys in current database
- `MOVE` — move key to another database
- `COPY ... DB` — copy key to another database
- `SWAPDB` — swap two databases

---

## SWAPDB Command

### Syntax

```
SWAPDB index1 index2
```

### Description

Atomically swaps two Redis databases. All clients connected to database `index1` immediately see the data from `index2`, and vice versa. The swap is instantaneous and atomic.

### Parameters

- **index1** (integer): First database index
- **index2** (integer): Second database index

### Return Value

**Simple string reply:** `+OK\r\n`

### Time Complexity

**O(N)** where N is the count of clients **watching or blocking** on keys from both databases.

- The swap itself is O(1) — just pointer exchange
- But all clients with `WATCH` or blocking commands on affected databases must be notified

### Atomicity Guarantees

- The swap is **fully atomic** — no commands execute during the swap
- Redis uses its single-threaded architecture to ensure atomicity
- All clients see the swap simultaneously
- No data is lost or duplicated during the swap

### Behavior

1. Database `index1` and `index2` are swapped
2. All keys in `index1` are now accessible as `index2` and vice versa
3. Clients do NOT change their selected database — they just see different data
4. Example: If a client was connected to DB 0, after `SWAPDB 0 1`, that client still thinks it's on DB 0, but sees the old DB 1 data

### Error Conditions

| Condition | Error Message | Return Value |
|-----------|---------------|--------------|
| Negative index | `ERR DB index is out of range` | Error reply |
| Index >= `databases` config | `ERR DB index is out of range` | Error reply |
| Invalid first index | `ERR invalid first DB index` | Error reply |
| Invalid second index | `ERR invalid second DB index` | Error reply |
| Wrong number of arguments | `ERR wrong number of arguments for 'swapdb' command` | Error reply |

### Special Cases

**Swapping a database with itself** (e.g., `SWAPDB 0 0`) is valid and returns `+OK` (no-op).

### Examples

```redis
# Start with DB 0 selected
SET key1 "value_in_db0"

SELECT 1
SET key2 "value_in_db1"

# Swap the databases
SWAPDB 0 1
# +OK

# Now clients on DB 0 see key2, clients on DB 1 see key1
SELECT 0
GET key2
# "value_in_db1"

SELECT 1
GET key1
# "value_in_db0"
```

### Version Introduced

**Redis 4.0.0**

### ACL Categories

`@keyspace`, `@write`, `@fast`, `@dangerous`

### Command Flags

- `write`
- `fast`

### Cluster Mode

**NOT SUPPORTED** — not available in Redis Cloud or Redis Software (cluster deployments).

### Use Cases

- **Database snapshots:** Prepare a database in the background, then atomically swap
- **Zero-downtime updates:** Build new data in DB 1, swap with DB 0 when ready
- **Testing:** Swap production data with test data temporarily

---

## MOVE Command

### Syntax

```
MOVE key db
```

### Description

Moves a key from the **currently selected database** to the specified destination database. The operation is atomic within the same Redis instance.

### Parameters

- **key** (string): The key to move
- **db** (integer): Destination database index

### Return Values

| Scenario | Return Value | Meaning |
|----------|--------------|---------|
| Key successfully moved | `:1\r\n` | Integer reply: 1 |
| Key NOT moved | `:0\r\n` | Integer reply: 0 |

### When MOVE Returns 0

The command does **nothing** and returns `0` when:
1. The key already exists in the destination database
2. The key does not exist in the source database (currently selected DB)

### Behavior

1. **Atomic operation** — the key is removed from the source and added to the destination in one atomic step
2. **TTL is preserved** — if the source key has an expiration, it is carried over to the destination
3. **No error on failure** — unlike many Redis commands, `MOVE` returns `0` instead of raising an error when it cannot move the key
4. **Idempotent** — safe to retry

### Error Conditions

| Condition | Error Message | Return Value |
|-----------|---------------|--------------|
| Wrong number of arguments | `ERR wrong number of arguments for 'move' command` | Error reply |
| Invalid DB index (not integer) | `ERR invalid DB index` | Error reply |
| Negative DB index | `ERR DB index is out of range` | Error reply |
| DB index >= `databases` config | `ERR DB index is out of range` | Error reply |

### Locking Primitive

Because `MOVE` is idempotent and returns `0` if the key exists in the destination, it can be used as a **distributed locking primitive**:

```redis
# Try to acquire lock in DB 1
SELECT 0
SET lockkey "mylock"
MOVE lockkey 1
# :1 if lock acquired
# :0 if lock already held
```

### Examples

```redis
# Set up key in DB 0
SELECT 0
SET mykey "value1"

# Move to DB 1
MOVE mykey 1
# :1 (success)

# Key no longer exists in DB 0
GET mykey
# (nil)

# Switch to DB 1
SELECT 1
GET mykey
# "value1"

# Try to move again (key no longer in source)
SELECT 0
MOVE mykey 1
# :0 (key doesn't exist in source)

# Try to move when key exists in destination
SELECT 0
SET mykey "value2"
MOVE mykey 1
# :0 (key already exists in destination)
```

### Time Complexity

**O(1)**

### ACL Categories

`@keyspace`, `@write`, `@fast`

### Command Flags

- `write`
- `fast`

### Version Information

**Available since:** Redis 1.0.0

### Limitations

- **Single instance only** — cannot move keys between different Redis instances (use `MIGRATE` for that)
- **Not available in Redis Software/Cloud** — blocked due to performance concerns in clustered deployments
- **Not available in cluster mode** — cluster mode only supports database 0

---

## COPY Command with DB Option

### Syntax

```
COPY source destination [DB destination-db] [REPLACE]
```

### Description

Copies the value stored at the `source` key to the `destination` key. Supports cross-database copy with the `DB` option.

### Parameters

- **source** (key): Source key to copy from
- **destination** (key): Destination key to copy to
- **DB destination-db** (optional, integer): Destination database index (if omitted, uses current database)
- **REPLACE** (optional, flag): Remove destination key before copying if it exists

### Return Values

| Scenario | Return Value | Meaning |
|----------|--------------|---------|
| Source successfully copied | `:1\r\n` | Integer reply: 1 |
| Source NOT copied | `:0\r\n` | Integer reply: 0 |

### When COPY Returns 0

Returns `0` without copying when:
- Destination key already exists (and `REPLACE` not specified)
- Source key does not exist

### Behavior

1. **Default database:** Without `DB` option, copies within the currently selected database
2. **Cross-database copy:** With `DB` option, copies to a different database
3. **TTL is copied:** Expiration time is preserved in the destination
4. **REPLACE semantics:** `REPLACE` removes the destination key before copying, allowing overwrite
5. **Non-destructive by default:** Without `REPLACE`, existing destination keys are NOT overwritten

### Error Conditions

| Condition | Error Message | Return Value |
|-----------|---------------|--------------|
| Too few arguments | `ERR wrong number of arguments for 'copy' command` | Error reply |
| Syntax error | `ERR syntax error` | Error reply |
| Invalid DB index | `ERR invalid DB index` | Error reply |

### Examples

```redis
# Basic copy within same database
SET dolly "sheep"
COPY dolly clone
# :1
GET clone
# "sheep"

# Cross-database copy
SELECT 0
SET mykey "value_in_db0"
COPY mykey newkey DB 1
# :1

SELECT 1
GET newkey
# "value_in_db0"

# Copy with REPLACE
SELECT 0
SET source "original"
SET dest "old_value"
COPY source dest
# :0 (dest already exists)

COPY source dest REPLACE
# :1 (dest replaced)
GET dest
# "original"
```

### Time Complexity

- **O(N)** worst case for collections (where N is the number of nested items)
- **O(1)** for string values

### Version Introduced

**Redis 6.2.0**

### ACL Categories

`@keyspace`, `@write`, `@slow`

### Cluster Mode Restriction

In **clustered environments** (Active-Active or Redis Cluster), the source and destination keys must be in the **same hash slot**.

---

## FLUSHDB Behavior

### Syntax

```
FLUSHDB [ASYNC | SYNC]
```

### Multi-Database Behavior

**FLUSHDB deletes all keys in the CURRENTLY SELECTED database only.**

- Does NOT affect other databases
- Only the database selected via `SELECT` is flushed
- Other databases remain intact

### Examples

```redis
# Set up data in multiple databases
SELECT 0
SET key1 "db0_value"
SELECT 1
SET key2 "db1_value"

# Flush DB 1
SELECT 1
FLUSHDB
# +OK

# DB 1 is empty
DBSIZE
# :0

# DB 0 is unaffected
SELECT 0
DBSIZE
# :1
GET key1
# "db0_value"
```

### Async vs Sync

- **SYNC (default):** Blocks until all keys are deleted
- **ASYNC:** Returns immediately, deletes in background thread (Redis 6.2+)
- **Configuration:** `lazyfree-lazy-user-flush yes` makes ASYNC the default

### Return Value

**Simple string reply:** `+OK\r\n`

### Time Complexity

- **O(N)** where N is the number of keys in the selected database
- **O(1)** with ASYNC (returns immediately)

### Version Information

**Available since:** Redis 1.0.0
**ASYNC option:** Redis 4.0.0

### Related Commands

- `FLUSHALL` — flush ALL databases (not just current)
- `DBSIZE` — count keys in current database

---

## DBSIZE Behavior

### Syntax

```
DBSIZE
```

### Multi-Database Behavior

**DBSIZE returns the number of keys in the CURRENTLY SELECTED database only.**

- Does NOT count keys in other databases
- Only counts keys in the database selected via `SELECT`

### Return Value

**Integer reply:** Number of keys in the current database

### Time Complexity

**O(1)** — Redis maintains a counter per database

### Examples

```redis
# Set up data in multiple databases
SELECT 0
SET key1 "value1"
SET key2 "value2"

SELECT 1
SET key3 "value3"

# DB 0 has 2 keys
SELECT 0
DBSIZE
# :2

# DB 1 has 1 key
SELECT 1
DBSIZE
# :1
```

### Version Information

**Available since:** Redis 1.0.0

---

## INFO Keyspace Format

### Syntax

```
INFO keyspace
```

### Output Format

The `keyspace` section of the `INFO` command displays per-database statistics in the following format:

```
# Keyspace
db0:keys=<total_keys>,expires=<keys_with_ttl>,avg_ttl=<average_ttl_ms>
db1:keys=<total_keys>,expires=<keys_with_ttl>,avg_ttl=<average_ttl_ms>
db2:keys=<total_keys>,expires=<keys_with_ttl>,avg_ttl=<average_ttl_ms>
...
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `dbN` | identifier | Database number (e.g., `db0`, `db1`, ..., `db15`) |
| `keys` | integer | Total number of keys in that database |
| `expires` | integer | Number of keys with an expiration set (TTL > -1) |
| `avg_ttl` | integer | Average time-to-live in **milliseconds** for keys with expiration |
| `subexpiry` | integer | Number of keys with field-level expiration (hashes, Redis 7.4+) |

### Behavior

- **Only non-empty databases are shown** — databases with 0 keys are omitted
- **avg_ttl is 0** if no keys have expiration
- **subexpiry field** is added in Redis 7.4+ for hash field expiration support

### Example Output

```
# Keyspace
db0:keys=3,expires=0,avg_ttl=0
db1:keys=5,expires=2,avg_ttl=1234567
db2:keys=10,expires=7,avg_ttl=5000000
```

Interpretation:
- DB 0: 3 keys, none have TTL
- DB 1: 5 keys, 2 have TTL, average TTL is ~20 minutes
- DB 2: 10 keys, 7 have TTL, average TTL is ~83 minutes

### Parsing Notes

- Format is stable and suitable for programmatic parsing
- Fields are comma-separated
- Database lines appear in numerical order
- Empty databases are NOT listed

---

## RESET Command Database Behavior

### Syntax

```
RESET
```

### Return Value

**Simple string reply:** `RESET` (not `OK` — special case)

### Database Selection Behavior

**RESET selects database 0** as part of its connection state reset.

### Complete State Reset

When called, `RESET` performs the following actions:

1. Discards the current `MULTI` transaction block
2. Unwatches all keys watched via `WATCH`
3. Disables `CLIENT TRACKING`
4. Sets connection to `READWRITE` mode
5. Cancels `ASKING` mode (cluster)
6. Sets `CLIENT REPLY` to `ON`
7. Sets protocol version to **RESP2**
8. **Selects database 0** ← Multi-DB behavior
9. Exits `MONITOR` mode
10. Aborts Pub/Sub subscription state (`SUBSCRIBE`, `PSUBSCRIBE`)
11. Deauthenticates the connection (requires `AUTH` to reauthenticate)
12. Turns off `NO-EVICT` mode
13. Turns off `NO-TOUCH` mode

### Use Case

**Connection pooling** — ensures a client connection is in a clean, predictable state before reuse.

### Examples

```redis
# Client selects DB 5
SELECT 5
# +OK

# Reset connection
RESET
# +RESET

# Connection is now on DB 0
INFO keyspace
# Shows DB 0 is selected
```

### Time Complexity

**O(1)**

### Version Introduced

**Redis 6.2.0**

### Availability

**NOT available** in Redis Software or Redis Cloud (Standard/Active-Active deployments)

---

## Per-Connection State Management

### Selected Database Tracking

Each client connection maintains its own selected database:

```zig
// Pseudo-code for per-connection state
pub const ClientState = struct {
    client_id: u64,
    selected_db: u32 = 0,  // Default to DB 0
    protocol_version: u8 = 2,  // RESP2 default
    // ... other state
};
```

### State Lifecycle

1. **Connection established:** Client starts on DB 0
2. **SELECT command:** Client switches to specified DB
3. **RESET command:** Client returns to DB 0
4. **Connection closed:** State is discarded
5. **Reconnection:** New connection starts at DB 0 (state NOT preserved)

### Persistence Across Reconnection

**Database selection is NOT persistent.** Clients must re-select after reconnection:

```python
# Python client example
r = redis.Redis()
r.select(5)       # Select DB 5
r.set('key', 'value')

# Connection lost and re-established
r.get('key')      # Will fail — client is back on DB 0

# Must re-select
r.select(5)
r.get('key')      # Works
```

### CLIENT LIST Output

The `CLIENT LIST` command shows the selected database for each client:

```
id=1 addr=127.0.0.1:12345 fd=8 db=0 ...
id=2 addr=127.0.0.1:54321 fd=9 db=5 ...
```

The `db=N` field shows the currently selected database.

---

## Error Messages

### Exact Error Strings

| Error Scenario | Exact Error Message |
|----------------|---------------------|
| SELECT: Negative index | `ERR DB index is out of range` |
| SELECT: Index too high | `ERR DB index is out of range` |
| SELECT: Invalid argument | `ERR invalid DB index` |
| SELECT: Wrong arg count | `ERR wrong number of arguments for 'select' command` |
| SELECT: Cluster mode (non-zero) | `ERR SELECT is not allowed in cluster mode` |
| SWAPDB: Negative index | `ERR DB index is out of range` |
| SWAPDB: Index too high | `ERR DB index is out of range` |
| SWAPDB: Invalid first index | `ERR invalid first DB index` |
| SWAPDB: Invalid second index | `ERR invalid second DB index` |
| SWAPDB: Wrong arg count | `ERR wrong number of arguments for 'swapdb' command` |
| MOVE: Invalid DB index | `ERR invalid DB index` |
| MOVE: Negative index | `ERR DB index is out of range` |
| MOVE: Index too high | `ERR DB index is out of range` |
| MOVE: Wrong arg count | `ERR wrong number of arguments for 'move' command` |
| COPY: Wrong arg count | `ERR wrong number of arguments for 'copy' command` |
| COPY: Syntax error | `ERR syntax error` |

### Error Format

All errors are returned as RESP error replies:
```
-ERR <message>\r\n
```

---

## Cluster Mode Restrictions

### Redis Cluster Limitations

**Redis Cluster only supports database 0.** Multiple databases are disabled in cluster mode.

| Command | Cluster Behavior |
|---------|------------------|
| `SELECT 0` | Allowed (no-op) |
| `SELECT N` (N > 0) | `-ERR SELECT is not allowed in cluster mode` |
| `SWAPDB` | Not available |
| `MOVE` | Not available |
| `COPY ... DB` | Destination must be in same hash slot |

### Rationale

Redis Cluster uses **hash slots** (16384 slots) to distribute keys across nodes. Multiple databases would add unnecessary complexity to the slot-to-node mapping and replication protocol.

### Redis Software / Redis Cloud

- **Redis Software (Standard/Active-Active):** Multiple databases are blocked for performance reasons
- **Redis Cloud (Standard/Active-Active):** Not supported

---

## Implementation Considerations

### Storage Architecture

```zig
// Multi-database storage structure
pub const Storage = struct {
    databases: []Database,       // Array of databases
    num_databases: u32,           // From config
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
};

pub const Database = struct {
    id: u32,
    data: std.StringHashMap(Value),
    expires: std.StringHashMap(i64),
    key_count: std.atomic.Atomic(u64),
};
```

### Per-Connection State

```zig
pub const ClientState = struct {
    client_id: u64,
    selected_db: u32,              // Current database index
    protocol_version: u8,          // RESP2 or RESP3
    auth_user: ?[]const u8,        // ACL user
    is_monitoring: bool,           // MONITOR mode
    is_blocked: bool,              // Blocking command active
    pubsub_channels: std.StringHashMap(void),
};
```

### SWAPDB Implementation

```zig
pub fn swapdb(self: *Storage, index1: u32, index2: u32) !void {
    if (index1 >= self.num_databases or index2 >= self.num_databases) {
        return error.OutOfRange;
    }

    self.mutex.lock();
    defer self.mutex.unlock();

    // Atomic pointer swap
    const temp = self.databases[index1];
    self.databases[index1] = self.databases[index2];
    self.databases[index2] = temp;

    // Notify clients with WATCH/blocking on these databases
    try self.notifyDatabaseSwap(index1, index2);
}
```

### MOVE Implementation

```zig
pub fn moveKey(
    self: *Storage,
    source_db: u32,
    dest_db: u32,
    key: []const u8
) !bool {
    if (source_db >= self.num_databases or dest_db >= self.num_databases) {
        return error.OutOfRange;
    }

    self.mutex.lock();
    defer self.mutex.unlock();

    // Check if key exists in source
    const source = &self.databases[source_db];
    const value = source.data.get(key) orelse return false;

    // Check if key exists in destination
    const dest = &self.databases[dest_db];
    if (dest.data.contains(key)) {
        return false;  // Destination key exists
    }

    // Move (copy + delete)
    try dest.data.put(key, value);
    if (source.expires.get(key)) |ttl| {
        try dest.expires.put(key, ttl);
    }
    _ = source.data.remove(key);
    _ = source.expires.remove(key);

    return true;
}
```

### INFO Keyspace Generation

```zig
pub fn generateKeyspaceInfo(self: *Storage, writer: anytype) !void {
    try writer.writeAll("# Keyspace\r\n");

    for (self.databases, 0..) |db, idx| {
        const key_count = db.key_count.load(.monotonic);
        if (key_count == 0) continue;  // Skip empty databases

        var expires_count: u64 = 0;
        var total_ttl: i64 = 0;

        // Calculate expiration stats
        var it = db.expires.iterator();
        while (it.next()) |entry| {
            expires_count += 1;
            total_ttl += entry.value_ptr.*;
        }

        const avg_ttl = if (expires_count > 0)
            @divTrunc(total_ttl, @as(i64, @intCast(expires_count)))
        else
            0;

        try writer.print("db{d}:keys={d},expires={d},avg_ttl={d}\r\n",
            .{ idx, key_count, expires_count, avg_ttl });
    }
}
```

### Configuration Loading

```zig
pub fn loadConfig(config_path: []const u8) !Config {
    // Parse redis.conf
    const databases = try parseConfigInt(config_path, "databases", 16);

    if (databases < 1 or databases > 16384) {
        return error.InvalidDatabaseCount;
    }

    return Config{
        .databases = databases,
        // ... other config
    };
}
```

---

## Sources

This specification was compiled from official Redis documentation and search results:

### Official Redis Documentation
- [SELECT | Docs](https://redis.io/docs/latest/commands/select/)
- [SWAPDB | Docs](https://redis.io/docs/latest/commands/swapdb/)
- [MOVE | Docs](https://redis.io/docs/latest/commands/move/)
- [COPY | Docs](https://redis.io/docs/latest/commands/copy/)
- [RESET | Docs](https://redis.io/docs/latest/commands/reset/)
- [DBSIZE | Docs](https://redis.io/docs/latest/commands/dbsize/)
- [FLUSHDB | Docs](https://redis.io/docs/latest/commands/flushdb/)
- [INFO | Docs](https://redis.io/docs/latest/commands/info/)

### Configuration and Best Practices
- [Redis configuration | Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/config/)
- [How to Use Multiple Redis Databases Effectively](https://oneuptime.com/blog/post/2026-01-25-redis-multiple-databases/view)

### Additional References
- [Redis FLUSHDB: Quick Guide and Other Ways to Clear Redis Cache](https://www.dragonflydb.io/guides/redis-flushdb-quick-guide)
- [Redis Connection: SELECT index - w3resource](https://www.w3resource.com/redis/redis-select-index.php)
- [Redis COPY Command Explained](https://database.guide/redis-copy-command-explained/)

---

**End of Specification**
