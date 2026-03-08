# Zoltraak

A high-performance, Redis-compatible in-memory data store written in Zig.

## Overview

Zoltraak is a modern alternative to Redis, built from the ground up using the Zig programming language. It aims to provide Redis compatibility while leveraging Zig's performance characteristics, memory safety, and simplicity.

## Goals

- **Redis Protocol Compatibility**: Full RESP (Redis Serialization Protocol) support for drop-in replacement
- **High Performance**: Leverage Zig's zero-overhead abstractions and manual memory control
- **Memory Efficiency**: Predictable memory usage with no garbage collection pauses
- **Simplicity**: Clean, auditable codebase with minimal dependencies
- **Cross-Platform**: Native support for Linux, macOS, and Windows

## Planned Features

### Data Structures
- Strings
- Lists
- Sets
- Hashes
- Sorted Sets

### Core Functionality
- Key expiration (TTL)
- Pub/Sub messaging
- Transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
- Persistence (RDB snapshots, AOF logging)
- Replication

## Requirements

- Zig 0.15.0 or later

## Building

```bash
# Build the project
zig build

# Run all tests (unit + integration)
zig build test

# Run only integration tests
zig build test-integration

# Build in release mode
zig build -Doptimize=ReleaseFast
```

## Usage

```bash
# Start the server (default: 127.0.0.1:6379)
./zig-out/bin/zoltraak

# Start with custom host/port
./zig-out/bin/zoltraak --host 0.0.0.0 --port 6380

# Connect using redis-cli
redis-cli -p 6379

# Use zoltraak-cli for interactive REPL
./zig-out/bin/zoltraak-cli

# Launch TUI key browser
./zig-out/bin/zoltraak-cli --tui

# Launch advanced TUI with Tree/LineChart/Dialog/Notification widgets (sailor v0.5.0+)
./zig-out/bin/zoltraak-cli --tui --advanced
```

## Supported Commands

### String Commands (Iterations 1, 19, 45-47)

| Command | Syntax | Description |
|---------|--------|-------------|
| PING | `PING [message]` | Test connectivity |
| SET | `SET key value [EX s] [PX ms] [NX\|XX]` | Set key-value with optional TTL |
| GET | `GET key` | Get value by key |
| DEL | `DEL key [key ...]` | Delete one or more keys |
| EXISTS | `EXISTS key [key ...]` | Check if keys exist |
| INCR | `INCR key` | Increment integer value by 1 |
| DECR | `DECR key` | Decrement integer value by 1 |
| INCRBY | `INCRBY key increment` | Increment integer value by increment |
| DECRBY | `DECRBY key decrement` | Decrement integer value by decrement |
| INCRBYFLOAT | `INCRBYFLOAT key increment` | Increment float value by increment |
| APPEND | `APPEND key value` | Append value to string, returns new length |
| STRLEN | `STRLEN key` | Get length of string value |
| GETSET | `GETSET key value` | Set value and return old value |
| GETDEL | `GETDEL key` | Get value and delete key |
| GETEX | `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ts\|PERSIST]` | Get value and optionally update expiry |
| SETNX | `SETNX key value` | Set value only if key does not exist |
| SETEX | `SETEX key seconds value` | Set value with expiry in seconds |
| PSETEX | `PSETEX key milliseconds value` | Set value with expiry in milliseconds |
| MGET | `MGET key [key ...]` | Get values of multiple keys |
| MSET | `MSET key value [key value ...]` | Set multiple keys to multiple values |
| MSETNX | `MSETNX key value [key value ...]` | Set multiple keys only if none exist |
| MSETEX | `MSETEX numkeys key value [key value ...] [NX\|XX] [EX seconds\|PX ms\|EXAT ts\|PXAT ts\|KEEPTTL]` | Atomically set multiple keys with optional shared expiration (returns 1 if all set, 0 otherwise) |
| LCS | `LCS key1 key2 [LEN]` | Find the longest common subsequence between two strings (returns LCS string by default, length with LEN option) |

### List Commands (Iterations 2, 11, 18, 49)

| Command | Syntax | Description |
|---------|--------|-------------|
| LPUSH | `LPUSH key element [element ...]` | Push elements to list head |
| RPUSH | `RPUSH key element [element ...]` | Push elements to list tail |
| LPOP | `LPOP key [count]` | Pop elements from list head |
| RPOP | `RPOP key [count]` | Pop elements from list tail |
| LRANGE | `LRANGE key start stop` | Get range of elements |
| LLEN | `LLEN key` | Get list length |
| LINDEX | `LINDEX key index` | Get element at index |
| LSET | `LSET key index element` | Set element at index |
| LTRIM | `LTRIM key start stop` | Trim list to range |
| LREM | `LREM key count element` | Remove elements by value |
| LPUSHX | `LPUSHX key element [element ...]` | Push to head if key exists |
| RPUSHX | `RPUSHX key element [element ...]` | Push to tail if key exists |
| LINSERT | `LINSERT key BEFORE\|AFTER pivot element` | Insert before/after pivot |
| LPOS | `LPOS key element [RANK rank] [COUNT num] [MAXLEN len]` | Find positions of element |
| LMOVE | `LMOVE source dest LEFT\|RIGHT LEFT\|RIGHT` | Atomically move element |
| RPOPLPUSH | `RPOPLPUSH source dest` | Pop from tail, push to head (legacy) |
| LMPOP | `LMPOP numkeys key [key ...] LEFT\|RIGHT [COUNT count]` | Multi-pop from first non-empty list |
| BLPOP | `BLPOP key [key ...] timeout` | Blocking pop from head |
| BRPOP | `BRPOP key [key ...] timeout` | Blocking pop from tail |
| BLMOVE | `BLMOVE source dest LEFT\|RIGHT LEFT\|RIGHT timeout` | Blocking move element |
| BLMPOP | `BLMPOP timeout numkeys key [key ...] LEFT\|RIGHT [COUNT count]` | Blocking multi-pop from first non-empty list |

### Set Commands (Iterations 3 + 12)

| Command | Syntax | Description |
|---------|--------|-------------|
| SADD | `SADD key member [member ...]` | Add members to set |
| SREM | `SREM key member [member ...]` | Remove members from set |
| SISMEMBER | `SISMEMBER key member` | Check if member exists |
| SMISMEMBER | `SMISMEMBER key member [member ...]` | Bulk membership check |
| SMEMBERS | `SMEMBERS key` | Get all members |
| SCARD | `SCARD key` | Get set cardinality |
| SPOP | `SPOP key [count]` | Remove and return random member(s) |
| SRANDMEMBER | `SRANDMEMBER key [count]` | Return random member(s) without removing |
| SMOVE | `SMOVE source destination member` | Atomically move member between sets |
| SINTERCARD | `SINTERCARD numkeys key [key ...] [LIMIT limit]` | Intersection cardinality |
| SUNION | `SUNION key [key ...]` | Union of sets |
| SINTER | `SINTER key [key ...]` | Intersection of sets |
| SDIFF | `SDIFF key [key ...]` | Difference of sets |
| SUNIONSTORE | `SUNIONSTORE dest key [key ...]` | Store union result |
| SINTERSTORE | `SINTERSTORE dest key [key ...]` | Store intersection result |
| SDIFFSTORE | `SDIFFSTORE dest key [key ...]` | Store difference result |
| SSCAN | `SSCAN key cursor [MATCH pattern] [COUNT count]` | Iterate set members |

### Hash Commands (Iterations 4, 42, 48, 50, 52)

| Command | Syntax | Description |
|---------|--------|-------------|
| HSET | `HSET key field value [field value ...]` | Set field values in hash |
| HGET | `HGET key field` | Get value of field |
| HDEL | `HDEL key field [field ...]` | Delete fields from hash |
| HGETALL | `HGETALL key` | Get all fields and values |
| HKEYS | `HKEYS key` | Get all field names |
| HVALS | `HVALS key` | Get all values |
| HEXISTS | `HEXISTS key field` | Check if field exists |
| HLEN | `HLEN key` | Get number of fields |
| HSTRLEN | `HSTRLEN key field` | Get string length of hash field value (returns 0 if field doesn't exist) |
| HRANDFIELD | `HRANDFIELD key [count [WITHVALUES]]` | Return random field(s) from hash (single field without count, array with count, field-value pairs with WITHVALUES, RESP3 map with WITHVALUES) |
| HEXPIRE | `HEXPIRE key seconds FIELDS numfields field [field ...] [NX\|XX\|GT\|LT]` | Set expiration time for hash fields (Redis 7.4+) |
| HPEXPIRE | `HPEXPIRE key milliseconds FIELDS numfields field [field ...] [NX\|XX\|GT\|LT]` | Set expiration time for hash fields in milliseconds (Redis 7.4+) |
| HEXPIREAT | `HEXPIREAT key unix-time-seconds FIELDS numfields field [field ...] [NX\|XX\|GT\|LT]` | Set expiration time at absolute Unix timestamp in seconds (Redis 7.4+) |
| HPEXPIREAT | `HPEXPIREAT key unix-time-milliseconds FIELDS numfields field [field ...] [NX\|XX\|GT\|LT]` | Set expiration time at absolute Unix timestamp in milliseconds (Redis 7.4+) |
| HPERSIST | `HPERSIST key FIELDS numfields field [field ...]` | Remove expiration from hash fields (Redis 7.4+) |
| HTTL | `HTTL key FIELDS numfields field [field ...]` | Get TTL in seconds for hash fields (-2 if field doesn't exist, -1 if no expiry) (Redis 7.4+) |
| HPTTL | `HPTTL key FIELDS numfields field [field ...]` | Get TTL in milliseconds for hash fields (Redis 7.4+) |
| HEXPIRETIME | `HEXPIRETIME key FIELDS numfields field [field ...]` | Get expiration time in seconds (Unix timestamp) for hash fields (Redis 7.4+) |
| HPEXPIRETIME | `HPEXPIRETIME key FIELDS numfields field [field ...]` | Get expiration time in milliseconds (Unix timestamp) for hash fields (Redis 7.4+) |
| HGETDEL | `HGETDEL key FIELDS numfields field [field ...]` | Atomically get hash field values and delete the fields (returns array of values, auto-deletes hash when all fields removed) |
| HGETEX | `HGETEX key [EX\|PX\|EXAT\|PXAT\|PERSIST] FIELDS numfields field [field ...]` | Atomically get hash field values and set/update field expiration (returns array of values) |
| HSETEX | `HSETEX key [FNX\|FXX] [EX\|PX\|EXAT\|PXAT\|KEEPTTL] FIELDS numfields field value [field value ...]` | Atomically set hash fields with values and expiration (returns 1 if all set, 0 if conditional failed) |

### Replication Commands (Iteration 10)

| Command | Syntax | Description |
|---------|--------|-------------|
| REPLICAOF | `REPLICAOF host port` | Become a replica of the given primary |
| REPLICAOF NO ONE | `REPLICAOF NO ONE` | Stop replication and promote to primary |
| REPLCONF | `REPLCONF subcommand [args...]` | Replica configuration (used during handshake) |
| PSYNC | `PSYNC replid offset` | Request partial/full synchronization |
| WAIT | `WAIT numreplicas timeout_ms` | Wait for replicas to acknowledge writes |
| INFO | `INFO [section]` | Return server information (replication section supported) |

### Transaction Commands (Iteration 9)

| Command | Syntax | Description |
|---------|--------|-------------|
| MULTI | `MULTI` | Begin a transaction block |
| EXEC | `EXEC` | Execute all queued commands |
| DISCARD | `DISCARD` | Abort the transaction |
| WATCH | `WATCH key [key ...]` | Watch keys for optimistic locking |
| UNWATCH | `UNWATCH` | Unwatch all watched keys |

### Pub/Sub Commands (Iterations 8, 62)

| Command | Syntax | Description |
|---------|--------|-------------|
| SUBSCRIBE | `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| UNSUBSCRIBE | `UNSUBSCRIBE [channel ...]` | Unsubscribe from channels |
| PSUBSCRIBE | `PSUBSCRIBE pattern [pattern ...]` | Subscribe to channels matching patterns (* and ?) |
| PUNSUBSCRIBE | `PUNSUBSCRIBE [pattern ...]` | Unsubscribe from pattern subscriptions |
| PUBLISH | `PUBLISH channel message` | Publish a message to a channel |
| PUBSUB CHANNELS | `PUBSUB CHANNELS [pattern]` | List active channels |
| PUBSUB NUMSUB | `PUBSUB NUMSUB [channel ...]` | Number of subscribers per channel |
| PUBSUB NUMPAT | `PUBSUB NUMPAT` | Number of active pattern subscriptions |
| PUBSUB HELP | `PUBSUB HELP` | Display PUBSUB command help |

### Server / Persistence Commands (Iterations 6–7)

| Command | Syntax | Description |
|---------|--------|-------------|
| SAVE | `SAVE` | Save snapshot to dump.rdb (synchronous) |
| BGSAVE | `BGSAVE` | Save snapshot to dump.rdb (reports as background) |
| BGREWRITEAOF | `BGREWRITEAOF` | Rewrite the AOF file from current storage state |
| DBSIZE | `DBSIZE` | Return number of keys in the database |
| FLUSHDB | `FLUSHDB` | Remove all keys from the database |
| FLUSHALL | `FLUSHALL` | Remove all keys from all databases |

### Client Connection Commands (Iteration 13)

| Command | Syntax | Description |
|---------|--------|-------------|
| CLIENT ID | `CLIENT ID` | Return the current connection ID |
| CLIENT GETNAME | `CLIENT GETNAME` | Get the current connection name |
| CLIENT SETNAME | `CLIENT SETNAME connection-name` | Set the current connection name (no spaces allowed) |
| CLIENT LIST | `CLIENT LIST [TYPE normal]` | List all active client connections with metadata |

### Configuration Commands (Iteration 14)

| Command | Syntax | Description |
|---------|--------|-------------|
| CONFIG GET | `CONFIG GET pattern [pattern ...]` | Get configuration parameters matching glob patterns |
| CONFIG SET | `CONFIG SET parameter value [parameter value ...]` | Set configuration parameters at runtime |
| CONFIG REWRITE | `CONFIG REWRITE` | Rewrite configuration file with current settings |
| CONFIG RESETSTAT | `CONFIG RESETSTAT` | Reset server statistics |
| CONFIG HELP | `CONFIG HELP` | Show CONFIG command help |

**Supported Configuration Parameters:**

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| maxmemory | integer | 0 | No | Maximum memory limit in bytes (0 = unlimited) |
| maxmemory-policy | string | noeviction | No | Eviction policy (noeviction, allkeys-lru, volatile-lru, etc.) |
| timeout | integer | 0 | No | Client connection timeout in seconds (0 = no timeout) |
| tcp-keepalive | integer | 300 | No | TCP keepalive interval in seconds |
| port | integer | 6379 | Yes | Server listen port |
| bind | string | 127.0.0.1 | Yes | Server bind address |
| save | string | "900 1 300 10 60 10000" | No | RDB save intervals |
| appendonly | boolean | no | No | Enable AOF persistence |
| appendfsync | string | everysec | No | AOF fsync mode (always, everysec, no) |
| databases | integer | 1 | Yes | Number of databases (Zoltraak uses 1) |

### Command Introspection Commands (Iteration 15)

| Command | Syntax | Description |
|---------|--------|-------------|
| COMMAND | `COMMAND` | Return all commands with metadata |
| COMMAND COUNT | `COMMAND COUNT` | Return number of commands |
| COMMAND INFO | `COMMAND INFO <command> [<command> ...]` | Return command details (arity, flags, key positions) |
| COMMAND GETKEYS | `COMMAND GETKEYS <command> [<arg> ...]` | Extract key positions from command arguments |
| COMMAND LIST | `COMMAND LIST [FILTERBY <filter>]` | List command names (supports pattern: filter) |
| COMMAND HELP | `COMMAND HELP` | Show COMMAND command help |

### Stream Commands (Iterations 16–17, 24, 27–28)

| Command | Syntax | Description |
|---------|--------|-------------|
| XADD | `XADD key <ID \| *> field value [field value ...]` | Append entry to stream with auto or explicit ID |
| XLEN | `XLEN key` | Get number of entries in stream |
| XRANGE | `XRANGE key start end [COUNT count]` | Query range of entries by ID (use `-` for min, `+` for max) |
| XREVRANGE | `XREVRANGE key start end [COUNT count]` | Query range in reverse order (use `+` for max, `-` for min) |
| XDEL | `XDEL key ID [ID ...]` | Remove specific entries from stream by ID |
| XTRIM | `XTRIM key MAXLEN [~] count` | Trim stream to approximately maxlen entries |
| XSETID | `XSETID key <ID \| $> [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]` | Set stream metadata (last_id, entries_added, max_deleted_entry_id) |
| XREAD | `XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]` | Read entries from one or more streams ($ = only new messages) |
| XGROUP CREATE | `XGROUP CREATE key groupname <id \| $> [MKSTREAM]` | Create a consumer group |
| XGROUP DESTROY | `XGROUP DESTROY key groupname` | Destroy a consumer group |
| XGROUP SETID | `XGROUP SETID key groupname <id \| $>` | Set consumer group's last delivered ID |
| XGROUP CREATECONSUMER | `XGROUP CREATECONSUMER key groupname consumername` | Explicitly create a consumer in a group (returns 1 if created, 0 if already exists) |
| XGROUP DELCONSUMER | `XGROUP DELCONSUMER key groupname consumername` | Delete a consumer from a group (returns number of pending messages the consumer had) |
| XREADGROUP | `XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]` | Read entries from streams using consumer groups (> = new messages) |
| XACK | `XACK key groupname id [id ...]` | Acknowledge processing of messages in a consumer group |
| XCLAIM | `XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]` | Claim ownership of pending messages and transfer to another consumer |
| XAUTOCLAIM | `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]` | Automatically claim old pending messages and return next cursor |
| XPENDING | `XPENDING key group [[IDLE min-idle-time] start end count [consumer]]` | Get information about pending messages in consumer group |
| XINFO STREAM | `XINFO STREAM key [FULL [COUNT count]]` | Get information about stream metadata and entries |

### Keyspace Scanning Commands (Iteration 12)

| Command | Syntax | Description |
|---------|--------|-------------|
| SCAN | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | Iterate keyspace |
| HSCAN | `HSCAN key cursor [MATCH pattern] [COUNT count]` | Iterate hash fields |
| SSCAN | `SSCAN key cursor [MATCH pattern] [COUNT count]` | Iterate set members |
| ZSCAN | `ZSCAN key cursor [MATCH pattern] [COUNT count]` | Iterate sorted set members |
| OBJECT ENCODING | `OBJECT ENCODING key` | Get internal encoding of value |
| OBJECT REFCOUNT | `OBJECT REFCOUNT key` | Get reference count (stub: 1) |
| OBJECT IDLETIME | `OBJECT IDLETIME key` | Get idle time (stub: 0) |
| OBJECT FREQ | `OBJECT FREQ key` | Get access frequency (stub: 0) |

### String Range Commands (Iteration 12)

| Command | Syntax | Description |
|---------|--------|-------------|
| GETRANGE | `GETRANGE key start end` | Get substring of string value (alias: SUBSTR) |
| SETRANGE | `SETRANGE key offset value` | Overwrite bytes at offset, returns new length |

### Bit Operations (Iterations 20, 44)

| Command | Syntax | Description |
|---------|--------|-------------|
| SETBIT | `SETBIT key offset value` | Set or clear the bit at offset (0 or 1), returns original bit value |
| GETBIT | `GETBIT key offset` | Returns the bit value at offset |
| BITCOUNT | `BITCOUNT key [start end]` | Count set bits in string (optionally within byte range) |
| BITOP | `BITOP operation destkey key [key ...]` | Perform bitwise operation (AND, OR, XOR, NOT) between strings |
| BITFIELD | `BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP\|SAT\|FAIL]` | Perform arbitrary bitfield integer operations on strings (supports signed/unsigned integers from 1 to 64 bits) |
| BITFIELD_RO | `BITFIELD_RO key [GET type offset] [GET type offset ...]` | Read-only variant of BITFIELD (only GET operations allowed) |

### Key Management Commands (Iteration 21)

| Command | Syntax | Description |
|---------|--------|-------------|
| TTL | `TTL key` | Get time-to-live in seconds (-2 if key doesn't exist, -1 if no expiry) |
| PTTL | `PTTL key` | Get time-to-live in milliseconds |
| EXPIRETIME | `EXPIRETIME key` | Get absolute Unix timestamp (seconds) when key will expire |
| PEXPIRETIME | `PEXPIRETIME key` | Get absolute Unix timestamp (milliseconds) when key will expire |
| EXPIRE | `EXPIRE key seconds [NX\|XX\|GT\|LT]` | Set expiry in seconds from now |
| PEXPIRE | `PEXPIRE key milliseconds [NX\|XX\|GT\|LT]` | Set expiry in milliseconds from now |
| EXPIREAT | `EXPIREAT key unix-time-seconds [NX\|XX\|GT\|LT]` | Set expiry at absolute Unix timestamp (seconds) |
| PEXPIREAT | `PEXPIREAT key unix-time-ms [NX\|XX\|GT\|LT]` | Set expiry at absolute Unix timestamp (milliseconds) |
| PERSIST | `PERSIST key` | Remove expiry from key (returns 1 if removed, 0 otherwise) |
| TYPE | `TYPE key` | Get the type of value stored at key (string, list, set, hash, zset, stream, or none) |
| KEYS | `KEYS pattern` | Find all keys matching glob pattern (use with caution in production) |
| RENAME | `RENAME key newkey` | Rename a key (overwrites newkey if exists) |
| RENAMENX | `RENAMENX key newkey` | Rename key only if newkey doesn't exist |
| RANDOMKEY | `RANDOMKEY` | Return a random key from the keyspace |
| UNLINK | `UNLINK key [key ...]` | Delete keys (async deletion, currently same as DEL) |

### Advanced Key Commands (Iteration 23)

| Command | Syntax | Description |
|---------|--------|-------------|
| DUMP | `DUMP key` | Serialize the value stored at key in RDB format |
| RESTORE | `RESTORE key ttl serialized-value [REPLACE] [ABSTTL]` | Create a key from a serialized value (ttl in ms, 0 = no expiry) |
| COPY | `COPY source destination [DB db] [REPLACE]` | Copy a key to a new key (returns 1 if copied, 0 otherwise) |
| TOUCH | `TOUCH key [key ...]` | Update last access time of keys (returns count of touched keys) |
| MOVE | `MOVE key db` | Move key to another database (stub: always returns 0, single-DB only) |

### Sorted Set Commands (Iterations 5, 11, 12, 42, 43, 49)

| Command | Syntax | Description |
|---------|--------|-------------|
| ZADD | `ZADD key [NX\|XX] [CH] score member [score member ...]` | Add members with scores |
| ZREM | `ZREM key member [member ...]` | Remove members from sorted set |
| ZRANGE | `ZRANGE key start stop [WITHSCORES]` | Get range of members by rank |
| ZREVRANGE | `ZREVRANGE key start stop [WITHSCORES]` | Get range in reverse order |
| ZRANGEBYSCORE | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Get members by score range |
| ZREVRANGEBYSCORE | `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` | Get members by score descending |
| ZRANGEBYLEX | `ZRANGEBYLEX key min max [LIMIT offset count]` | Return members in lexicographical range (for equal scores) |
| ZREVRANGEBYLEX | `ZREVRANGEBYLEX key max min [LIMIT offset count]` | Return members in reverse lexicographical range |
| ZSCORE | `ZSCORE key member` | Get score of member |
| ZMSCORE | `ZMSCORE key member [member ...]` | Get scores for multiple members |
| ZCARD | `ZCARD key` | Get number of members |
| ZRANK | `ZRANK key member` | Get rank of member (ascending) |
| ZREVRANK | `ZREVRANK key member` | Get rank of member (descending) |
| ZCOUNT | `ZCOUNT key min max` | Count members in score range |
| ZLEXCOUNT | `ZLEXCOUNT key min max` | Count members in lexicographical range |
| ZRANGESTORE | `ZRANGESTORE dest source start stop [WITHSCORES]` | Store ZRANGE result in destination sorted set, returns count of members stored |
| ZINTERCARD | `ZINTERCARD numkeys key [key ...] [LIMIT limit]` | Return cardinality of intersection of sorted sets (up to limit if specified) |
| ZINCRBY | `ZINCRBY key increment member` | Increment score of member |
| ZPOPMIN | `ZPOPMIN key [count]` | Remove and return lowest-score members |
| ZPOPMAX | `ZPOPMAX key [count]` | Remove and return highest-score members |
| ZMPOP | `ZMPOP numkeys key [key ...] MIN\|MAX [COUNT count]` | Multi-pop from first non-empty sorted set |
| BZPOPMIN | `BZPOPMIN key [key ...] timeout` | Blocking pop minimum from first non-empty sorted set |
| BZPOPMAX | `BZPOPMAX key [key ...] timeout` | Blocking pop maximum from first non-empty sorted set |
| BZMPOP | `BZMPOP timeout numkeys key [key ...] MIN\|MAX [COUNT count]` | Blocking multi-pop from first non-empty sorted set |
| ZRANDMEMBER | `ZRANDMEMBER key [count [WITHSCORES]]` | Return random members |
| ZREMRANGEBYRANK | `ZREMRANGEBYRANK key start stop` | Remove all members in a sorted set within the given rank range |
| ZREMRANGEBYSCORE | `ZREMRANGEBYSCORE key min max` | Remove all members in a sorted set within the given score range |
| ZREMRANGEBYLEX | `ZREMRANGEBYLEX key min max` | Remove all members in a sorted set within the given lexicographical range |
| ZSCAN | `ZSCAN key cursor [MATCH pattern] [COUNT count]` | Iterate sorted set members |
| ZUNION | `ZUNION numkeys key [key ...] [WITHSCORES]` | Return the union of multiple sorted sets (scores are summed) |
| ZINTER | `ZINTER numkeys key [key ...] [WITHSCORES]` | Return the intersection of multiple sorted sets (scores are summed) |
| ZDIFF | `ZDIFF numkeys key [key ...] [WITHSCORES]` | Return the difference of sorted sets (first minus others) |
| ZUNIONSTORE | `ZUNIONSTORE destination numkeys key [key ...]` | Store union of sorted sets in destination, returns member count |
| ZINTERSTORE | `ZINTERSTORE destination numkeys key [key ...]` | Store intersection of sorted sets in destination, returns member count |
| ZDIFFSTORE | `ZDIFFSTORE destination numkeys key [key ...]` | Store difference of sorted sets in destination, returns member count |

### Geospatial Commands (Iterations 25, 56, 58)

| Command | Syntax | Description |
|---------|--------|-------------|
| GEOADD | `GEOADD key longitude latitude member [longitude latitude member ...]` | Add geospatial items (longitude, latitude, name) to a key |
| GEOPOS | `GEOPOS key member [member ...]` | Returns longitude and latitude of members |
| GEODIST | `GEODIST key member1 member2 [m\|km\|ft\|mi]` | Returns distance between two members (default: meters) |
| GEOHASH | `GEOHASH key member [member ...]` | Returns geohash strings representing positions of members |
| GEORADIUS | `GEORADIUS key longitude latitude radius m\|km\|ft\|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC\|DESC]` | Query members within radius from point |
| GEORADIUS_RO | `GEORADIUS_RO key longitude latitude radius m\|km\|ft\|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC\|DESC]` | Read-only variant of GEORADIUS (deprecated but used by read-only replicas) |
| GEORADIUSBYMEMBER | `GEORADIUSBYMEMBER key member radius m\|km\|ft\|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC\|DESC]` | Query members within radius from a member's location |
| GEORADIUSBYMEMBER_RO | `GEORADIUSBYMEMBER_RO key member radius m\|km\|ft\|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC\|DESC]` | Read-only variant of GEORADIUSBYMEMBER |
| GEOSEARCH | `GEOSEARCH key FROMMEMBER member \| FROMLONLAT lon lat BYRADIUS radius \| BYBOX width height m\|km\|ft\|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC\|DESC]` | Modern unified geospatial query with BYRADIUS and BYBOX support |
| GEOSEARCHSTORE | `GEOSEARCHSTORE dest source FROMMEMBER member \| FROMLONLAT lon lat BYRADIUS radius \| BYBOX width height m\|km\|ft\|mi [COUNT count] [ASC\|DESC] [STOREDIST]` | Store GEOSEARCH results in sorted set (STOREDIST stores distances instead of geohashes) |

### HyperLogLog Commands (Iteration 26)

| Command | Syntax | Description |
|---------|--------|-------------|
| PFADD | `PFADD key element [element ...]` | Add elements to HyperLogLog, returns 1 if at least one register was updated |
| PFCOUNT | `PFCOUNT key [key ...]` | Return the approximated cardinality of the set(s) observed by HyperLogLog (can merge multiple keys) |
| PFMERGE | `PFMERGE destkey sourcekey [sourcekey ...]` | Merge multiple HyperLogLog values into a single one at destkey |

### Server Introspection Commands (Iterations 29–30)

| Command | Syntax | Description |
|---------|--------|-------------|
| MEMORY STATS | `MEMORY STATS` | Show memory usage statistics (stub implementation) |
| MEMORY USAGE | `MEMORY USAGE key` | Estimate memory usage of a key in bytes |
| MEMORY DOCTOR | `MEMORY DOCTOR` | Get memory usage advice |
| MEMORY HELP | `MEMORY HELP` | Show MEMORY command help |
| SLOWLOG GET | `SLOWLOG GET [count]` | Get slow log entries (stub - always returns empty) |
| SLOWLOG LEN | `SLOWLOG LEN` | Get slow log length (stub - always returns 0) |
| SLOWLOG RESET | `SLOWLOG RESET` | Reset slow log (stub - always returns OK) |
| INFO | `INFO [section]` | Get comprehensive server information (supports server, clients, memory, persistence, stats, replication, cpu, keyspace, all, default sections) |

### Protocol Negotiation Commands (Iterations 31-35)

| Command | Syntax | Description |
|---------|--------|-------------|
| HELLO | `HELLO [protover [AUTH username password] [SETNAME clientname]]` | Protocol negotiation command - negotiates RESP2/RESP3 protocol version per connection, returns server information in negotiated format (map for RESP3, array for RESP2) |

**RESP3 Protocol-Aware Response Formatting (Iterations 33-35):**
- `HGETALL`: Returns RESP3 map (`%<count>\r\n<key><value>...\r\n`) when RESP3 is negotiated, RESP2 array otherwise
- `SMEMBERS`: Returns RESP3 set (`~<count>\r\n<elem>...\r\n`) when RESP3 is negotiated, RESP2 array otherwise
- `HKEYS`: Returns RESP3 set when RESP3 is negotiated (field names are unique), RESP2 array otherwise
- `ZRANGE` with WITHSCORES: Returns RESP3 map (member → score) when RESP3 is negotiated, RESP2 flat array otherwise
- `ZREVRANGE` with WITHSCORES: Returns RESP3 map (member → score) when RESP3 is negotiated, RESP2 flat array otherwise
- `SINTER`: Returns RESP3 set when RESP3 is negotiated, RESP2 array otherwise
- `SUNION`: Returns RESP3 set when RESP3 is negotiated, RESP2 array otherwise
- `SDIFF`: Returns RESP3 set when RESP3 is negotiated, RESP2 array otherwise
- Protocol version is tracked per connection and persists for the session duration
- All other commands continue to work with both RESP2 and RESP3

### Scripting Commands (Iteration 36)

| Command | Syntax | Description |
|---------|--------|-------------|
| EVAL | `EVAL script numkeys key [key ...] arg [arg ...]` | Execute a Lua script server side (stub implementation - returns nil) |
| EVALSHA | `EVALSHA sha1 numkeys key [key ...] arg [arg ...]` | Execute a cached script by SHA1 digest (stub implementation - returns nil) |
| SCRIPT LOAD | `SCRIPT LOAD script` | Load a script into the script cache and return its SHA1 digest |
| SCRIPT EXISTS | `SCRIPT EXISTS sha1 [sha1 ...]` | Check if scripts exist in the cache (returns array of 1/0) |
| SCRIPT FLUSH | `SCRIPT FLUSH [ASYNC\|SYNC]` | Remove all scripts from the script cache |
| SCRIPT HELP | `SCRIPT HELP` | Show help for SCRIPT command |

**Note**: The scripting commands provide Redis-compatible interfaces but currently return stub values (nil for EVAL/EVALSHA). Full Lua script execution would require embedding a Lua interpreter. The SCRIPT LOAD command generates proper SHA1 hashes and stores scripts for future reference.

### ACL (Access Control List) Commands (Iteration 37)

| Command | Syntax | Description |
|---------|--------|-------------|
| ACL WHOAMI | `ACL WHOAMI` | Return the current connection username (stub: always returns "default") |
| ACL LIST | `ACL LIST` | List all ACL rules in config file format (stub: only default user) |
| ACL USERS | `ACL USERS` | List all usernames (stub: only "default" exists) |
| ACL GETUSER | `ACL GETUSER <username>` | Get user details including flags, passwords, commands, and keys (stub: only default user supported) |
| ACL SETUSER | `ACL SETUSER <username> <attribute> [<attribute> ...]` | Create or modify user with specified attributes (stub: accepts but doesn't persist) |
| ACL DELUSER | `ACL DELUSER <username> [<username> ...]` | Delete one or more users (stub: cannot delete default user) |
| ACL CAT | `ACL CAT [<category>]` | List command categories or commands in a category |
| ACL HELP | `ACL HELP` | Show help for ACL command |

**Note**: The ACL commands provide Redis-compatible interfaces but are stub implementations. Authentication and authorization are not enforced - all clients have full access. The commands return appropriate responses for compatibility with Redis clients but do not store or enforce user permissions.

### CLUSTER Commands (Iteration 38)

| Command | Syntax | Description |
|---------|--------|-------------|
| CLUSTER SLOTS | `CLUSTER SLOTS` | Return cluster slots configuration (stub: single node covering all 16384 slots) |
| CLUSTER NODES | `CLUSTER NODES` | Return cluster nodes configuration (stub: single standalone node) |
| CLUSTER INFO | `CLUSTER INFO` | Return cluster state information (stub: reports cluster as ok with 1 node) |
| CLUSTER MYID | `CLUSTER MYID` | Return the node ID (stub: returns "zoltraak-standalone-node") |
| CLUSTER KEYSLOT | `CLUSTER KEYSLOT <key>` | Return hash slot (0-16383) for a key using CRC16 algorithm |
| CLUSTER COUNTKEYSINSLOT | `CLUSTER COUNTKEYSINSLOT <slot>` | Count keys in a hash slot (stub: always returns 0) |
| CLUSTER GETKEYSINSLOT | `CLUSTER GETKEYSINSLOT <slot> <count>` | Return keys in a hash slot (stub: always returns empty array) |
| CLUSTER HELP | `CLUSTER HELP` | Show help for CLUSTER command |

**Note**: The CLUSTER commands provide Redis-compatible interfaces for cluster mode, but Zoltraak operates as a single standalone node. Most commands return stub values indicating a single-node cluster. CLUSTER KEYSLOT correctly implements the Redis CRC16 hash slot algorithm with hash tag support (e.g., `{foo}bar` uses "foo" for hashing), which is useful for understanding key distribution even in standalone mode.

### Utility Commands (Iterations 39-41)

| Command | Syntax | Description |
|---------|--------|-------------|
| ECHO | `ECHO message` | Returns the given message |
| QUIT | `QUIT` | Close the connection (returns OK then server closes connection) |
| SELECT | `SELECT index` | Select the database by index (only DB 0 supported, single-database mode) |
| SWAPDB | `SWAPDB index1 index2` | Swap two databases (stub: only SWAPDB 0 0 supported in single-database mode) |
| TIME | `TIME` | Returns the current Unix timestamp in seconds and microseconds |
| LASTSAVE | `LASTSAVE` | Returns Unix timestamp of the last successful RDB save to disk |
| MONITOR | `MONITOR` | Enable real-time command monitoring (stub: returns OK, monitoring not implemented) |
| DEBUG OBJECT | `DEBUG OBJECT key` | Show low-level info about key and associated value |
| DEBUG HELP | `DEBUG HELP` | Show help for DEBUG command |
| SHUTDOWN | `SHUTDOWN [NOSAVE\|SAVE]` | Request graceful server shutdown (stub: returns OK, doesn't actually shut down) |

**Note**: MONITOR and SHUTDOWN are stub implementations that return OK for Redis compatibility but do not perform actual monitoring or shutdown operations. SELECT only accepts database index 0 (Zoltraak uses a single-database architecture). SWAPDB is a stub that only accepts swapping database 0 with itself (a no-op).

## Example Session

```
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "Hello World"
OK
127.0.0.1:6379> GET mykey
"Hello World"
127.0.0.1:6379> SET session token EX 60
OK
127.0.0.1:6379> EXISTS mykey session
(integer) 2
127.0.0.1:6379> DEL mykey session
(integer) 2

# String manipulation operations
127.0.0.1:6379> SET counter "10"
OK
127.0.0.1:6379> INCR counter
(integer) 11
127.0.0.1:6379> INCRBY counter 5
(integer) 16
127.0.0.1:6379> DECR counter
(integer) 15
127.0.0.1:6379> APPEND counter "00"
(integer) 4
127.0.0.1:6379> GET counter
"1500"
127.0.0.1:6379> STRLEN counter
(integer) 4
127.0.0.1:6379> MSET key1 "val1" key2 "val2" key3 "val3"
OK
127.0.0.1:6379> MGET key1 key2 key3
1) "val1"
2) "val2"
3) "val3"

# List operations
127.0.0.1:6379> RPUSH tasks "task1" "task2" "task3"
(integer) 3
127.0.0.1:6379> LRANGE tasks 0 -1
1) "task1"
2) "task2"
3) "task3"
127.0.0.1:6379> LPOP tasks
"task1"
127.0.0.1:6379> LLEN tasks
(integer) 2

# Set operations
127.0.0.1:6379> SADD tags "redis" "zig" "database"
(integer) 3
127.0.0.1:6379> SISMEMBER tags "zig"
(integer) 1
127.0.0.1:6379> SMEMBERS tags
1) "redis"
2) "zig"
3) "database"
127.0.0.1:6379> SCARD tags
(integer) 3

# Hash operations
127.0.0.1:6379> HSET user:1 name "Alice" email "alice@example.com" age "30"
(integer) 3
127.0.0.1:6379> HGET user:1 name
"Alice"
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "email"
4) "alice@example.com"
5) "age"
6) "30"
127.0.0.1:6379> HLEN user:1
(integer) 3
127.0.0.1:6379> HDEL user:1 email
(integer) 1
127.0.0.1:6379> HEXISTS user:1 email
(integer) 0
```

## Project Status

Iterations 1–50 are complete.
- Iteration 12: 22 commands (SCAN family, SPOP, SRANDMEMBER, SMOVE, SMISMEMBER, SINTERCARD, ZPOPMIN, ZPOPMAX, ZMSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZRANDMEMBER, GETRANGE, SETRANGE, OBJECT subcommands)
- Iteration 13: 4 CLIENT commands (CLIENT ID, CLIENT GETNAME, CLIENT SETNAME, CLIENT LIST)
- Iteration 14: 5 CONFIG commands (CONFIG GET, CONFIG SET, CONFIG REWRITE, CONFIG RESETSTAT, CONFIG HELP) with 10 configuration parameters
- Iteration 15: 6 COMMAND introspection commands (COMMAND, COMMAND COUNT, COMMAND INFO, COMMAND GETKEYS, COMMAND LIST, COMMAND HELP)
- Iteration 16: 3 STREAM commands (XADD, XLEN, XRANGE) - basic stream data type support
- Iteration 17: 3 additional STREAM commands (XREVRANGE, XDEL, XTRIM) - stream manipulation and maintenance
- Iteration 18: 3 blocking list commands (BLPOP, BRPOP, BLMOVE) - immediate-return implementation for single-threaded architecture
- Iteration 19: 16 string manipulation commands (INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETSET, GETDEL, GETEX, SETNX, SETEX, PSETEX, MGET, MSET, MSETNX) - comprehensive string operations
- Iteration 20: 4 bit operation commands (SETBIT, GETBIT, BITCOUNT, BITOP) - bitmap manipulation for efficient storage and operations
- Iteration 21: 15 key management commands (TTL, PTTL, EXPIRETIME, PEXPIRETIME, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TYPE, KEYS, RENAME, RENAMENX, RANDOMKEY, UNLINK) - comprehensive key lifecycle management
- Iteration 22: 3 advanced blocking commands (BLMPOP, BZPOPMIN, BZPOPMAX) - blocking multi-key pop operations for lists and sorted sets
- Iteration 23: 5 advanced key commands (DUMP, RESTORE, COPY, TOUCH, MOVE) - key serialization, copying, and migration support
- Iteration 24: 6 advanced stream commands (XREAD, XGROUP CREATE/DESTROY/SETID, XREADGROUP, XACK) - consumer groups and advanced stream consumption patterns
- Iteration 25: 6 geospatial commands (GEOADD, GEOPOS, GEODIST, GEOHASH, GEORADIUS, GEOSEARCH) - geospatial indexing and radius queries using geohash encoding
- Iteration 26: 3 HyperLogLog commands (PFADD, PFCOUNT, PFMERGE) - probabilistic cardinality estimation with 16384 6-bit registers
- Iteration 27: 2 stream introspection commands (XPENDING, XINFO STREAM) - pending message tracking and stream metadata inspection
- Iteration 28: 2 stream consumer group recovery commands (XCLAIM, XAUTOCLAIM) - claim ownership of pending messages and transfer between consumers
- Iteration 29: 7 server introspection commands (MEMORY STATS/USAGE/DOCTOR/HELP, SLOWLOG GET/LEN/RESET) - server monitoring and debugging tools (stub implementations)
- Iteration 30: Comprehensive INFO command - complete implementation with all major sections (Server, Clients, Memory, Persistence, Stats, Replication, CPU, Keyspace)
- Iteration 31: RESP3 protocol support - parser and writer for RESP3 types (null, boolean, double, big number, bulk error, verbatim string, map, set, push), HELLO command for protocol negotiation (basic RESP2 implementation)
- Iteration 32: Full RESP3 integration - per-connection protocol tracking, HELLO command negotiates and persists protocol version (RESP2 or RESP3), responses formatted according to negotiated protocol
- Iteration 33: Protocol-aware response formatting - HGETALL returns RESP3 map when RESP3 negotiated, SMEMBERS returns RESP3 set when RESP3 negotiated, leveraging native RESP3 collection types for better semantic clarity
- Iteration 34: Extended RESP3-aware commands - HKEYS returns RESP3 set (field names are unique), ZRANGE/ZREVRANGE with WITHSCORES return RESP3 map (member → score), expanding native RESP3 type usage for improved semantic clarity
- Iteration 35: Set operation RESP3 support - SINTER/SUNION/SDIFF return RESP3 set when RESP3 negotiated, completing RESP3 native type usage for all set-returning commands
- Iteration 36: Basic scripting support - EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH/HELP commands with SHA1 script caching (stub implementation - returns nil, full Lua execution pending)
- Iteration 37: ACL (Access Control List) basic stubs - ACL WHOAMI/LIST/USERS/GETUSER/SETUSER/DELUSER/CAT/HELP commands (stub implementation - authentication not enforced, always uses "default" user)
- Iteration 38: CLUSTER basic stubs - CLUSTER SLOTS/NODES/INFO/MYID/KEYSLOT/COUNTKEYSINSLOT/GETKEYSINSLOT/HELP commands (stub implementation - single standalone node, KEYSLOT implements full CRC16 hash slot algorithm with hash tag support)
- Iteration 39: Utility commands - ECHO, QUIT, TIME, LASTSAVE, MONITOR, DEBUG (OBJECT, HELP), SHUTDOWN commands for server management and debugging (MONITOR and SHUTDOWN are stubs)
- Iteration 40: SELECT command - database selection (single-database mode, only DB 0 supported for Redis client compatibility)
- Iteration 41: SWAPDB command - database swapping (stub implementation - single-database mode, only SWAPDB 0 0 supported as no-op)
- Iteration 42: Advanced sorted set operations and hash string length - ZUNION/ZINTER/ZDIFF (set-like operations on sorted sets with score aggregation), ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE (store results), HSTRLEN (hash field value string length)
- Iteration 43: Sorted set range removal and lexicographical operations - ZREMRANGEBYRANK/ZREMRANGEBYSCORE/ZREMRANGEBYLEX (range deletion by rank/score/lex), ZRANGEBYLEX/ZREVRANGEBYLEX (lexicographical range queries for equal-score members), ZLEXCOUNT (count members in lex range)
- Iteration 44: Bitfield operations - BITFIELD (arbitrary bitfield integer operations with GET/SET/INCRBY and WRAP/SAT/FAIL overflow modes), BITFIELD_RO (read-only variant) - supports signed and unsigned integers from 1 to 64 bits
- Iteration 45: LCS (Longest Common Subsequence) command - LCS key1 key2 [LEN] for string comparison, returns the longest common subsequence string or its length
- Iteration 46: LCS IDX mode - LCS key1 key2 IDX [MINMATCHLEN len] [WITHMATCHLEN] returns match positions as arrays with "matches" (key1_range, key2_range, optional match_len) and "len" keys
- Iteration 47: MSETEX command - MSETEX numkeys key value [key value ...] [NX|XX] [EX/PX/EXAT/PXAT/KEEPTTL] atomically sets multiple keys with optional shared expiration (new Redis 8.4+ command)
- Iteration 48: HRANDFIELD command - HRANDFIELD key [count [WITHVALUES]] returns random field(s) from hash (single field without count, array with count, field-value pairs with WITHVALUES, RESP3 map support)
- Iteration 49: Multi-pop commands (Redis 7.0+) - LMPOP (non-blocking list multi-pop), ZMPOP (sorted set multi-pop with MIN/MAX), BZMPOP (blocking sorted set multi-pop) - unified pop operations from multiple keys
- Iteration 50: Hash field-level TTL commands (Redis 7.4+) - HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT (set field expiration), HPERSIST (remove field expiration), HTTL/HPTTL (get field TTL), HEXPIRETIME/HPEXPIRETIME (get field expiration timestamp) - fine-grained expiration control for hash fields
- Iteration 52: Hash atomic commands (Redis 8.0+) - HGETDEL (atomically get and delete fields), HGETEX (atomically get and set field expiration), HSETEX (atomically set fields with expiration and conditionals FNX/FXX/KEEPTTL) - Phase 1.1 core hash command gaps from PRD
- Iteration 53: Sorted set store and intersection cardinality — ZRANGESTORE (store ZRANGE result in destination key, returns member count), ZINTERCARD (count intersection cardinality with LIMIT support for early exit optimization)
- Iteration 54: Stream consumer management — XGROUP CREATECONSUMER (explicitly create consumer in group, returns 1 if new/0 if exists), XGROUP DELCONSUMER (delete consumer from group, returns pending message count) - Phase 1.3 stream command gaps from PRD

### Roadmap

- [x] Project structure and build system
- [x] TCP server implementation
- [x] RESP2 protocol parser
- [x] Basic commands (GET, SET, DEL)
- [x] String operations (PING, EXISTS)
- [x] Key expiration (EX, PX options)
- [x] List operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, +10 more)
- [x] Set operations (SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SMISMEMBER, SINTERCARD, +union/inter/diff)
- [x] Hash operations (HSET, HGET, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN, HMGET, HINCRBY, HINCRBYFLOAT, HSETNX)
- [x] Sorted set operations (ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD, ZRANK, ZREVRANK, ZINCRBY, ZCOUNT, ZPOPMIN, ZPOPMAX, ZMSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZRANDMEMBER)
- [x] Persistence (RDB snapshots — SAVE, BGSAVE, auto-load on startup)
- [x] Persistence (AOF — append-only log, replay on startup, BGREWRITEAOF)
- [x] Pub/Sub (SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PUBSUB CHANNELS/NUMSUB/NUMPAT/HELP, PSUBSCRIBE, PUNSUBSCRIBE — pattern matching with * and ?)
- [x] Transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH — optimistic locking)
- [x] Replication (REPLICAOF, REPLCONF, PSYNC, WAIT, INFO — primary/replica mode)

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Redis](https://redis.io/) - The original inspiration
- [Zig](https://ziglang.org/) - The implementation language
