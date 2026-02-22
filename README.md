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
```

## Supported Commands

### String Commands (Iterations 1, 19)

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

### List Commands (Iterations 2, 11, 18)

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

### Hash Commands (Iteration 4)

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

### Pub/Sub Commands (Iteration 8)

| Command | Syntax | Description |
|---------|--------|-------------|
| SUBSCRIBE | `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| UNSUBSCRIBE | `UNSUBSCRIBE [channel ...]` | Unsubscribe from channels |
| PUBLISH | `PUBLISH channel message` | Publish a message to a channel |
| PUBSUB CHANNELS | `PUBSUB CHANNELS [pattern]` | List active channels |
| PUBSUB NUMSUB | `PUBSUB NUMSUB [channel ...]` | Number of subscribers per channel |

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

### Stream Commands (Iterations 16–17)

| Command | Syntax | Description |
|---------|--------|-------------|
| XADD | `XADD key <ID \| *> field value [field value ...]` | Append entry to stream with auto or explicit ID |
| XLEN | `XLEN key` | Get number of entries in stream |
| XRANGE | `XRANGE key start end [COUNT count]` | Query range of entries by ID (use `-` for min, `+` for max) |
| XREVRANGE | `XREVRANGE key start end [COUNT count]` | Query range in reverse order (use `+` for max, `-` for min) |
| XDEL | `XDEL key ID [ID ...]` | Remove specific entries from stream by ID |
| XTRIM | `XTRIM key MAXLEN [~] count` | Trim stream to approximately maxlen entries |

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

### Bit Operations (Iteration 20)

| Command | Syntax | Description |
|---------|--------|-------------|
| SETBIT | `SETBIT key offset value` | Set or clear the bit at offset (0 or 1), returns original bit value |
| GETBIT | `GETBIT key offset` | Returns the bit value at offset |
| BITCOUNT | `BITCOUNT key [start end]` | Count set bits in string (optionally within byte range) |
| BITOP | `BITOP operation destkey key [key ...]` | Perform bitwise operation (AND, OR, XOR, NOT) between strings |

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

### Sorted Set Commands (Iterations 5, 11, 12)

| Command | Syntax | Description |
|---------|--------|-------------|
| ZADD | `ZADD key [NX\|XX] [CH] score member [score member ...]` | Add members with scores |
| ZREM | `ZREM key member [member ...]` | Remove members from sorted set |
| ZRANGE | `ZRANGE key start stop [WITHSCORES]` | Get range of members by rank |
| ZREVRANGE | `ZREVRANGE key start stop [WITHSCORES]` | Get range in reverse order |
| ZRANGEBYSCORE | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Get members by score range |
| ZREVRANGEBYSCORE | `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` | Get members by score descending |
| ZSCORE | `ZSCORE key member` | Get score of member |
| ZMSCORE | `ZMSCORE key member [member ...]` | Get scores for multiple members |
| ZCARD | `ZCARD key` | Get number of members |
| ZRANK | `ZRANK key member` | Get rank of member (ascending) |
| ZREVRANK | `ZREVRANK key member` | Get rank of member (descending) |
| ZCOUNT | `ZCOUNT key min max` | Count members in score range |
| ZINCRBY | `ZINCRBY key increment member` | Increment score of member |
| ZPOPMIN | `ZPOPMIN key [count]` | Remove and return lowest-score members |
| ZPOPMAX | `ZPOPMAX key [count]` | Remove and return highest-score members |
| BZPOPMIN | `BZPOPMIN key [key ...] timeout` | Blocking pop minimum from first non-empty sorted set |
| BZPOPMAX | `BZPOPMAX key [key ...] timeout` | Blocking pop maximum from first non-empty sorted set |
| ZRANDMEMBER | `ZRANDMEMBER key [count [WITHSCORES]]` | Return random members |
| ZSCAN | `ZSCAN key cursor [MATCH pattern] [COUNT count]` | Iterate sorted set members |

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

Iterations 1–23 are complete.
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
- [x] Pub/Sub (SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PUBSUB CHANNELS/NUMSUB)
- [x] Transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH — optimistic locking)
- [x] Replication (REPLICAOF, REPLCONF, PSYNC, WAIT, INFO — primary/replica mode)

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Redis](https://redis.io/) - The original inspiration
- [Zig](https://ziglang.org/) - The implementation language
