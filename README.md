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

### String Commands (Iteration 1)

| Command | Syntax | Description |
|---------|--------|-------------|
| PING | `PING [message]` | Test connectivity |
| SET | `SET key value [EX s] [PX ms] [NX\|XX]` | Set key-value with optional TTL |
| GET | `GET key` | Get value by key |
| DEL | `DEL key [key ...]` | Delete one or more keys |
| EXISTS | `EXISTS key [key ...]` | Check if keys exist |

### List Commands (Iteration 2)

| Command | Syntax | Description |
|---------|--------|-------------|
| LPUSH | `LPUSH key element [element ...]` | Push elements to list head |
| RPUSH | `RPUSH key element [element ...]` | Push elements to list tail |
| LPOP | `LPOP key [count]` | Pop elements from list head |
| RPOP | `RPOP key [count]` | Pop elements from list tail |
| LRANGE | `LRANGE key start stop` | Get range of elements |
| LLEN | `LLEN key` | Get list length |

### Set Commands (Iteration 3)

| Command | Syntax | Description |
|---------|--------|-------------|
| SADD | `SADD key member [member ...]` | Add members to set |
| SREM | `SREM key member [member ...]` | Remove members from set |
| SISMEMBER | `SISMEMBER key member` | Check if member exists |
| SMEMBERS | `SMEMBERS key` | Get all members |
| SCARD | `SCARD key` | Get set cardinality |

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

### Sorted Set Commands (Iteration 5)

| Command | Syntax | Description |
|---------|--------|-------------|
| ZADD | `ZADD key [NX\|XX] [CH] score member [score member ...]` | Add members with scores |
| ZREM | `ZREM key member [member ...]` | Remove members from sorted set |
| ZRANGE | `ZRANGE key start stop [WITHSCORES]` | Get range of members by rank |
| ZRANGEBYSCORE | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Get members by score range |
| ZSCORE | `ZSCORE key member` | Get score of member |
| ZCARD | `ZCARD key` | Get number of members |

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

Iterations 1–10 are complete. Replication (primary/replica mode) is now fully implemented.

### Roadmap

- [x] Project structure and build system
- [x] TCP server implementation
- [x] RESP2 protocol parser
- [x] Basic commands (GET, SET, DEL)
- [x] String operations (PING, EXISTS)
- [x] Key expiration (EX, PX options)
- [x] List operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- [x] Set operations (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
- [x] Hash operations (HSET, HGET, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN)
- [x] Sorted set operations (ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD)
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
