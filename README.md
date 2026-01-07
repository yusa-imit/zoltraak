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
- Transactions (MULTI/EXEC)
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
```

## Project Status

Iteration 2 is complete with List data structure support.

### Roadmap

- [x] Project structure and build system
- [x] TCP server implementation
- [x] RESP2 protocol parser
- [x] Basic commands (GET, SET, DEL)
- [x] String operations (PING, EXISTS)
- [x] Key expiration (EX, PX options)
- [x] List operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- [ ] Set operations (SADD, SREM, SMEMBERS, SISMEMBER)
- [ ] Hash operations (HSET, HGET, HDEL, HGETALL)
- [ ] Sorted set operations (ZADD, ZREM, ZRANGE)
- [ ] Persistence (RDB)
- [ ] Persistence (AOF)
- [ ] Pub/Sub
- [ ] Transactions
- [ ] Replication

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Redis](https://redis.io/) - The original inspiration
- [Zig](https://ziglang.org/) - The implementation language
