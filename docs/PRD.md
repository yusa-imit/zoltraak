# Zoltraak 1.0 — Product Requirements Document

**Goal: 100% Redis compatibility (core + optional + enterprise features)**

**Current state:** 150+ commands across 50 iterations, RESP2/RESP3, persistence, replication, pub/sub, transactions
**Target state:** 500+ commands, full Lua scripting, ACL enforcement, cluster mode, Sentinel, modules API, TLS, and all Redis 8.x built-in data structures

---

## Table of Contents

1. [Gap Analysis Summary](#1-gap-analysis-summary)
2. [Phase 1 — Core Command Gaps](#2-phase-1--core-command-gaps)
3. [Phase 2 — Lua Scripting Engine](#3-phase-2--lua-scripting-engine)
4. [Phase 3 — ACL Enforcement](#4-phase-3--acl-enforcement)
5. [Phase 4 — Full Pub/Sub](#5-phase-4--full-pubsub)
6. [Phase 5 — Full Client/Connection Commands](#6-phase-5--full-clientconnection-commands)
7. [Phase 6 — Server Management (LATENCY, SLOWLOG, MONITOR, DEBUG)](#7-phase-6--server-management)
8. [Phase 7 — Multi-Database Support](#8-phase-7--multi-database-support)
9. [Phase 8 — Cluster Mode](#9-phase-8--cluster-mode)
10. [Phase 9 — Redis Sentinel](#10-phase-9--redis-sentinel)
11. [Phase 10 — TLS/SSL](#11-phase-10--tlsssl)
12. [Phase 11 — Redis Functions (7.0+)](#12-phase-11--redis-functions)
13. [Phase 12 — JSON Data Type](#13-phase-12--json-data-type)
14. [Phase 13 — Search Engine (FT)](#14-phase-13--search-engine)
15. [Phase 14 — Time Series](#15-phase-14--time-series)
16. [Phase 15 — Probabilistic Data Structures](#16-phase-15--probabilistic-data-structures)
17. [Phase 16 — Vector Sets](#17-phase-16--vector-sets)
18. [Phase 17 — Modules API](#18-phase-17--modules-api)
19. [Phase 18 — Advanced Features & Polish](#19-phase-18--advanced-features--polish)
20. [Non-Functional Requirements](#20-non-functional-requirements)
21. [Release Criteria](#21-release-criteria)

---

## 1. Gap Analysis Summary

### What's implemented (Iterations 1-50)

| Category | Implemented | Redis Total | Coverage |
|----------|-------------|-------------|----------|
| String | 25 | 25 | 100% |
| List | 22 | 22 | 100% |
| Set | 17 | 17 | 100% |
| Sorted Set | 32 | 35 | 91% |
| Hash | 19 | 28 | 68% |
| Stream | 14 | 24 | 58% |
| Bitmap | 6 | 7 | 86% |
| HyperLogLog | 3 | 3 | 100% |
| Geospatial | 6 | 10 | 60% |
| Pub/Sub | 5 | 14 | 36% |
| Transactions | 5 | 5 | 100% |
| Scripting & Functions | 6 (stubs) | 21 | 0% (functional) |
| Connection / CLIENT | 6 | 25 | 24% |
| Server / Admin | ~30 (many stubs) | ~60 | ~25% (functional) |
| Cluster | 8 (stubs) | ~34 | 0% (functional) |
| Generic Key | ~20 | 35 | 57% |
| JSON | 0 | 26 | 0% |
| Search (FT) | 0 | 30 | 0% |
| Time Series (TS) | 0 | 17 | 0% |
| Bloom Filter (BF) | 0 | 10 | 0% |
| Cuckoo Filter (CF) | 0 | 12 | 0% |
| Count-Min Sketch (CMS) | 0 | 6 | 0% |
| T-Digest | 0 | 14 | 0% |
| TopK | 0 | 7 | 0% |
| Vector Set (V) | 0 | 13 | 0% |

### Stub-only implementations requiring real logic

- **Lua scripting** — EVAL/EVALSHA return nil; no Lua interpreter embedded
- **ACL** — commands exist but no authentication/authorization enforcement
- **Cluster** — single-node stubs; no sharding, gossip, or failover
- **MONITOR** — returns OK but doesn't stream commands
- **SLOWLOG** — always returns empty
- **SHUTDOWN** — returns OK but doesn't shut down
- **SELECT** — only DB 0
- **MEMORY** — stub values for STATS/DOCTOR
- **Blocking commands** — immediate-return; don't truly block-wait

---

## 2. Phase 1 — Core Command Gaps

Fill missing commands in already-implemented data structures.

### 2.1 Hash commands (9 missing)

| Command | Priority | Notes |
|---------|----------|-------|
| HINCRBY | P0 | Basic arithmetic on hash fields |
| HINCRBYFLOAT | P0 | Float arithmetic on hash fields |
| HMGET | P0 | Multi-field GET |
| HMSET | P1 | Deprecated but clients still use it |
| HSETNX | P0 | Conditional field set |
| HGETDEL | P0 | Redis 8.0 — GET and DELETE atomically |
| HGETEX | P0 | Redis 8.0 — GET and set expiration |
| HSETEX | P0 | Redis 8.0 — SET with expiration |
| HSCAN (NOVALUES) | P1 | NOVALUES option for HSCAN |

### 2.2 Sorted Set commands (3 missing)

| Command | Priority | Notes |
|---------|----------|-------|
| ZRANGESTORE | P0 | Store range result in destination key |
| ZINTERCARD | P0 | Intersection cardinality with LIMIT |
| ZRANGE unified | P1 | Ensure BYSCORE, BYLEX, REV, LIMIT all work in unified ZRANGE |

### 2.3 Stream commands (10 missing)

| Command | Priority | Notes |
|---------|----------|-------|
| XGROUP CREATECONSUMER | P0 | Explicit consumer creation |
| XGROUP DELCONSUMER | P0 | Delete consumer from group |
| XINFO CONSUMERS | P0 | List consumers in a group |
| XINFO GROUPS | P0 | List all groups on a stream |
| XSETID | P1 | Set stream last-delivered-id |
| XACKDEL | P1 | Redis 8.2 — ACK + DELETE atomically |
| XDELEX | P1 | Redis 8.2 — DELETE with consumer group ref handling |
| XCFGSET | P2 | Redis 8.6 — Configure stream settings |
| XREAD BLOCK | P0 | True blocking XREAD (not immediate-return) |
| XREADGROUP BLOCK | P0 | True blocking XREADGROUP |

### 2.4 Bitmap commands (1 missing)

| Command | Priority | Notes |
|---------|----------|-------|
| BITPOS | P0 | Find first set/clear bit position |

### 2.5 Geospatial commands (4 missing)

| Command | Priority | Notes |
|---------|----------|-------|
| GEOSEARCH BYBOX | P0 | Box-based geospatial query |
| GEOSEARCHSTORE | P0 | Store GEOSEARCH results |
| GEORADIUS_RO | P1 | Deprecated but used by read-only replicas |
| GEORADIUSBYMEMBER_RO | P1 | Deprecated but used by read-only replicas |

### 2.6 Generic Key commands (15 missing)

| Command | Priority | Notes |
|---------|----------|-------|
| SORT | P0 | Sort list/set/zset with BY, GET, LIMIT, ALPHA, STORE |
| SORT_RO | P1 | Read-only SORT (Redis 7.0) |
| WAIT (full) | P0 | Currently basic — needs proper replica ACK tracking |
| WAITAOF | P1 | Wait for AOF fsync acknowledgment (Redis 7.2) |
| OBJECT HELP | P1 | Help text for OBJECT |
| DELEX | P2 | Redis 8.4 — Conditional delete |
| DIGEST | P2 | Redis 8.4 — Cryptographic digest |
| SUBSTR | P1 | Alias for GETRANGE |

### 2.7 True blocking command semantics

Currently BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP use immediate-return. For 1.0, implement proper event-loop-based blocking with timeout support.

**Estimated iterations: 8-10**

---

## 3. Phase 2 — Lua Scripting Engine

Replace stub EVAL/EVALSHA with a real embedded Lua interpreter.

### Requirements

| Feature | Description |
|---------|-------------|
| Lua 5.1 interpreter | Embed via Zig C interop (link against liblua5.1) |
| redis.call() | Execute Redis commands from Lua, propagate errors |
| redis.pcall() | Execute commands, catch errors as Lua tables |
| redis.log() | Log from scripts at configurable levels |
| redis.error_reply() | Return custom error from script |
| redis.status_reply() | Return custom status from script |
| KEYS/ARGV tables | Pass keys and arguments to scripts |
| Atomicity | No other commands execute during script |
| Sandboxing | Block global variables, restrict OS/IO access |
| Available libraries | cjson, cmsgpack, struct, bit, math, string, table |
| SHA1 caching | Already implemented — connect to real execution |
| Script timeout | lua-time-limit config parameter (default 5000ms) |
| SCRIPT KILL | Kill long-running script |
| SCRIPT DEBUG | Script debugging mode (YES, SYNC, NO) |
| EVAL_RO / EVALSHA_RO | Read-only script execution (Redis 7.0) |
| Replication | Replicate EVAL/EVALSHA to replicas |

### Implementation approach

1. Add liblua5.1 as a Zig C dependency via `build.zig.zon`
2. Create `src/scripting/lua_engine.zig` — Lua state management
3. Create `src/scripting/redis_api.zig` — redis.call/pcall bridge
4. Wire EVAL/EVALSHA in `src/commands/scripting.zig` to real engine
5. Implement sandbox (remove dangerous globals, restrict loadlib)
6. Add cjson/cmsgpack/struct libraries
7. Add script timeout + SCRIPT KILL support

**Estimated iterations: 5-6**

---

## 4. Phase 3 — ACL Enforcement

Replace ACL stubs with real authentication and authorization.

### Requirements

| Feature | Description |
|---------|-------------|
| User management | Create/modify/delete users with ACL SETUSER |
| Password auth | SHA256-hashed passwords, multiple passwords per user |
| AUTH command | Full AUTH username password (Redis 6.0+) |
| Command permissions | Per-command allow/deny with +/- prefixes |
| Category permissions | +@category / -@category (30+ categories) |
| Key permissions | ~pattern and %R~pattern / %W~pattern |
| Channel permissions | &pattern for pub/sub channel access |
| ACL selectors | ACLv2 multi-rule selectors (Redis 7.0) |
| Default user | Configurable default user permissions |
| ACL file | Load/save ACL rules from aclfile |
| ACL LOG | Track denied command attempts |
| ACL DRYRUN | Simulate command execution for a user |
| ACL GENPASS | Cryptographically random password generation |
| Per-connection state | Track authenticated user per connection |
| requirepass config | Legacy single-password compatibility |

### Missing ACL commands

| Command | Notes |
|---------|-------|
| ACL DRYRUN | Simulate without executing |
| ACL GENPASS | Random password generation |
| ACL LOAD | Reload from ACL file |
| ACL SAVE | Save to ACL file |
| ACL LOG | Security event log |

**Estimated iterations: 4-5**

---

## 5. Phase 4 — Full Pub/Sub

### Currently implemented
SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PUBSUB CHANNELS, PUBSUB NUMSUB

### Missing

| Command | Priority | Notes |
|---------|----------|-------|
| PSUBSCRIBE | P0 | Pattern-based subscription (glob patterns) |
| PUNSUBSCRIBE | P0 | Unsubscribe from patterns |
| PUBSUB NUMPAT | P0 | Count active pattern subscriptions |
| PUBSUB HELP | P1 | Help text |
| SSUBSCRIBE | P1 | Sharded pub/sub (Redis 7.0) — for cluster mode |
| SUNSUBSCRIBE | P1 | Unsubscribe from shard channels |
| SPUBLISH | P1 | Publish to shard channel |
| PUBSUB SHARDCHANNELS | P1 | List active shard channels |
| PUBSUB SHARDNUMSUB | P1 | Count shard channel subscribers |

**Estimated iterations: 2-3**

---

## 6. Phase 5 — Full Client/Connection Commands

### Currently implemented
CLIENT ID, CLIENT GETNAME, CLIENT SETNAME, CLIENT LIST, ECHO, HELLO, PING, QUIT, SELECT

### Missing

| Command | Priority | Notes |
|---------|----------|-------|
| AUTH | P0 | Requires Phase 3 (ACL) |
| RESET | P0 | Reset connection state to clean |
| CLIENT INFO | P0 | Current client info string |
| CLIENT KILL | P0 | Kill connections by ID/ADDR/USER/LADDR/MAXAGE |
| CLIENT PAUSE | P1 | Pause all clients (ALL or WRITE mode) |
| CLIENT UNPAUSE | P1 | Resume paused clients |
| CLIENT UNBLOCK | P1 | Unblock blocked client (TIMEOUT/ERROR) |
| CLIENT NO-EVICT | P1 | Set no-eviction mode for client |
| CLIENT NO-TOUCH | P2 | Prevent LRU/LFU touch (Redis 7.2) |
| CLIENT REPLY | P2 | Set reply mode (ON/OFF/SKIP) |
| CLIENT SETINFO | P2 | Set LIB-NAME / LIB-VER (Redis 7.2) |
| CLIENT CACHING | P1 | Control tracking cache for OPTIN/OPTOUT |
| CLIENT TRACKING | P1 | Server-assisted client-side caching |
| CLIENT TRACKINGINFO | P1 | Get tracking status |
| CLIENT GETREDIR | P2 | Get tracking redirect client ID |
| CLIENT HELP | P2 | Help text |

**Estimated iterations: 3-4**

---

## 7. Phase 6 — Server Management

Replace all stubs with real implementations.

### 7.1 SLOWLOG (real implementation)

- Track commands exceeding `slowlog-log-slower-than` threshold
- Ring buffer of configurable size (`slowlog-max-len`)
- Record: ID, timestamp, duration, command+args, client info
- SLOWLOG GET [count], SLOWLOG LEN, SLOWLOG RESET, SLOWLOG HELP

### 7.2 MONITOR (real implementation)

- Stream all commands to MONITOR clients in real-time
- Format: `+<timestamp> [<db> <addr>] "<command>" "<arg1>" ...`
- Properly handle multiple concurrent MONITOR connections

### 7.3 LATENCY monitoring

| Command | Notes |
|---------|-------|
| LATENCY LATEST | Latest samples for all event types |
| LATENCY HISTORY | Time series for specific event |
| LATENCY RESET | Reset events |
| LATENCY GRAPH | ASCII art latency graph |
| LATENCY HISTOGRAM | Per-command cumulative distribution (Redis 7.0) |
| LATENCY DOCTOR | Automated analysis with recommendations |
| LATENCY HELP | Help text |

Event types to track: command, fast-command, fork, rdb-unlink-temp-file, aof-write, aof-fsync-always, aof-write-pending-fsync, aof-rewrite-diff-write, expire-cycle, eviction-cycle, eviction-del

### 7.4 MEMORY (real implementation)

| Command | Notes |
|---------|-------|
| MEMORY STATS | Real allocator stats from Zig's GeneralPurposeAllocator |
| MEMORY USAGE | Accurate per-key memory estimation |
| MEMORY DOCTOR | Real memory advice based on usage patterns |
| MEMORY MALLOC-STATS | Allocator internal statistics |
| MEMORY PURGE | Release unused memory pages |
| MEMORY HELP | Help text |

### 7.5 DEBUG (expanded)

| Command | Notes |
|---------|-------|
| DEBUG SET-ACTIVE-EXPIRE | Toggle active expiration |
| DEBUG SLEEP | Sleep for N seconds |
| DEBUG RELOAD | Save + reload RDB |
| DEBUG CHANGE-REPL-ID | Force new replication ID |
| DEBUG QUICKLIST-PACKED-THRESHOLD | Set encoding threshold |
| DEBUG POPULATE | Fill DB with test keys |
| DEBUG GETKEYS | Extract keys from command |
| DEBUG JMAP | JeMalloc stats |

### 7.6 Other server commands

| Command | Notes |
|---------|-------|
| SHUTDOWN (real) | Graceful shutdown with NOSAVE/SAVE/NOW/FORCE |
| FAILOVER | Coordinated primary-to-replica failover |
| ROLE | Return replication role (master/slave/sentinel) |
| SLAVEOF | Deprecated alias for REPLICAOF |
| LOLWUT | Display version art |
| COMMAND DOCS | Command documentation (Redis 7.0) |
| COMMAND GETKEYSANDFLAGS | Keys and access flags (Redis 7.0) |
| MODULE HELP/LIST/LOAD/LOADEX/UNLOAD | Module management stubs (until Phase 17) |
| HOTKEYS GET/RESET/START/STOP | Redis 8.x hot key tracking |

### 7.7 INFO command sections

Ensure all standard sections return accurate data:
server, clients, memory, persistence, stats, replication, cpu, modules, commandstats, errorstats, latencystats, keyspace, cluster, all, everything, default

### 7.8 CONFIG parameters

Expand from 10 to full coverage (~150+ parameters). Priority parameters:

| Parameter | Notes |
|-----------|-------|
| slowlog-log-slower-than | For SLOWLOG |
| slowlog-max-len | For SLOWLOG |
| requirepass | Legacy auth |
| aclfile | ACL file path |
| lua-time-limit | Script timeout |
| maxclients | Connection limit |
| hz | Active expire frequency |
| lfu-log-factor | LFU tuning |
| lfu-decay-time | LFU decay |
| notify-keyspace-events | Keyspace notifications |
| list-max-ziplist-size | Encoding thresholds |
| zset-max-ziplist-entries | Encoding thresholds |
| hash-max-ziplist-entries | Encoding thresholds |
| set-max-intset-entries | Encoding thresholds |
| activedefrag | Active defragmentation |
| lazyfree-lazy-eviction | Async eviction |
| lazyfree-lazy-expire | Async expiry cleanup |
| lazyfree-lazy-server-del | Async server-side DEL |
| replica-lazy-flush | Async FLUSHALL on replica |
| repl-backlog-size | Replication backlog size |
| repl-backlog-ttl | Backlog TTL after disconnect |
| min-replicas-to-write | Write quorum |
| min-replicas-max-lag | Max replica lag |

**Estimated iterations: 6-8**

---

## 8. Phase 7 — Multi-Database Support

### Requirements

- Support configurable number of databases (default: 16, max: 16384)
- SELECT <index> switches between databases
- SWAPDB <db1> <db2> atomically swaps two databases
- MOVE <key> <db> moves key between databases
- Per-database keyspace in INFO
- Per-database FLUSHDB
- DBSIZE per selected database
- COPY ... DB <target-db> cross-database copy

### Architecture change

Replace single `MemoryStorage` with `[databases]MemoryStorage` array. Track selected DB per connection.

**Estimated iterations: 2-3**

---

## 9. Phase 8 — Cluster Mode

Full Redis Cluster implementation for horizontal scaling.

### 9.1 Core cluster architecture

| Feature | Description |
|---------|-------------|
| Hash slot assignment | 16,384 slots distributed across nodes |
| CRC16 hashing | Already implemented — wire to actual slot routing |
| Hash tags | {tag} support for multi-key co-location |
| Gossip protocol | PING/PONG heartbeat for failure detection |
| Cluster bus | Dedicated port (port + 10000) for node communication |
| Configuration epoch | Monotonic epoch for configuration ordering |
| Cluster config file | nodes.conf auto-save/load |

### 9.2 Redirections

| Feature | Description |
|---------|-------------|
| MOVED redirect | Client redirect to correct node |
| ASK redirect | Temporary redirect during migration |
| ASKING command | Client acknowledges ASK redirect |
| Smart client support | Slot map publication for client-side routing |

### 9.3 Slot management

| Command | Description |
|---------|-------------|
| CLUSTER ADDSLOTS | Assign slots to current node |
| CLUSTER ADDSLOTSRANGE | Assign slot ranges |
| CLUSTER DELSLOTS | Remove slot assignment |
| CLUSTER DELSLOTSRANGE | Remove slot ranges |
| CLUSTER FLUSHSLOTS | Remove all assignments |
| CLUSTER SETSLOT | IMPORTING/MIGRATING/STABLE/NODE |
| CLUSTER MIGRATION | Atomic slot migration (Redis 8.4) |
| MIGRATE | Migrate key(s) between nodes |

### 9.4 Failover

| Feature | Description |
|---------|-------------|
| Automatic failover | pfail → fail → election → promotion |
| Manual failover | CLUSTER FAILOVER [FORCE/TAKEOVER] |
| Replica migration | Auto-balance replicas across masters |
| CLUSTER RESET | HARD/SOFT reset |

### 9.5 Cluster info commands

| Command | Description |
|---------|-------------|
| CLUSTER INFO | Full cluster state |
| CLUSTER NODES | Node topology |
| CLUSTER SHARDS | Shard info (Redis 7.0, replaces SLOTS) |
| CLUSTER SLOTS | Deprecated but needed |
| CLUSTER MYID | Node ID |
| CLUSTER MYSHARDID | Shard ID (Redis 7.2) |
| CLUSTER LINKS | Bus connections (Redis 7.0) |
| CLUSTER REPLICAS | List replicas |
| CLUSTER REPLICATE | Configure as replica |
| CLUSTER SLOT-STATS | Per-slot metrics (Redis 8.2) |
| CLUSTER COUNT-FAILURE-REPORTS | Failure report count |
| CLUSTER COUNTKEYSINSLOT | Real key count per slot |
| CLUSTER GETKEYSINSLOT | Real key retrieval per slot |
| CLUSTER SAVECONFIG | Save config to disk |
| CLUSTER SET-CONFIG-EPOCH | Set epoch |
| CLUSTER BUMPEPOCH | Advance epoch |
| CLUSTER MEET | Join node to cluster |
| CLUSTER FORGET | Remove node |
| READONLY / READWRITE | Replica read routing |

### 9.6 Sharded Pub/Sub

Depends on Phase 4 pub/sub completion. SPUBLISH/SSUBSCRIBE/SUNSUBSCRIBE route messages within shard boundaries.

**Estimated iterations: 12-15**

---

## 10. Phase 9 — Redis Sentinel

High-availability monitoring and automatic failover.

### Requirements

| Feature | Description |
|---------|-------------|
| Monitoring | Track master/replica health via PING |
| Notification | Pub/Sub alerts for state changes |
| Automatic failover | Promote replica when master is unreachable |
| Configuration provider | Clients query Sentinel for current master |
| Quorum | Configurable quorum for failure agreement |
| SENTINEL commands | 19 subcommands |

### SENTINEL subcommands

SENTINEL MASTERS, SENTINEL MASTER <name>, SENTINEL REPLICAS <name>, SENTINEL SENTINELS <name>, SENTINEL GET-MASTER-ADDR-BY-NAME <name>, SENTINEL IS-MASTER-DOWN-BY-ADDR, SENTINEL RESET <pattern>, SENTINEL FAILOVER <name>, SENTINEL CKQUORUM <name>, SENTINEL FLUSHCONFIG, SENTINEL MONITOR <name> <ip> <port> <quorum>, SENTINEL REMOVE <name>, SENTINEL SET <name> <option> <value>, SENTINEL CONFIG GET <param>, SENTINEL CONFIG SET <param> <value>, SENTINEL SIMULATE-FAILURE, SENTINEL MYID, SENTINEL PENDING-SCRIPTS, SENTINEL DEBUG

### Architecture

Sentinel runs as a separate mode (`zoltraak --sentinel`). Requires separate TCP listener, Sentinel-specific state machine, and leader election via Raft-like protocol.

**Estimated iterations: 8-10**

---

## 11. Phase 10 — TLS/SSL

### Requirements

| Feature | Description |
|---------|-------------|
| TLS 1.2+ | Minimum TLS version support |
| Certificate auth | Server cert, client cert, CA cert |
| Configuration | 20 TLS config parameters |
| Cluster bus TLS | Encrypted inter-node communication |
| Replication TLS | Encrypted primary-replica sync |

### Config parameters

tls-port, tls-cert-file, tls-key-file, tls-key-file-pass, tls-ca-cert-file, tls-ca-cert-dir, tls-auth-clients, tls-replication, tls-cluster, tls-protocols, tls-ciphers, tls-ciphersuites, tls-prefer-server-ciphers, tls-session-caching, tls-session-cache-size, tls-session-cache-timeout, tls-client-cert-file, tls-client-key-file, tls-client-key-file-pass, tls-allowlisted-certs

### Implementation approach

Use Zig's `std.crypto.tls` or link against OpenSSL/BoringSSL via C interop.

**Estimated iterations: 3-4**

---

## 12. Phase 11 — Redis Functions (7.0+)

Extend scripting beyond EVAL with the Functions API.

### Commands

| Command | Description |
|---------|-------------|
| FUNCTION LOAD | Create/replace function library (REPLACE flag) |
| FUNCTION CALL / FCALL | Call a function |
| FUNCTION CALL_RO / FCALL_RO | Call read-only function |
| FUNCTION DELETE | Delete a library |
| FUNCTION DUMP | Serialize all libraries |
| FUNCTION FLUSH | Delete all libraries (ASYNC/SYNC) |
| FUNCTION HELP | Help text |
| FUNCTION KILL | Kill running function |
| FUNCTION LIST | List libraries (LIBRARYNAME, WITHCODE) |
| FUNCTION RESTORE | Restore libraries (FLUSH/APPEND/REPLACE) |
| FUNCTION STATS | Running function info |

### Requirements

- Library-based organization (multiple functions per library)
- Function flags: no-writes, allow-oom, allow-stale, no-cluster, allow-cross-slot-keys, raw-arguments
- Shared Lua engine with Phase 2
- Replication of FUNCTION LOAD/DELETE/FLUSH

**Estimated iterations: 3-4**

---

## 13. Phase 12 — JSON Data Type

Native JSON document storage and manipulation (built-in since Redis 8.0).

### Commands (26)

JSON.SET, JSON.GET, JSON.MGET, JSON.MSET, JSON.DEL, JSON.FORGET, JSON.TYPE, JSON.NUMINCRBY, JSON.NUMMULTBY, JSON.STRAPPEND, JSON.STRLEN, JSON.TOGGLE, JSON.CLEAR, JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM, JSON.OBJKEYS, JSON.OBJLEN, JSON.RESP, JSON.MERGE, JSON.DEBUG, JSON.DEBUG HELP, JSON.DEBUG MEMORY

### Requirements

- JSONPath query syntax (both legacy `.path` and RFC 9535 `$..path`)
- Atomic sub-document operations
- Memory-efficient internal representation
- Integration with FT.SEARCH indexing (Phase 13)
- RDB/AOF persistence of JSON values
- RESP3-aware responses

### Implementation approach

1. Add JSON data type variant to storage tagged union
2. Implement JSONPath parser and evaluator
3. Add 26 command handlers in `src/commands/json.zig`
4. Integrate with persistence layer

**Estimated iterations: 5-6**

---

## 14. Phase 13 — Search Engine (FT)

Full-text search and secondary indexing (built-in since Redis 8.0).

### Commands (30)

FT.CREATE, FT.SEARCH, FT.AGGREGATE, FT.INFO, FT.DROPINDEX, FT.ALTER, FT._LIST, FT.EXPLAIN, FT.EXPLAINCLI, FT.PROFILE, FT.SPELLCHECK, FT.CURSOR READ, FT.CURSOR DEL, FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE, FT.DICTADD, FT.DICTDEL, FT.DICTDUMP, FT.SYNDUMP, FT.SYNUPDATE, FT.SUGADD, FT.SUGDEL, FT.SUGGET, FT.SUGLEN, FT.TAGVALS, FT.CONFIG GET, FT.CONFIG SET, FT.CONFIG HELP, FT.HYBRID

### Requirements

- Index types: HASH, JSON
- Field types: TEXT, TAG, NUMERIC, GEO, VECTOR, GEOSHAPE
- Full-text: Stemming, stop words, phonetic matching, synonyms
- Query syntax: Boolean operators, field specifiers, ranges, wildcards
- Aggregation pipeline: GROUPBY, SORTBY, APPLY, FILTER, LIMIT, REDUCE
- Scoring: TF-IDF, BM25, custom scoring
- Auto-complete: Suggestion dictionaries with fuzzy matching
- Vector similarity: KNN and hybrid search (Redis 8.4 FT.HYBRID)
- Cursor-based result pagination

### Implementation approach

This is the largest single feature. Consider:
1. Inverted index data structure for TEXT fields
2. B-tree or skip list for NUMERIC/GEO range queries
3. HNSW graph for vector similarity
4. Query parser and execution engine
5. Aggregation pipeline executor

**Estimated iterations: 15-20**

---

## 15. Phase 14 — Time Series

Time series data type (built-in since Redis 8.0).

### Commands (17)

TS.CREATE, TS.ALTER, TS.ADD, TS.MADD, TS.INCRBY, TS.DECRBY, TS.DEL, TS.GET, TS.MGET, TS.RANGE, TS.REVRANGE, TS.MRANGE, TS.MREVRANGE, TS.QUERYINDEX, TS.INFO, TS.CREATERULE, TS.DELETERULE

### Requirements

- Configurable retention policy
- Downsampling with compaction rules (avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s, twa)
- Labels for filtering (key=value pairs)
- Duplicate policy: BLOCK, FIRST, LAST, MIN, MAX, SUM
- Chunk encoding: COMPRESSED (Gorilla), UNCOMPRESSED
- Cross-key queries with label-based filtering

**Estimated iterations: 5-6**

---

## 16. Phase 15 — Probabilistic Data Structures

All built-in since Redis 8.0.

### 16.1 Bloom Filter (10 commands)

BF.RESERVE, BF.ADD, BF.MADD, BF.EXISTS, BF.MEXISTS, BF.INSERT, BF.INFO, BF.CARD, BF.SCANDUMP, BF.LOADCHUNK

Implementation: Standard Bloom filter with configurable error rate and capacity. Auto-scaling sub-filters.

### 16.2 Cuckoo Filter (12 commands)

CF.RESERVE, CF.ADD, CF.ADDNX, CF.INSERT, CF.INSERTNX, CF.EXISTS, CF.MEXISTS, CF.DEL, CF.COUNT, CF.INFO, CF.SCANDUMP, CF.LOADCHUNK

Implementation: Cuckoo hashing with fingerprints. Supports deletion (unlike Bloom).

### 16.3 Count-Min Sketch (6 commands)

CMS.INITBYDIM, CMS.INITBYPROB, CMS.INCRBY, CMS.QUERY, CMS.MERGE, CMS.INFO

Implementation: 2D counter matrix with multiple hash functions.

### 16.4 T-Digest (14 commands)

TDIGEST.CREATE, TDIGEST.ADD, TDIGEST.MERGE, TDIGEST.RESET, TDIGEST.QUANTILE, TDIGEST.CDF, TDIGEST.RANK, TDIGEST.REVRANK, TDIGEST.BYRANK, TDIGEST.BYREVRANK, TDIGEST.MIN, TDIGEST.MAX, TDIGEST.TRIMMED_MEAN, TDIGEST.INFO

Implementation: Merging t-digest algorithm for streaming quantile estimation.

### 16.5 TopK (7 commands)

TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY, TOPK.QUERY, TOPK.COUNT, TOPK.LIST, TOPK.INFO

Implementation: Heavy Keeper algorithm for top-K frequent items.

**Estimated iterations: 8-10**

---

## 17. Phase 16 — Vector Sets

Vector similarity search (Redis 8.0+ beta).

### Commands (13)

VADD, VCARD, VDIM, VEMB, VGETATTR, VINFO, VISMEMBER, VLINKS, VRANDMEMBER, VRANGE, VREM, VSETATTR, VSIM

### Requirements

- Distance metrics: L2 (Euclidean), IP (Inner Product), COSINE
- HNSW graph for approximate nearest neighbor search
- Attribute filtering on search
- Variable dimensionality per vector set
- Quantization support (FP32, FP16, INT8)
- SIMD acceleration (AVX2/AVX512 on x86, NEON on ARM)

### Implementation approach

1. HNSW graph implementation in Zig
2. SIMD-accelerated distance calculations
3. Attribute storage and filtering
4. Integration with persistence layer

**Estimated iterations: 6-8**

---

## 18. Phase 17 — Modules API

Allow loading custom C/Zig modules at runtime.

### Requirements

| Feature | Description |
|---------|-------------|
| Module loading | Load .so/.dylib at runtime |
| Custom commands | Register new commands with key specs |
| Custom data types | Register new data types with RDB serialization |
| Module API | RedisModule_* function set |
| Blocking commands | Module-level blocking command support |
| Hooks & notifications | Key miss, key change, cron, etc. |
| Thread-safe contexts | Background thread command execution |
| Timers | Periodic callback registration |
| Memory accounting | Module memory tracking |
| MODULE commands | LOAD, LOADEX, UNLOAD, LIST, HELP |

### Implementation approach

Define a C ABI-compatible header (`zoltraak_module.h`) and implement the RedisModule_* function table. Use Zig's `@cImport` and dynamic library loading.

**Estimated iterations: 8-10**

---

## 19. Phase 18 — Advanced Features & Polish

### 19.1 Client-side caching (server-assisted)

- CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
- RESP3 push invalidation messages
- Tracking table with configurable size
- CLIENT CACHING YES|NO for OPTIN/OPTOUT
- CLIENT TRACKINGINFO

### 19.2 Keyspace notifications

- CONFIG SET notify-keyspace-events
- All event flags: K, E, g, $, l, s, h, z, t, d, x, e, m, n, o, c, A
- Publish events to `__keyevent@<db>__:<event>` and `__keyspace@<db>__:<key>` channels

### 19.3 Eviction policies (real implementation)

Currently `maxmemory-policy` is config-only. Implement all 8 policies:
- noeviction, allkeys-lru, volatile-lru, allkeys-lfu, volatile-lfu, allkeys-random, volatile-random, volatile-ttl

Requires LRU clock and LFU counter tracking per key.

### 19.4 Lazy freeing

- lazyfree-lazy-eviction, lazyfree-lazy-expire, lazyfree-lazy-server-del, lazyfree-lazy-user-del, replica-lazy-flush
- FLUSHALL ASYNC / FLUSHDB ASYNC
- UNLINK async deletion (currently same as DEL)

### 19.5 Active defragmentation

- activedefrag config parameter
- Background memory defragmentation using Zig allocator

### 19.6 Internal encoding optimizations

Redis uses compact encodings for small values:
- ziplist / listpack for small lists, hashes, sorted sets
- intset for small integer-only sets
- embstr for short strings
- Encoding transitions at configurable thresholds

### 19.7 Redis 8.x new commands

| Command | Version | Notes |
|---------|---------|-------|
| HGETDEL | 8.0 | Covered in Phase 1 |
| HGETEX | 8.0 | Covered in Phase 1 |
| HSETEX | 8.0 | Covered in Phase 1 |
| MSETEX | 8.4 | Already implemented |
| DELEX | 8.4 | Conditional delete |
| DIGEST | 8.4 | Key digest |
| XACKDEL | 8.2 | Covered in Phase 1 |
| XDELEX | 8.2 | Covered in Phase 1 |
| XCFGSET | 8.6 | Stream config |
| BITOP DIFF/DIFF1/ANDOR/ONE | 8.2 | Extended bitwise ops |
| CLUSTER SLOT-STATS | 8.2 | Covered in Phase 8 |
| CLUSTER MIGRATION | 8.4 | Covered in Phase 8 |
| HOTKEYS | 8.x | Covered in Phase 6 |

### 19.8 Deprecated command aliases

Ensure all deprecated commands work as aliases:
GETSET → SET with GET, SETEX/PSETEX → SET EX/PX, SETNX → SET NX, HMSET → HSET, RPOPLPUSH → LMOVE, BRPOPLPUSH → BLMOVE, GEORADIUS → GEOSEARCH, GEORADIUSBYMEMBER → GEOSEARCH, ZRANGEBYSCORE → ZRANGE BYSCORE, ZREVRANGEBYSCORE → ZRANGE BYSCORE REV, ZRANGEBYLEX → ZRANGE BYLEX, ZREVRANGEBYLEX → ZRANGE BYLEX REV, ZREVRANGE → ZRANGE REV, CLUSTER SLOTS → CLUSTER SHARDS, CLUSTER SLAVES → CLUSTER REPLICAS, SLAVEOF → REPLICAOF, QUIT → (just close), SUBSTR → GETRANGE

**Estimated iterations: 8-10**

---

## 20. Non-Functional Requirements

### Performance

| Metric | Target |
|--------|--------|
| Throughput | >= 80% of Redis on equivalent hardware |
| Latency p50 | <= 1.2x Redis |
| Latency p99 | <= 1.5x Redis |
| Memory efficiency | <= 1.1x Redis memory for equivalent dataset |
| Connection handling | 10,000+ concurrent connections |

### Compatibility

| Metric | Target |
|--------|--------|
| Protocol | 100% RESP2 + RESP3 byte-compatible |
| Client libraries | All major clients work unmodified (redis-py, ioredis, go-redis, Jedis, redis-rb, Lettuce, StackExchange.Redis) |
| redis-cli | Full compatibility |
| redis-benchmark | Full compatibility |
| Command behavior | Byte-for-byte response matching with Redis 8.x |

### Testing

| Metric | Target |
|--------|--------|
| Unit test coverage | >= 90% of command handlers |
| Integration tests | Every command with edge cases |
| Redis compatibility tests | Differential testing against real Redis for every command |
| Cluster tests | Multi-node integration tests |
| Sentinel tests | Failover scenario tests |
| Performance regression | Automated benchmarking on every release |

### Platform support

| Platform | Status |
|----------|--------|
| Linux x86_64 | Primary target |
| Linux aarch64 | Primary target |
| macOS x86_64 | Supported |
| macOS aarch64 (Apple Silicon) | Supported |
| Windows x86_64 | Best effort |

---

## 21. Release Criteria

Zoltraak 1.0 will ship when ALL of the following are met:

### Must-have (blocks release)

- [ ] All 500+ Redis commands implemented with correct behavior
- [ ] Full Lua 5.1 scripting engine (EVAL/EVALSHA/EVAL_RO/EVALSHA_RO)
- [ ] Full Redis Functions support (FUNCTION LOAD/CALL/DELETE/...)
- [ ] ACL enforcement with AUTH, user management, per-command permissions
- [ ] Full Pub/Sub including pattern subscriptions and sharded pub/sub
- [ ] Multi-database support (SELECT 0-15)
- [ ] All CLIENT subcommands
- [ ] Real SLOWLOG, MONITOR, LATENCY, MEMORY implementations
- [ ] Real blocking command semantics (BLPOP, BRPOP, etc.)
- [ ] All 8 eviction policies implemented
- [ ] TLS/SSL support
- [ ] JSON data type (26 commands)
- [ ] Full-text search engine (30 FT.* commands)
- [ ] Time Series (17 TS.* commands)
- [ ] Probabilistic data structures (BF, CF, CMS, T-Digest, TopK — 49 commands)
- [ ] Vector Sets (13 V* commands)
- [ ] Cluster mode with hash slot routing, failover, and migration
- [ ] Redis Sentinel for high availability
- [ ] Keyspace notifications
- [ ] Client-side caching support
- [ ] >= 95% compatibility score in differential testing vs Redis 8.x
- [ ] >= 80% of Redis throughput on redis-benchmark
- [ ] All major client libraries verified working
- [ ] RDB/AOF persistence for all data types including JSON, TS, BF, etc.

### Should-have (target but won't block)

- [ ] Modules API for custom data types and commands
- [ ] Active defragmentation
- [ ] Internal encoding optimizations (listpack, intset, embstr)
- [ ] SIMD-accelerated vector operations
- [ ] >= 90% of Redis throughput

### Total estimated iterations: 95-130 (Iterations 51-180+)

---

## Appendix: Phase Dependency Graph

```
Phase 1 (Core gaps) ─────────────────────────────────────────┐
Phase 2 (Lua scripting) ─────────────────┐                   │
Phase 3 (ACL) ───────────────────────────┤                   │
Phase 4 (Pub/Sub) ───────────────────────┤                   │
Phase 5 (Client commands) ← Phase 3 ────┤                   │
Phase 6 (Server mgmt) ──────────────────┤                   │
Phase 7 (Multi-DB) ─────────────────────┤                   │
                                         ├→ Phase 8 (Cluster) ← Phase 4
Phase 9 (Sentinel) ← Phase 8 ──────────┤
Phase 10 (TLS) ─────────────────────────┤
Phase 11 (Functions) ← Phase 2 ────────┤
Phase 12 (JSON) ────────────────────────┤
Phase 13 (Search) ← Phase 12 ──────────┤
Phase 14 (Time Series) ────────────────┤
Phase 15 (Probabilistic DS) ───────────┤
Phase 16 (Vector Sets) ────────────────┤
Phase 17 (Modules API) ← Phase 8 ─────┤
Phase 18 (Polish) ← All ──────────────┘
```

Phases 1-7 can largely proceed in parallel. Phase 8 (Cluster) depends on Phases 1, 4. Phase 9 (Sentinel) depends on Phase 8. Phase 11 (Functions) depends on Phase 2. Phase 13 (Search) depends on Phase 12 (JSON). Phase 18 is final polish after all others complete.
