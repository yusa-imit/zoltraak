# CLAUDE.md

Zoltraak — Redis-compatible in-memory data store written in Zig.

## Project Status

**Current: v0.1.0 — Iterations 1-93 complete (187+ Redis commands)**
**Target: v1.0 — 100% Redis compatibility (500+ commands)**
**Roadmap: [docs/PRD.md](docs/PRD.md)**

### Completed (Iterations 1-90)

| Range | Features |
|-------|----------|
| 1-5 | Core data structures: Strings, Lists, Sets, Hashes, Sorted Sets |
| 6-7 | Persistence: RDB snapshots, AOF logging |
| 8, 62 | Pub/Sub (SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PUBSUB) + pattern subscriptions (PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB NUMPAT/HELP) |
| 9 | Transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH) |
| 10 | Replication (REPLICAOF, REPLCONF, PSYNC, WAIT) |
| 11-12 | Extended Set/SortedSet commands, SCAN family, OBJECT |
| 13-15 | CLIENT, CONFIG, COMMAND introspection |
| 16-17, 24, 27-28 | Streams + consumer groups (XADD through XAUTOCLAIM) |
| 18-19 | Extended String/List commands (LPOS, LMOVE, LCS, MGET, etc.) |
| 20, 44 | Bit operations (SETBIT through BITFIELD_RO) |
| 21, 23 | Key management (TTL, EXPIRE, DUMP, RESTORE, COPY, TOUCH) |
| 25 | Geospatial (GEOADD, GEOPOS, GEODIST, GEOHASH, GEORADIUS, GEOSEARCH) |
| 26 | HyperLogLog (PFADD, PFCOUNT, PFMERGE) |
| 29-30 | Server introspection (MEMORY, SLOWLOG stubs, INFO) |
| 31-35 | RESP3 protocol + per-connection negotiation (HELLO) |
| 36 | Scripting stubs (EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH) |
| 37 | ACL stubs (WHOAMI, LIST, USERS, GETUSER, SETUSER, DELUSER, CAT) |
| 38 | Cluster stubs (SLOTS, NODES, INFO, MYID, KEYSLOT) |
| 39-41 | Utility (ECHO, QUIT, SELECT, SWAPDB, TIME, MONITOR, DEBUG, SHUTDOWN) |
| 42-43 | Extended SortedSet (ZRANGEBYLEX, ZREVRANGEBYLEX, ZINTERCARD, SINTERCARD) |
| 45-47 | MSETEX, LCS, GETRANGE/SETRANGE enhancements |
| 48 | HRANDFIELD |
| 49 | LMPOP, ZMPOP, BZMPOP |
| 50 | Hash field-level TTL (HEXPIRE, HPEXPIRE, HTTL, HPTTL, etc.) |
| 51 | BITPOS |
| 52 | HGETDEL, HGETEX, HSETEX |
| 53 | ZRANGESTORE, ZINTERCARD |
| 54 | XGROUP CREATECONSUMER, XGROUP DELCONSUMER |
| 55 | XINFO CONSUMERS, XINFO GROUPS |
| 56 | GEOSEARCH BYBOX, GEOSEARCHSTORE |
| 57 | SORT |
| 58 | GEORADIUS_RO, GEORADIUSBYMEMBER, GEORADIUSBYMEMBER_RO |
| 59 | XSETID |
| 60 | XACKDEL, XDELEX (atomic ACK+DELETE with PEL ref control) |
| 61 | HMSET (deprecated HSET alias), SORT_RO (read-only SORT) |
| 62 | PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB NUMPAT, PUBSUB HELP (pattern-based pub/sub with * and ? wildcards, pmessage delivery format) |
| 63 | XCFGSET (Redis 8.6+) — configure stream IDMP (Idempotent Message Processing) settings: IDMP-DURATION (1-86400 sec) and IDMP-MAXSIZE (1-10000 entries), stream configuration for at-most-once production guarantees |
| 64 | **XREAD/XREADGROUP BLOCK infrastructure (Phase 1)** — BlockingQueue data structure, BlockedClient tracking, specification document (full event-loop integration pending) |
| 65 | WAITAOF command (Redis 7.2+) — wait for AOF fsync acknowledgment from local Redis and/or replicas, returns array [local_fsynced_count, replicas_fsynced_count], validates numlocal (0 or 1), rejects execution on replica instances, stub implementation (full AOF fsync offset tracking pending) |
| 66 | **XREAD/XREADGROUP BLOCK with polling (Phase 2)** — true blocking semantics using polling approach (checks every 100ms), validates timeout >= 0, XREADGROUP only blocks for ID=">", returns immediately for "0" or specific IDs, completes Phase 2 of blocking implementation |
| 67 | True blocking semantics for list commands — BLPOP/BRPOP/BLMOVE now use polling with 100ms intervals (same approach as XREAD/XREADGROUP BLOCK), validates timeout >= 0, returns null on timeout, completes Phase 1.7 true blocking command semantics for core list operations (BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP remain immediate-return for now) |
| 68 | **True blocking semantics for BLMPOP and sorted set commands** — BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP now use polling with 100ms intervals, validates timeout >= 0, returns null on timeout, **completes Phase 1.7 true blocking semantics for ALL blocking commands** (BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/XREAD BLOCK/XREADGROUP BLOCK) |
| 69 | **Sharded Pub/Sub (Redis 7.0+)** — SSUBSCRIBE (subscribe to sharded channels), SUNSUBSCRIBE (unsubscribe from sharded channels), SPUBLISH (publish to sharded channel), PUBSUB SHARDCHANNELS (list active sharded channels), PUBSUB SHARDNUMSUB (get sharded channel subscriber counts) — cluster-mode ready pub/sub with hash-slot routing, **completes Phase 4 pub/sub feature set (9/9 commands, 100%)** |
| 70-72 | **Sailor library migrations** — v1.5.0 (TUI snapshot testing), v1.6.0 (data visualization widgets), v0.5.0 (advanced TUI widgets with Tree/LineChart/Dialog/Notification) |
| 73 | **Sailor v1.1.0 migration** — Accessibility & Internationalization (Unicode width calculation for CJK/emoji, keyboard navigation, screen reader support, bidirectional text for RTL languages) — critical for multi-language Redis data display in CLI/TUI |
| 74 | **Sailor v1.3.0 migration** — Performance & Developer Experience (RenderBudget for 60fps, LazyBuffer for dirty region tracking, EventBatcher for rapid event coalescing, DebugOverlay for visual debugging, ThemeWatcher for hot-reload) + bugfix: Storage.init() signature in 44 unit tests |
| 75 | **Sailor v1.7.0 migration** — Advanced Layout & Rendering (FlexBox layout with justify/align, Viewport clipping for efficient scrolling, Shadow & 3D border effects, Custom widget traits, Layout caching with LRU) — non-breaking upgrade, all tests pass |
| 76 | **Sailor v1.2.0 migration** — Layout & Composition (Grid layout system for multi-pane UI, ScrollView widget for large content, Overlay/z-index system for popups/tooltips, Widget composition helpers with split panes, Responsive breakpoints for terminal sizes) — non-breaking upgrade, all tests pass |
| 77 | **DIGEST and DELEX commands (Redis 8.4)** — DIGEST returns Wyhash digest (XXH3 placeholder) of string value as hex string for efficient comparison, DELEX conditional delete with IFEQ/IFNE/IFDEQ/IFDNE modes for atomic compare-and-delete, optimistic concurrency control for single-key operations, 10 unit tests, all tests pass |
| 78 | **Client Management Commands (Phase 5)** — RESET (reset connection state, discard MULTI, unsubscribe all, clear name, switch to RESP2), CLIENT INFO (return current client connection info string), CLIENT HELP (help text for CLIENT subcommands) — Phase 5.1 client/connection commands (3 new commands) |
| 79 | **Sailor v1.8.0 migration** — Network & Async Integration (HttpClient widget for download progress, WebSocket widget for live feeds, AsyncEventLoop for non-blocking I/O, TaskRunner for parallel ops, LogViewer for tail -f style logs) — fixed missing Writer.writeArrayOfBulkStrings method, fixed TxState method call, non-breaking upgrade, all tests pass |
| 80 | **CLIENT KILL command (Phase 5)** — CLIENT KILL with full filter support (ID, ADDR, LADDR, USER, TYPE, SKIPME, MAXAGE), both old addr:port and new filter-based syntax, returns OK (old) or killed count (new), logical AND combination of filters, SKIPME defaults to YES (caller skipped), 10 unit tests + 6 integration tests, all tests pass — Phase 5.1 client/connection commands (4/14 P0 commands, 29%) |
| 81 | **CLIENT PAUSE/UNPAUSE commands (Phase 5)** — CLIENT PAUSE <timeout> [WRITE|ALL] (pause clients for timeout milliseconds, WRITE mode pauses only writes, ALL mode pauses all commands), CLIENT UNPAUSE (resume paused clients), pause state tracking in ClientRegistry (pause_until_ms, pause_all), isClientsPaused(is_write) checks pause status with expiration, validates timeout >= 0, 9 unit tests + 7 integration tests, manual testing confirms functionality — Phase 5.1 client/connection commands (6/14 P0 commands, 43%) |
| 82 | **CLIENT UNBLOCK command (Phase 5)** — CLIENT UNBLOCK <client-id> [TIMEOUT|ERROR] (unblock a client blocked in a blocking operation from a different connection), returns 1 if client found and unblock requested, 0 if client not found or not blocked, TIMEOUT mode (default, unblock as if timeout occurred), ERROR mode (return UNBLOCKED error), BlockingQueue extended with unblock_requests map and requestUnblock/checkUnblockRequest methods, 5 unit tests + 5 integration tests, all tests pass — Phase 5.1 client/connection commands (7/14 P0 commands, 50%) |
| 83 | **Sailor v1.9.0 migration** — Developer Tools & Ecosystem (WidgetDebugger for layout inspection, PerformanceProfiler for frame timing, CompletionPopup for REPL tab completion, ThemeEditor for live customization, Widget Gallery catalog), non-breaking upgrade, all tests pass |
| 84 | **CLIENT NO-EVICT and CLIENT REPLY commands (Phase 5)** — CLIENT NO-EVICT [ON|OFF] (control whether client's keys are protected from eviction, returns current status if no argument), CLIENT REPLY ON|OFF|SKIP (control client reply behavior: ON=normal, OFF=suppress all, SKIP=skip next only), ClientInfo extended with reply_mode (ON/OFF/SKIP) and no_evict (bool) fields, ClientRegistry methods: setReplyMode/getReplyMode/processReplySkip/setNoEvict/getNoEvict, 9 unit tests + 9 integration tests, all tests pass — Phase 5.1 client/connection commands (9/14 P0 commands, 64%) |
| 85 | **Sailor v1.10.0 migration** — Mouse & Gamepad Input (mouse event handling with SGR protocol for click/drag/scroll/double-click, widget mouse interaction traits: Clickable/Draggable/Scrollable/Hoverable, gamepad/controller input with buttons/analog sticks/triggers, touch gesture recognition for tap/swipe/pinch/multi-touch, input mapping to remap mouse/gamepad/touch to keyboard events), non-breaking upgrade, all tests pass |
| 86 | **CLIENT NO-TOUCH and CLIENT SETINFO commands (Phase 5)** — CLIENT NO-TOUCH [ON|OFF] (control whether client alters LRU/LFU stats, ON=no updates unless TOUCH sent, OFF=normal updates), CLIENT SETINFO LIB-NAME|LIB-VER <value> (assign library name/version metadata, validates no spaces/newlines/non-printable chars), ClientInfo extended with no_touch/lib_name/lib_ver fields, ClientRegistry methods: setNoTouch/getNoTouch/setLibName/setLibVer, 9 unit tests + 8 integration tests, all tests pass — Phase 5.1 client/connection commands (11/14 P0 commands, 79%), Redis 7.2+ compatibility for library metadata tracking |
| 87 | **CLIENT TRACKING, CLIENT TRACKINGINFO, and CLIENT CACHING commands (Phase 5)** — CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP] (enable/disable server-assisted client-side caching tracking, supports redirect to different client, broadcasting mode with key prefixes, OPTIN/OPTOUT modes for selective tracking, NOLOOP to skip self-modified keys), CLIENT TRACKINGINFO (return tracking status with flags/redirect/prefixes, RESP3 map or RESP2 array format), CLIENT CACHING YES|NO (control tracking for next command in OPTIN/OPTOUT modes), ClientInfo extended with tracking_enabled/tracking_redirect/tracking_bcast/tracking_optin/tracking_optout/tracking_noloop/tracking_next_cache/tracking_prefixes fields, ClientRegistry methods: setTracking/getTrackingInfo/setTrackingNextCache/resetTrackingNextCache, validates OPTIN/OPTOUT mutual exclusivity, validates redirect client ID, 9 unit tests + 6 integration tests, all tests pass — Phase 5.1 client/connection commands (14/14 P0 commands, 100% P0 complete), Redis 6.0+ client-side caching infrastructure |
| 88 | **SLOWLOG real implementation (Phase 6)** — SLOWLOG GET [count] (return slow log entries with ID, timestamp, duration, command, client address, client name, most recent first), SLOWLOG LEN (return slow log length), SLOWLOG RESET (clear slow log), SLOWLOG HELP (help text), SlowLog data structure with ring buffer (configurable max length, microsecond threshold), CONFIG parameters: slowlog-log-slower-than (10000µs default), slowlog-max-len (128 entries default), Storage.slowlog field tracks commands exceeding threshold, 9 unit tests in slowlog.zig + 4 integration tests in introspection.zig + 5 integration tests in test_slowlog.zig, all tests pass — Phase 6.1 server management (SLOWLOG complete, MONITOR and LATENCY pending) |
| 89 | **Sailor v1.11.0 migration** — Terminal graphics and visual effects (shadow effects with configurable depth/direction, 3D border effects for raised/sunken widgets, blur effects with multiple rendering modes, transparency effects, Sixel graphics protocol for inline raster images with color quantization, Kitty graphics protocol for efficient image transmission, easing functions for smooth animations, color interpolation), added 9 tests in tests/test_sailor_v1_11_0.zig, non-breaking upgrade, all tests pass |
| 90 | **MONITOR real implementation (Phase 6.2)** — MONITOR command enables real-time command streaming to monitoring clients (returns OK), ClientInfo.monitor_mode field, ClientRegistry methods (setMonitorMode, isMonitoring, getMonitoringClients, broadcastToMonitors), MonitorMessage struct for typed messages, command dispatcher integration (broadcasts all commands except MONITOR/QUIT before execution), message format: +timestamp.usec [db addr] "cmd" "arg1" ..., quote/backslash escaping in arguments, 2 unit tests in utility.zig (enable mode, wrong args) + 5 unit tests in client.zig (setMonitorMode, getMonitoringClients, broadcastToMonitors, quote escaping) + 6 integration tests in test_monitor.zig (enable mode, broadcast, exclusions, multi-client, timestamp format), all tests pass — Phase 6.2 server management (MONITOR complete, LATENCY and MEMORY real impl pending) |
| 91 | **Sailor v1.12.0 migration** — Enterprise & Accessibility (session recording & playback for debugging TUI, audit logging with 10 event types and 4 severity levels, 4 high contrast WCAG AAA themes with 21:1+ contrast ratios, screen reader enhancements with OSC8/ARIA/JSON modes, keyboard-only navigation with 5 focus indicator styles), all opt-in features, non-breaking upgrade, all tests pass |
| 92 | **LATENCY real implementation (Phase 6.3)** — LATENCY LATEST/HISTORY/RESET/GRAPH/HISTOGRAM/DOCTOR/HELP commands, LatencyMonitor data structure with event tracking (11 event types) and command histograms (16 logarithmic buckets), HistoryBuffer ring buffer (160 samples per event), EventType enum with fromString/toString conversion, EventSample named type for LATENCY LATEST, 9 unit tests in latency.zig + 9 unit tests in introspection.zig + 14 integration tests in test_latency.zig, all tests pass — Phase 6.3 complete (SLOWLOG/MONITOR/LATENCY done, MEMORY real impl and DEBUG expansion pending) |
| 93 | **MEMORY real implementation (Phase 6.4)** — MemoryTracker struct tracks peak/current/startup allocated memory, dataset/overhead/replication/AOF buffer bytes, calculates fragmentation ratio, dataset percentage, peak percentage (7 unit tests in memory_tracker.zig), MEMORY STATS returns 15+ real statistics, MEMORY USAGE with improved estimation + overhead calculation + SAMPLES parameter support, MEMORY DOCTOR with real analysis (fragmentation warnings, peak memory, small keys, AOF buffer growth), MEMORY PURGE (no-op for Zig GPA), MEMORY MALLOC-STATS (minimal stats), MEMORY HELP updated, 9 unit tests in introspection.zig + 16 integration tests in test_memory.zig, all tests pass — Phase 6.4 complete (SLOWLOG/MONITOR/LATENCY/MEMORY real implementations done, DEBUG expansion pending) |
| 92 | **LATENCY real implementation (Phase 6.3)** — LATENCY LATEST (get latest samples for all events), LATENCY HISTORY <event> (get 160-sample history), LATENCY RESET [event...] (reset specific/all events), LATENCY GRAPH <event> (ASCII bar chart), LATENCY HISTOGRAM [cmd...] (per-command latency distribution), LATENCY DOCTOR (automated analysis with warnings), LATENCY HELP (help text), LatencyMonitor data structure (event tracking with 11 event types, command histograms with 16 logarithmic buckets), HistoryBuffer ring buffer (160 samples), EventType enum (command, fast-command, fork, rdb-unlink-temp-file, aof-*, expire-cycle, eviction-cycle, eviction-del), EventSample named type, 9 unit tests in latency.zig + 9 unit tests in introspection.zig + 14 integration tests in test_latency.zig, all tests pass — Phase 6.3 complete (SLOWLOG/MONITOR/LATENCY done, MEMORY real impl and DEBUG expansion pending) |
| 93 | **MEMORY real implementation (Phase 6.4)** — MemoryTracker struct tracks peak/current/startup allocated memory + dataset/overhead bytes, calculates fragmentation ratio/dataset %/peak %, integrated into Storage struct, MEMORY STATS returns 15+ real statistics, MEMORY USAGE with improved estimation + SAMPLES parameter, MEMORY DOCTOR with real analysis (fragmentation/peak/small keys/AOF warnings), MEMORY PURGE (no-op), MEMORY MALLOC-STATS (minimal), 9 unit tests + 16 integration tests, all tests pass — Phase 6.4 complete |
| 94 | **DEBUG expansion (Phase 6.5)** — DEBUG SET-ACTIVE-EXPIRE (toggle active expiration 0/1), DEBUG SLEEP (sleep for N seconds), DEBUG RELOAD (save + reload RDB from disk), DEBUG CHANGE-REPL-ID (stub - returns OK), DEBUG POPULATE (create test keys with prefix/size), active_expire_enabled field in Storage struct (default: true), HELP text updated with all subcommands, 7 unit tests in utility.zig + 4 integration tests in test_utility.zig, all tests pass — Phase 6.5 complete (DEBUG expansion done, Phase 6 server management 80% complete) |

### Known stubs (need real implementation for 1.0)

Lua scripting (EVAL returns nil), ACL (no enforcement), Cluster (single-node), SHUTDOWN (no-op), SELECT (DB 0 only). **All blocking commands now have true blocking semantics using polling** (BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD BLOCK, XREADGROUP BLOCK). **SLOWLOG, MONITOR, LATENCY, and MEMORY now have real implementations** (SLOWLOG: ring buffer tracking, MONITOR: real-time command streaming, LATENCY: event tracking + command histograms, MEMORY: real memory tracking with fragmentation analysis). **DEBUG expanded with 5 new subcommands** (SET-ACTIVE-EXPIRE, SLEEP, RELOAD, CHANGE-REPL-ID stub, POPULATE) — Phase 6 server management 80% complete.

---

## Build & Run

```bash
zig build                          # Build
zig build -Doptimize=ReleaseFast   # Release build
zig build run                      # Build and run server
zig build test                     # All tests (unit + integration)
zig build test-integration         # Integration tests only
./zig-out/bin/zoltraak             # Start server (default 127.0.0.1:6379)
./zig-out/bin/zoltraak --host 0.0.0.0 --port 6380
./zig-out/bin/zoltraak-cli         # REPL client
redis-cli -p 6379                  # Connect with standard redis-cli
```

---

## Project Structure

```
zoltraak/
├── src/
│   ├── main.zig                 # Server entry point
│   ├── server.zig               # TCP server, command routing
│   ├── cli.zig                  # REPL client (zoltraak-cli)
│   ├── protocol/
│   │   ├── parser.zig           # RESP2/RESP3 parser
│   │   └── writer.zig           # RESP2/RESP3 response writer
│   ├── commands/                # Command handlers (26 files)
│   │   ├── strings.zig          # String commands
│   │   ├── lists.zig            # List commands
│   │   ├── sets.zig             # Set commands
│   │   ├── hashes.zig           # Hash commands
│   │   ├── sorted_sets.zig      # Sorted set commands
│   │   ├── streams.zig          # Stream commands
│   │   ├── streams_advanced.zig # Consumer groups, XCLAIM, XAUTOCLAIM
│   │   ├── geo.zig              # Geospatial commands
│   │   ├── hyperloglog.zig      # HyperLogLog commands
│   │   ├── bits.zig             # SETBIT, GETBIT, BITCOUNT, BITOP
│   │   ├── bitfield.zig         # BITFIELD, BITFIELD_RO
│   │   ├── keys.zig             # Key management (TTL, EXPIRE, SCAN, etc.)
│   │   ├── transactions.zig     # MULTI/EXEC/WATCH
│   │   ├── pubsub.zig           # Pub/Sub commands
│   │   ├── replication.zig      # REPLICAOF, REPLCONF, PSYNC
│   │   ├── client.zig           # CLIENT subcommands
│   │   ├── config.zig           # CONFIG subcommands
│   │   ├── command.zig          # COMMAND introspection
│   │   ├── info.zig             # INFO command
│   │   ├── scripting.zig        # EVAL/EVALSHA stubs
│   │   ├── acl.zig              # ACL stubs
│   │   ├── cluster.zig          # Cluster stubs
│   │   ├── utility.zig          # ECHO, QUIT, SELECT, TIME, etc.
│   │   ├── server_commands.zig  # SAVE, BGSAVE, FLUSHDB, etc.
│   │   └── introspection.zig    # OBJECT, DEBUG
│   └── storage/                 # Data layer
│       ├── memory.zig           # Core storage engine (tagged union Value)
│       ├── persistence.zig      # RDB persistence
│       ├── aof.zig              # AOF logging
│       ├── config.zig           # Runtime configuration
│       ├── replication.zig      # Replication state
│       ├── pubsub.zig           # Pub/Sub state
│       ├── acl.zig              # ACL storage (stub)
│       ├── scripting.zig        # Script cache
│       └── blocking.zig         # Blocking queue for XREAD/XREADGROUP BLOCK
├── tests/                       # Integration tests
│   ├── test_integration.zig     # Main integration suite
│   ├── test_key_management.zig
│   ├── test_hash_field_ttl.zig
│   ├── test_resp3.zig
│   ├── test_bitfield.zig
│   ├── test_client.zig
│   ├── test_utility.zig
│   └── integration_test.sh      # Shell-based tests (redis-cli)
├── docs/
│   └── PRD.md                   # 1.0 product requirements (18 phases)
├── .claude/
│   └── agents/                  # 9 specialized agents
├── build.zig                    # Build config (sailor dependency)
├── build.zig.zon                # Package manifest (Zig 0.15+, sailor v0.4.0)
└── README.md                    # User-facing documentation
```

> **Note**: 파일 구조는 참고안. 실제 구현에 따라 변경 가능하며, 소스 코드가 기준.

---

## Zig Guidelines

### Conventions
- `std.mem.Allocator` for all allocations
- `errdefer` for cleanup on error paths
- `comptime` for compile-time computations where beneficial
- snake_case for functions/variables, PascalCase for types

### Zig 0.15 API Notes
- `ArrayListUnmanaged` — mutation methods take allocator as first arg
- `std.io.getStdOut().writer(&buf)` + `.interface.print()` — flush before exit
- `std.builtin.Type` tags are lowercase: `.int`, `.@"struct"`, etc.
- `b.createModule()` for build system execution/test targets

### Memory Management
- Arena allocator for per-request allocations
- General purpose allocator for long-lived data
- Always free in same scope or defer cleanup
- `std.testing.allocator` in tests for leak detection

### Error Handling
- Use Zig error unions (`!`) for fallible operations
- Provide meaningful error types — no `anyerror`
- Log errors at appropriate levels before returning

---

## Redis Command Implementation Pattern

1. **Storage layer** (`src/storage/memory.zig`): Add data type variant to `Value` tagged union, implement operations
2. **Command handlers** (`src/commands/<type>.zig`): Parse args → validate → execute against storage → format RESP response
3. **Command routing** (`src/server.zig`): Register in dispatch table
4. **WRONGTYPE errors**: Always check type match before operating
5. **RESP3 awareness**: Return native RESP3 types (maps, sets) when protocol version is 3

---

## Development Cycle (8 Phases)

Each iteration follows this workflow. One iteration = one feature/command group. No scope creep.

### Phase 1 — Planning
**Agent**: `redis-spec-analyzer`
- Analyze Redis spec for target commands
- Document command syntax, return types, error conditions, edge cases
- Cross-reference with redis.io/commands

### Phase 2 — Implementation + Unit Tests
**Agents**: `zig-implementor` + `unit-test-writer` (parallel)
- Implement in `src/commands/`, `src/storage/`
- Embed unit tests at end of source files (Zig convention)
- All tests pass with `zig build test`
- **Gate**: compiles, unit tests pass, no memory leaks

### Phase 3 — Code Quality Review
**Agents**: `zig-quality-reviewer` → `code-reviewer` (sequential)
- Zig review: memory safety, error handling, idioms, doc comments
- Architecture review: separation of concerns, API design, maintainability
- **Gate**: Critical issues → BLOCK. Important issues → FIX or BLOCK.

### Phase 4 — Integration Testing
**Agent**: `integration-test-orchestrator`
- Create E2E RESP protocol tests in `tests/` directory
- Test full request-response cycles and command interactions
- **Gate**: all integration tests pass

### Phase 5 — Validation
**Agents**: `redis-compatibility-validator` + `performance-validator` (parallel)
- Differential testing vs real Redis (byte-by-byte RESP comparison)
- redis-benchmark throughput/latency comparison
- **Gate**: compatibility >= 95%, performance >= 70% of Redis, regression < 10%

### Phase 6 — Documentation
- Update README.md command tables
- Update CLAUDE.md if new patterns emerged

### Phase 7 — Cleanup
- Delete temporary spec/summary documents
- Remove debug prints, TODO comments
- **Kill all background processes**: `pkill -f zoltraak 2>/dev/null; sleep 1`
- Verify port 6379 is free: `lsof -ti :6379 | xargs kill 2>/dev/null`

### Phase 8 — Commit
**Agent**: `git-commit-push`
- Commit format: `feat(<scope>): implement Iteration N — <description>`
- Push to origin/main

### Failure Handling

| Phase | Action |
|-------|--------|
| 1 (Planning) | Request clarification, retry with more context |
| 2 (Implementation) | Review errors, attempt fix, retry |
| 3 (Quality) | Apply fixes, re-run review. Never skip. |
| 4 (Integration) | Fix tests or implementation, retry |
| 5 (Validation) | Fix compatibility/performance, re-run. Never skip. |
| 6-8 | Investigate and escalate if needed |

### Agent Coordination

| Phase | Agent(s) | Execution |
|-------|----------|-----------|
| 1 | redis-spec-analyzer | Solo |
| 2 | zig-implementor + unit-test-writer | Parallel |
| 3 | zig-quality-reviewer → code-reviewer | Sequential |
| 4 | integration-test-orchestrator | Solo |
| 5 | redis-compatibility-validator + performance-validator | Parallel |
| 6-7 | Orchestrator direct | Solo |
| 8 | git-commit-push | Solo |

### Phase 2 Agents (Planned)

- `documentation-writer`: Automated doc updates
- `zig-build-maintainer`: Safe build.zig management
- `redis-subsystem-planner`: Plans complex subsystems
- `redis-command-validator`: Validates command arguments
- `ci-validator`: CI/CD integration
- `bug-investigator`: Systematic debugging on test failures
- `resp-protocol-specialist`: RESP3 protocol edge cases

---

## Autonomous Session Protocol

자동화 세션(cron job 등)에서는 다음 프로토콜을 실행한다.

**컨텍스트 복원** — 세션 시작 시 읽을 파일:
1. `CLAUDE.md` — 프로젝트 규칙, 개발 사이클, 에이전트 아키텍처
2. `README.md` — 지원 명령어, 프로젝트 상태
3. `docs/PRD.md` — 1.0 로드맵, 다음 구현 대상 확인

**10단계 실행 사이클**:

| Phase | 내용 | 비고 |
|-------|------|------|
| 1. 상태 파악 | git log, 빌드, 테스트 상태 점검 | PRD에서 다음 미완료 이터레이션 식별 |
| 2. 이슈 확인 | `gh issue list --state open --limit 10` | 아래 **이슈 우선순위 프로토콜** 참조 |
| 3. 계획 | 구현 전략을 내부적으로 수립 (텍스트 출력) | 비대화형 세션에서 plan mode 도구 사용 금지 |
| 4. 구현 | Development Cycle 8 Phases 수행 (Planning → Commit) | 사이클당 하나의 이터레이션만 |
| 5. 검증 | `zig build test` 전체 통과 확인 | 실패 시 수정 후 재시도 |
| 6. 코드 리뷰 | 메모리 안전성, Redis 호환성, 테스트 커버리지 확인 | 이슈 발견 시 수정 후 재커밋 |
| 7. 커밋 & 푸시 | `feat(<scope>): implement Iteration N — <description>` | `git add -A` 금지 |
| 8. 릴리즈 판단 | 아래 **릴리즈 판단 기준** 확인 | 조건 충족 시 자율 릴리즈 수행 |
| 9. 프로세스 정리 | `pkill -f zoltraak`, 포트 확인 | 백그라운드 프로세스 전부 종료 |
| 10. 세션 요약 | 구조화된 요약 출력 | 아래 템플릿 참조 |

### 이슈 우선순위 프로토콜

세션 시작 시 GitHub Issues를 확인하고 우선순위를 결정한다:

```bash
gh issue list --state open --limit 10 --json number,title,labels,createdAt
```

| 우선순위 | 조건 | 행동 |
|---------|------|------|
| 1 (최우선) | `bug` 라벨 | 다른 작업보다 항상 우선 처리 |
| 2 (높음) | `feature-request` + 현재 이터레이션 범위 내 | 현재 작업과 병행 |
| 3 (낮음) | `feature-request` + 미래 범위 | 적어두고 넘어감 |

- 이슈 처리 후: `gh issue close <number> --comment "Fixed in <commit-hash>"`
- bug 라벨 이슈가 있으면 새 이터레이션 구현보다 반드시 먼저 수정한다
- bug 수정은 별도 커밋: `fix(<scope>): <description>`

**작업 선택 규칙**:
- bug 이슈가 있으면 → 버그 수정 우선
- bug 없으면 → PRD.md의 Phase 순서를 따름 (Phase 1 → Phase 2 → ...)
- 사이클당 하나의 이터레이션만 구현
- 이전 세션의 미완료 작업(빌드 실패, 테스트 실패)이 있으면 먼저 수정

**안전 규칙**:
- Force push 및 파괴적 git 명령어 금지
- 모든 작업은 `main` 브랜치에서 (단일 브랜치 워크플로우)
- 동일 에러 3회 시도 후 지속 시 문서화하고 다음 항목으로 이동
- 스코프 크리프 금지: 사이클당 하나의 이터레이션만 구현
- 기존 테스트가 반드시 계속 통과해야 함
- `zig build test`가 60초 이상 걸리면 hang으로 간주하고 강제 종료

**프로세스 정리 (필수)**:
- 사이클 중 시작한 모든 백그라운드 프로세스는 사이클 종료 전에 반드시 종료
- 커밋 전 마지막 단계: `pkill -f zoltraak 2>/dev/null; sleep 1`
- 포트 확인: `lsof -ti :6379 | xargs kill 2>/dev/null`

**세션 요약 템플릿**:

    ## Session Summary
    ### Iteration
    - [완료한 이터레이션]
    ### Commands Implemented
    - [추가된 Redis 명령어 목록]
    ### Files Changed
    - [생성/수정된 파일 목록]
    ### Tests
    - Unit tests: [수, 상태]
    - Integration tests: [수, 상태]
    ### Redis Compatibility
    - [호환성 검증 결과]
    ### Issues Resolved
    - [처리한 GitHub Issues (번호, 제목)]
    ### Release
    - [릴리즈 수행 여부 및 버전]
    ### Next Priority
    - [다음 사이클에서 구현할 내용]
    ### Issues / Blockers
    - [발생한 문제 또는 미해결 이슈]

---

## Release & Patch Policy

세션 사이클의 Phase 8(릴리즈 판단)에서 아래 조건을 확인하고, 충족 시 자율적으로 릴리즈를 수행한다.

### 릴리즈 판단 기준

**패치 릴리즈 (v0.1.X)** — 다음 중 하나라도 해당하면 즉시 발행:
- 사용자 보고 버그를 수정한 커밋이 마지막 릴리즈 태그 이후에 존재
- 빌드/테스트 실패 수정
- Redis 호환성 깨짐 수정

**마이너 릴리즈 (v0.X.0)** — 다음 조건을 모두 충족 시 발행:
1. 마지막 릴리즈 이후 새로운 Redis 명령어 그룹이 구현됨 (최소 20개 이상의 새 명령어)
2. 해당 명령어에 대한 테스트가 작성되어 있음
3. `zig build test` — 전체 통과, 0 failures
4. `bug` 라벨 이슈가 0개 (open)

**메이저 릴리즈 (v1.0.0)** — 500+ Redis 명령어 구현 완료 + 사용자 승인

### 릴리즈 조건 확인 방법

```bash
# 마지막 태그 이후 커밋 확인
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LAST_TAG" ]; then
  git log ${LAST_TAG}..HEAD --oneline
else
  git log --oneline -20
fi

# open bug 이슈 확인
gh issue list --state open --label bug --limit 5
```

### 릴리즈 절차

1. `build.zig.zon`의 version 업데이트
2. 커밋: `chore: bump version to v0.X.0`
3. 태그: `git tag -a v0.X.0 -m "Release v0.X.0: <릴리즈 요약>"`
4. 푸시: `git push && git push origin v0.X.0`
5. GitHub Release: `gh release create v0.X.0 --title "v0.X.0: <요약>" --notes "<릴리즈 노트>"`
6. 관련 이슈 닫기: `gh issue close <number> --comment "Resolved in v0.X.0"`
7. Discord 알림: `openclaw message send --channel discord --target user:264745080709971968 --message "[zoltraak] Released v0.X.0 — <요약>"`

### 패치 릴리즈 절차

버그 수정 시 패치 릴리즈를 즉시 발행한다. PATCH 번호만 증가. 기능 커밋을 패치에 포함하지 않음.

1. 버그 수정 커밋 식별
2. `zig build test` 통과 확인
3. 태그: `git tag -a v0.X.Y <commit-hash> -m "Release v0.X.Y: <수정 요약>"`
4. 푸시: `git push origin v0.X.Y`
5. GitHub Release: `gh release create v0.X.Y --title "v0.X.Y: <요약>" --notes "<릴리즈 노트>"`
6. 관련 이슈에 릴리즈 코멘트 추가
7. Discord 알림

---

## Sailor Migration

zoltraak은 `sailor` 라이브러리(https://github.com/yusa-imit/sailor)를 점진적으로 도입한다.

**마이그레이션 프로토콜**:
1. 세션 시작 시 이 섹션을 확인
2. `status: READY`인 미완료 마이그레이션이 있으면 현재 작업보다 우선 수행
3. 마이그레이션 완료 후 `status: DONE`으로 변경하고 커밋
4. `zig build test` 통과 확인 필수

**로컬 워크어라운드 금지**: sailor에 버그가 있으면 로컬 우회 금지. 반드시 `gh issue create --repo yusa-imit/sailor`로 발행하고 수정을 기다린다.

### v0.1.0 — arg, color (DONE)
### v0.2.0 — REPL (DONE)
- sailor v0.2.0의 `repl` 모듈은 Zig 0.15.x 미호환 (Issue #2). `sailor.arg`만 사용, REPL은 자체 구현.
### v0.3.0 — fmt (DONE)
### v0.4.0 — tui (DONE)

### v0.5.0 — advanced widgets (DONE)
- Updated to sailor v0.5.1 (hash: sailor-0.5.0-53_z3PIyBgC7lm9ua1RdBuVdYg9CNCZmx74pUmnoOriy)
- Added src/tui_advanced.zig with advanced TUI widgets (Iteration 72)
- Tree widget for hierarchical key browsing with : delimiter hierarchy
- LineChart for memory/connection metrics dashboard (memory usage bar chart)
- Dialog widget for DEL command confirmation (centered modal with Y/N buttons)
- Notification toast for connection status (bottom-right corner, auto-dismissing)
- New CLI flag: --advanced (-a) for enhanced TUI mode with all widgets
- Dashboard struct manages state (selected_index, dialogs, notifications, timer)
- KeysTree data structure for hierarchical key organization
- MemoryStats tracking (used_memory, peak_memory, num_keys)
- All tests pass, non-breaking upgrade

### v1.0.0 — production ready (READY)
**v1.0.1 패치**: 크로스 컴파일 수정 (API 변경 없음)

- [ ] sailor v1.0.0 의존성 업데이트
- [ ] Getting Started Guide / API Reference 기반 리팩토링
- [ ] 테마 시스템 (다크/라이트 모드)

### v1.0.3 — bug fix release (READY)

**sailor v1.0.3 released** (2026-03-02) — Zig 0.15.2 compatibility patch

- **Bug fix**: Tree widget ArrayList API updated for Zig 0.15.2
- **Impact on zoltraak**: None (zoltraak doesn't use Tree widget yet)
- [ ] `build.zig.zon`에 sailor v1.0.3 의존성 업데이트 (optional)
- [ ] 기존 테스트 전체 통과 확인

**Note**: Optional upgrade. Tree widget fix affects v0.5.0 advanced widgets migration only.

### v1.1.0 — Accessibility & Internationalization (DONE)

**sailor v1.1.0 released** (2026-03-02) — Accessibility and i18n features

- **New features**:
  - Accessibility module (screen reader hints, semantic labels)
  - Focus management system (tab order, focus ring)
  - Keyboard navigation protocol (custom key bindings)
  - Unicode width calculation (CJK, emoji proper sizing)
  - Bidirectional text support (RTL rendering for Arabic/Hebrew)
- **Impact on zoltraak**: Medium priority — improves Redis CLI experience
  - Unicode width fixes critical for multi-language Redis keys/values
  - Keyboard navigation enhances interactive CLI usability
  - Accessibility features improve screen reader support
- Updated to sailor v1.1.0 (hash: sailor-1.1.0-53_z3CCMBwBNK-7VJxsyLL8ljM8qG82hZ37JfOJhqGq7)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 73

**Note**: Non-breaking upgrade. Unicode improvements automatically benefit international Redis data display.

---

## Resources

- [Redis Commands](https://redis.io/commands/)
- [RESP Protocol Spec](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Zig Documentation](https://ziglang.org/documentation/master/)
- [Zoltraak 1.0 PRD](docs/PRD.md)

### v1.2.0 — Layout & Composition (DONE)

**sailor v1.2.0 released** (2026-03-02) — Advanced layout and composition features

- **New features**:
  - Grid layout system (CSS Grid-inspired 2D constraint solver)
  - ScrollView widget (virtual scrolling for large content)
  - Overlay/z-index system (non-modal popups, tooltips, dropdown menus)
  - Widget composition helpers (split panes, resizable borders)
  - Responsive breakpoints (adaptive layouts based on terminal size)
- **Impact on zoltraak**: High priority — enables interactive TUI features
  - Grid layout for multi-pane prompt editor UI
  - ScrollView for long prompt history and chat logs
  - Overlay system for context menus and tooltips
  - Split panes for side-by-side prompt editing and preview
  - Responsive layouts for different terminal sizes
- Updated to sailor v1.2.0 (hash: 1220357e36aec8ac9e47e11bf80a014b226c816bfc42078297984afc091fd2d73a7b)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 76

**Note**: Non-breaking upgrade. Layout features enable future interactive TUI prompt editor and chat interface.

### v1.3.0 — Performance & Developer Experience (DONE)

**sailor v1.3.0 released** (2026-03-02) — Performance optimization and debugging tools

- **New features**:
  - RenderBudget: Frame time tracking with automatic frame skip for 60fps
  - LazyBuffer: Dirty region tracking (only render changed cells)
  - EventBatcher: Coalesce rapid events (resize storms, key bursts)
  - DebugOverlay: Visual debugging (layout rects, FPS, event log)
  - ThemeWatcher: Hot-reload JSON themes without restart
- **Impact on zoltraak**: High priority — critical for TUI performance
  - Lazy rendering essential for large key/data viewers (skip unchanged rows)
  - Event batching handles rapid resize during live monitoring
  - DebugOverlay invaluable for REPL development
  - ThemeWatcher enables theme iteration for CLI/REPL styling
- Updated to sailor v1.3.0 (hash: sailor-1.3.0-53_z3JpKCQCpG_KsgDaxXF3soaMuVYdmoTiD8kPxdGmK)
- Fixed Storage.init() signature in 44 unit tests (port/bind parameters)
- All existing tests pass
- Performance features available for future TUI enhancements

**Note**: Non-breaking upgrade. Performance features are opt-in. REPL and TUI viewers will benefit significantly from lazy rendering.

### v1.4.0 — Advanced Input & Forms (DONE)

**sailor v1.4.0 released** (2026-03-03) — Form widgets and input validation

- **New features**:
  - Form widget: Field validation, submit/cancel handlers, error display
  - Select/Dropdown widget: Single/multi-select with keyboard navigation
  - Checkbox widget: Single and grouped checkboxes with state management
  - RadioGroup widget: Mutually exclusive selection
  - Validators module: Comprehensive input validation (email, URL, IPv4, numeric, patterns)
  - Input masks: SSN, phone, dates, credit card formatting
- **Impact on zoltraak**: High priority — critical for prompt editor and config UI
  - Form widget essential for interactive prompt parameter editor
  - Validators ensure valid Redis connection strings, API keys, model names
  - Select widget for model selection dropdown (GPT-4, Claude, etc.)
  - Checkbox for feature toggles (streaming, embeddings, RAG)
  - RadioGroup for mutually exclusive settings (temperature presets, formats)
- Updated to sailor v1.4.0 (hash: sailor-1.4.0-53_z3OFrCgA54UWtI6XB--VquKygCaJwzXqceaqki3sh)
- All existing tests pass
- Form widgets available for future TUI enhancements

**Note**: Non-breaking upgrade. Form features enable the interactive TUI prompt editor planned for v2.0 milestone.

### v1.5.0 — State Management & Testing (DONE)

**sailor v1.5.0 released** (2026-03-07) — Testing utilities and state management

- **New features**:
  - Widget snapshot testing: assertSnapshot() method for pixel-perfect verification
  - Example test suite: 10 comprehensive integration test patterns
  - Previously released: Event bus, Command pattern, MockTerminal, EventSimulator
- **Impact on zoltraak**: HIGH — Essential for TUI/REPL testing
  - MockTerminal already used in zoltraak's TUI tests
  - assertSnapshot() ensures exact REPL output verification
  - Example patterns guide zoltraak's TUI test expansion
  - Event bus useful for REPL component communication (e.g., completion → highlighting)
  - Command pattern enables undo/redo in prompt editor (future feature)
- Updated to sailor v1.5.0 (hash: sailor-1.5.0-53_z3NZQCwA9JfYkm5M6Lc2fREUBxPsu9XyB_3JLhgiM)
- Added TUI snapshot tests in tests/test_tui_snapshot.zig (5 tests covering Style, Color, Rect, Buffer)
- All existing tests pass
- Testing utilities available for future TUI enhancements

**Note**: Non-breaking upgrade. Testing utilities improve test quality without breaking existing code. Critical for maintaining REPL rendering quality.

### v1.6.0 — Data Visualization & Advanced Charts (DONE)

**sailor v1.6.0 released** (2026-03-08) — Advanced data visualization widgets

- **New features**:
  - ScatterPlot: X-Y coordinate plotting with markers and multiple series
  - Histogram: Frequency distribution bars (vertical/horizontal)
  - TimeSeriesChart: Time-based line chart with Unix timestamp support
  - Heatmap & PieChart (previously released)
- **Impact on zoltraak**: MEDIUM — Useful for Redis monitoring TUI
  - TimeSeriesChart for memory usage over time (INFO stats)
  - Histogram for key distribution across slots (CLUSTER)
  - ScatterPlot for latency vs. throughput analysis
  - Optional for future monitoring dashboard
- Updated to sailor v1.6.0 (hash: sailor-1.6.0-53_z3OizDAALyxQeXTsc1AfcO6Wny1Wz0hvzwZbTsBBx)
- All existing tests pass
- Visualization widgets available for future monitoring TUI enhancements

**sailor v1.6.1 patch released** (2026-03-08) — Critical bug fixes for v1.6.0 widgets

- **Bug fixes**:
  - PieChart: Fixed integer overflow in coordinate calculation (prevented panics)
  - Multiple widgets: Fixed API compilation errors (Color.rgb, buffer.set, u16 casts)
- **Impact on zr**: None (zr doesn't use v1.6.0 widgets yet)
- [ ] Optional: Update to v1.6.1 for stable data visualization widgets

**Note**: Patch release, no breaking changes. Safe to upgrade when/if data visualization widgets are needed.

**Note**: Non-breaking upgrade. Visualization widgets are opt-in. Enables future monitoring TUI enhancements.

### v1.7.0 — Advanced Layout & Rendering (DONE)

**sailor v1.7.0 released** (2026-03-09) — Advanced layout and rendering features

- **New features**:
  - FlexBox layout: CSS flexbox-inspired with justify/align (16 tests)
  - Viewport clipping: Efficient rendering of large virtual buffers (14 tests)
  - Shadow & 3D border effects: Visual depth for widgets (15 tests)
  - Custom widget traits: Extensible widget protocol
  - Layout caching: LRU cache for constraint computation (13 tests)
- **Impact on zoltraak**: MEDIUM — Layout improvements for redis-cli-like TUI
  - FlexBox useful for responsive command/response layouts
  - Viewport clipping enables efficient scrolling through command history
  - Shadow effects add visual polish to TUI mode
  - Layout caching improves performance for complex data browser layouts
- Updated to sailor v1.7.0 (hash: 1220c49110e756c2d9a539fb9df710c86b9ca888a924ac0680ebb223f5bd591348fd)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 75

**Note**: Non-breaking upgrade. All features are opt-in. Layout features available for future TUI enhancements.

### v1.8.0 — Network & Async Integration (DONE)

**sailor v1.8.0 released** (2026-03-10) — Network and async widgets

- **New features**:
  - HttpClient widget: Download progress visualization with speed/stats (16 tests)
  - WebSocket widget: Live data feed with auto-scroll (16 tests)
  - AsyncEventLoop: Non-blocking I/O for network operations (8 tests)
  - TaskRunner widget: Parallel operation status indicator (20 tests)
  - LogViewer widget: Tail -f style with filtering and search (20 tests)
- **Impact on zoltraak**: MEDIUM — Network widgets useful for future features
  - AsyncEventLoop enables non-blocking Redis operations in TUI
  - LogViewer perfect for displaying Redis MONITOR output
  - TaskRunner could visualize multi-key operations progress
  - WebSocket useful for Redis pub/sub visualization
- Updated to sailor v1.8.0 (hash: 1220063abf0348544c529399ac31b06cd5cc645aa66e147181d752b59a22ac8057ab)
- Fixed missing Writer.writeArrayOfBulkStrings method (convenience helper for array of strings)
- Fixed TxState.discard() → TxState.reset() method call in RESET command
- All existing tests pass
- Non-breaking upgrade completed in Iteration 79

**Note**: Non-breaking upgrade. All features are opt-in. Network features available for future Redis TUI enhancements.

### v1.9.0 — Developer Tools & Ecosystem (DONE)

**sailor v1.9.0 released** (2026-03-11) — Developer tools and ecosystem improvements

- **New features**:
  - WidgetDebugger: Widget tree inspection with layout bounds visualization
  - PerformanceProfiler: Frame timing & memory profiling with histogram display
  - CompletionPopup: REPL tab completion popup (resolves repl.zig TODO)
  - ThemeEditor: Live theme customization with RGB editing and preview (18 tests)
  - Widget Gallery: Comprehensive catalog of 40+ widgets across 7 categories
- **Impact on zoltraak**: HIGH — Directly improves Redis CLI user experience
  - CompletionPopup is CRITICAL for Redis command/key completion in interactive mode
  - PerformanceProfiler helps optimize TUI performance for large key sets
  - WidgetDebugger aids debugging complex data visualization layouts
  - ThemeEditor enables user-customizable Redis CLI themes
- Updated to sailor v1.9.0 (hash: 122088d326e2de1b2c40547ee39927182412f68da173f46f2bcabc3aa653aec5e465)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 83

**Note**: Non-breaking upgrade. Developer tools and CompletionPopup widget available for future Redis CLI enhancements.

---

### v1.10.0 — Mouse & Gamepad Input (READY)

**sailor v1.10.0 released** (2026-03-12) — Mouse and gamepad input support

- **New features**: Mouse event handling, widget-level mouse interaction, gamepad/controller support, touch gestures, input mapping
- **Impact on zoltraak**: MEDIUM — Mouse support useful for interactive Redis CLI
  - Click to select keys in key browser widget
  - Scroll through large datasets with mouse wheel
  - Gamepad support not relevant for Redis CLI
- [ ] Update to sailor v1.10.0
- [ ] Consider mouse interaction for key browser and data viewer widgets

### v1.11.0 — Terminal Graphics & Effects (READY)

**sailor v1.11.0 released** (2026-03-12) — Terminal graphics protocols and visual effects

- **New features**: Sixel/Kitty graphics protocols, animated transitions, particle effects, blur/transparency
- **Impact on zoltraak**: LOW — Graphics effects not essential for Redis CLI
  - Sixel/Kitty could display Redis key value images (if stored as binary)
  - Particle effects for successful command execution
  - Transitions for smoother navigation in TUI mode
- [ ] Update to sailor v1.11.0
- [ ] (Optional) Add transitions for TUI navigation smoothness

### v1.12.0 — Enterprise & Accessibility (READY)

**sailor v1.12.0 released** (2026-03-13) — Enterprise features and accessibility

- **New features**: Session recording/playback, audit logging, high contrast themes, screen reader support, keyboard navigation
- **Impact on zoltraak**: HIGH — Enterprise features critical for production Redis management
  - Session recording enables reproducing Redis CLI bugs from user reports
  - Audit logging tracks Redis commands for compliance (GDPR, SOC2)
  - High contrast themes for accessibility
  - Screen reader support for visually impaired administrators
- [ ] Update to sailor v1.12.0
- [ ] Implement audit logging for Redis commands
- [ ] Enable high contrast themes for Redis CLI

### v1.13.0 — Advanced Text Editing & Rich Input (READY)

**sailor v1.13.0 released** (2026-03-14) — Multi-cursor editing and rich text input

- **New features**: Syntax highlighting, code editor widget, autocomplete widget, multi-cursor editing, rich text input
- **Impact on zoltraak**: HIGH — Directly improves Redis command input experience
  - Syntax highlighting for Redis commands and Lua scripts
  - Autocomplete widget for Redis command completion (CRITICAL for UX)
  - Multi-cursor editing for bulk key updates
  - Rich text input for formatted command documentation
- [ ] Update to sailor v1.13.0
- [ ] Integrate autocomplete widget for Redis command/key completion
- [ ] Add syntax highlighting for Redis commands and Lua scripts
- [ ] (Optional) Use code editor widget for multi-line Lua script editing

---

## zuda Migration

zoltraak는 현재 자체 구현한 자료구조/알고리즘을 `zuda` 라이브러리(https://github.com/yusa-imit/zuda)로 점진적으로 대체할 예정이다.
zuda의 해당 구현이 완료되면 `from:zuda` 라벨 이슈가 발행된다.

### 마이그레이션 대상

| 자체 구현 | 파일 | zuda 대체 | status |
|-----------|------|-----------|--------|
| Sorted Set (HashMap + sorted list) | `src/storage/memory.zig` | `zuda.containers.lists.SkipList` | PENDING |
| HyperLogLog | `src/storage/memory.zig` | `zuda.containers.probabilistic.HyperLogLog` | PENDING |
| Glob Pattern Matching | `src/utils/glob.zig` | `zuda.algorithms.string.glob_match` | PENDING |
| Geohash encoding | `src/commands/geo.zig` | `zuda.algorithms.geometry.geohash` | PENDING |
| Haversine Distance | `src/commands/geo.zig` | `zuda.algorithms.geometry.haversine` | PENDING |

### 마이그레이션 제외 (domain-specific)

- `src/storage/memory.zig` (core storage) — Redis 시맨틱에 밀접, 자체 유지
- `src/storage/pubsub.zig` — Redis Pub/Sub 전용 프로토콜
- `src/storage/blocking.zig` — Redis BLOCK 전용 큐
- `src/commands/streams.zig` — Redis Stream 전용 로직
- `src/commands/bits.zig` — Redis BITOP 전용 로직

> Sorted Set은 zoltraak에서 가장 복잡한 자체 구현(1800 LOC)이다. zuda의 SkipList가 score 기반 정렬 + 동점 시 lexicographic 비교를 지원하는지 확인 후 마이그레이션한다.

### 마이그레이션 프로토콜

1. zuda에서 `from:zuda` 라벨 이슈가 도착하면 해당 마이그레이션의 status를 `READY`로 변경
2. **Sorted Set 마이그레이션 특별 절차**:
   - zuda SkipList가 `(score: f64, member: []const u8)` 복합 키 정렬 지원하는지 확인
   - rank 기반 range query (ZRANGE), score 기반 range query (ZRANGEBYSCORE) 모두 지원하는지 확인
   - 모든 기존 sorted set 테스트를 zuda 기반으로 포팅하여 동작 확인
   - Redis 호환성 검증 통과 확인
3. 일반 마이그레이션:
   - `build.zig.zon`에 zuda 의존성 추가
   - 자체 구현을 zuda import로 교체
4. `zig build test` + `tests/integration_test.sh` 전체 통과 확인
5. status를 `DONE`으로 변경하고 커밋

### zuda 이슈 발행 프로토콜

zuda를 사용하는 중 버그를 발견하거나 필요한 기능이 없을 때:

```bash
gh issue create --repo yusa-imit/zuda \
  --title "bug: <간단한 설명>" \
  --label "bug,from:zoltraak" \
  --body "## 증상
<어떤 문제가 발생했는지>

## 재현 방법
<코드 또는 단계>

## 환경
- zuda: <version>
- zig: $(zig version)"
```

- **로컬 워크어라운드 금지**: zuda에 버그가 있으면 자체 구현으로 우회하지 않고, 이슈 발행 후 수정 대기
- zuda 에이전트가 `from:*` 라벨 이슈를 최우선 처리한다

### v1.10.0 — Mouse & Gamepad Input (DONE)

**sailor v1.10.0 released** (2026-03-11) — Mouse, gamepad, and touch input support

- **New features**:
  - Mouse event handling: SGR protocol, click/drag/scroll/double-click (19 tests)
  - Widget mouse interaction: Clickable, Draggable, Scrollable, Hoverable traits (17 tests)
  - Gamepad/controller input: Buttons, analog sticks, triggers, multi-controller (13 tests)
  - Touch gesture recognition: Tap, swipe, pinch, multi-touch support (18 tests)
  - Input mapping: Remap mouse/gamepad/touch to keyboard events (16 tests)
- **Impact on zoltraak**: MEDIUM — Enhances interactive features
  - Mouse click for zoltraak-cli browser interface
  - Scrollable log viewer and data inspector
  - Touch gestures for mobile terminal support
  - Input mapping for accessibility
- Updated to sailor v1.10.0 (hash: 1220a5d9787ed4c22adb54f9e0cc8bc6b7a5a9fc5f157f31d33cbb7b051726cbf133)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 85

**Note**: Non-breaking upgrade. Mouse/gamepad/touch support is opt-in. Features available for future TUI enhancements.

---


### v1.11.0 — Terminal Graphics & Effects (DONE)

**sailor v1.11.0 released** (2026-03-12) — Terminal graphics and visual effects

- **New features**:
  - Shadow effects (subtle/medium/heavy with configurable depth and direction)
  - 3D border effects (raised buttons, sunken inputs)
  - Blur effects (box drawing, half blocks, shade chars, braille patterns)
  - Transparency effects (char fade, color dim, checkerboard)
  - Sixel graphics protocol (DEC Sixel for inline raster images with color quantization)
  - Kitty graphics protocol (efficient image transmission with compression)
  - Easing functions (linear, ease-in/out, cubic, bounce, elastic)
  - Color interpolation for smooth transitions
- **Impact on zoltraak**: MEDIUM — enhances TUI visual quality
  - Shadow effects add depth to TUI widgets (key browser panels, dialogs)
  - Blur effects improve modal overlay aesthetics
  - Sixel/Kitty protocols enable inline Redis data visualization (graphs, images)
  - Easing functions smooth out widget transitions and animations
  - Optional features for future TUI enhancements
- Updated to sailor v1.11.0 (hash: 12206266e32da3d1201970510c2c3d3696bd25b714e820e952cb996b63f7ab7ff6ce)
- Added 9 tests in tests/test_sailor_v1_11_0.zig (shadow, 3D borders, blur, transparency, Sixel, Kitty, easing)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 89

**Note**: Non-breaking upgrade. All graphics features are opt-in. Sixel/Kitty protocols require compatible terminals.

### sailor v1.12.0 — Enterprise & Accessibility (DONE)
- **Features**:
  - Session recording & playback system for debugging TUI interactions
  - Audit logging with 10 event types, 4 severity levels, JSON/file export
  - 4 high contrast WCAG AAA themes (dark 21:1, light 21:1, amber 13.7:1+, green 15.2:1+)
  - Screen reader enhancements (OSC8, ARIA, JSON output modes, auto-detection)
  - Keyboard-only navigation improvements (skip links, 5 focus indicator styles, navigation hints)
- **Usage in zoltraak**:
  - Audit logging for spell execution history (compliance tracking)
  - High contrast themes for accessibility mode
  - Keyboard navigation hints for CLI help displays
- **Breaking**: None — all additive features, fully backward compatible
- Updated to sailor v1.12.0 (hash: 122057e28b0c60aca4981d2402eb22cdb642aba1a1eabec345b8cb944ee515ae15f8)
- All existing tests pass
- Non-breaking upgrade completed in Iteration 91
