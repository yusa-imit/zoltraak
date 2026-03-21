# zoltraak — Milestones

## Current Status

- **Latest release**: v0.1.0
- **Iterations complete**: 123 (193+ Redis commands, ACL key permissions + SETUSER pattern parsing complete, 2/5 zuda migrations, sailor v1.18.0 migrated)
- **Target**: v1.0 — 100% Redis compatibility (500+ commands)
- **Current phase**: Phase 3 ACL enforcement (95% complete — AUTH + command permissions + dispatcher integration + key pattern matching + ACL SETUSER pattern rules done, enforcement points pending)
- **Next milestone**: Phase 3 (dispatcher enforcement points), Phase 7 (multi-DB)
- **zuda migrations**: 2/5 complete (Glob ✅, Haversine ✅, HyperLogLog BLOCKED, Geohash BLOCKED, SortedSet DEFERRED)
- **Known stubs**: ACL (AUTH done, command permissions enforced, key/channel patterns structure done), Cluster (single-node), SELECT (DB 0 only)
- **Real implementations**: SLOWLOG, MONITOR, LATENCY, MEMORY, DEBUG, SHUTDOWN, FAILOVER, ROLE, WAIT, AUTH (all have real implementations as of Iteration 95-115)
- **Blocking commands**: All blocking commands have true polling-based semantics (BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD BLOCK, XREADGROUP BLOCK)
- **Hash enhancements (Phase 1.1)**: HMSET, HGETDEL, HGETEX, HSETEX, HRANDFIELD, HEXPIRE*, HPERSIST, HTTL/HPTTL, HEXPIRETIME/HPEXPIRETIME, HSCAN NOVALUES (all 10 implemented)
- **WAIT command**: Full per-client replication offset tracking (Iteration 102)
- **Sailor library**: v1.18.0 (advanced terminal features + developer experience: capability database, bracketed paste, synchronized output, hyperlinks, focus tracking, hot reload, widget inspector, benchmarks)

---

## Active Milestones

### Phase 6 — Server Management (100% complete) ✅

| Iteration | Command | Status |
|-----------|---------|--------|
| 88 | SLOWLOG (real implementation with ring buffer) | Done |
| 90 | MONITOR (real-time command streaming) | Done |
| 92 | LATENCY (event tracking + command histograms) | Done |
| 93 | MEMORY (real memory tracking with fragmentation) | Done |
| 94 | DEBUG (SET-ACTIVE-EXPIRE, SLEEP, RELOAD, POPULATE) | Done |
| 95 | SHUTDOWN (graceful with SAVE/NOSAVE/NOW/FORCE/ABORT) | Done |
| 97 | FAILOVER (coordinated manual failover, stub execution) | Done |
| 98 | ROLE (replication role introspection) | Done |

### Upcoming Phases (from PRD)

- Phase 7+: See `docs/PRD.md` for remaining phases toward v1.0

---

## Completed Milestones

100 iterations complete. Grouped by feature area:

| Group | Iterations | Summary |
|-------|-----------|---------|
| Core Data Structures | 1-5 | Strings, Lists, Sets, Hashes, Sorted Sets — basic CRUD operations |
| Persistence | 6-7 | RDB snapshots, AOF append-only logging |
| Pub/Sub | 8, 62, 69 | SUBSCRIBE/PUBLISH, pattern subscriptions (PSUBSCRIBE), sharded pub/sub (SSUBSCRIBE/SPUBLISH) |
| Transactions | 9 | MULTI/EXEC/DISCARD/WATCH/UNWATCH |
| Replication | 10 | REPLICAOF, REPLCONF, PSYNC, WAIT |
| Extended Data Types | 11-12, 18-19, 42-43, 45-54, 99-100 | Extended Set/SortedSet/String/List commands, SCAN family, OBJECT, LPOS, LMOVE, LCS, MGET, HRANDFIELD, LMPOP, ZMPOP, Hash field-level TTL, HGETDEL/HGETEX/HSETEX, ZRANGESTORE, HSCAN NOVALUES, ZRANGE unified |
| Introspection | 13-15, 29-30 | CLIENT, CONFIG, COMMAND, MEMORY stubs, SLOWLOG stubs, INFO |
| Streams | 16-17, 24, 27-28, 54-55, 59-60, 63-64, 66 | XADD through XAUTOCLAIM, consumer groups, XSETID, XACKDEL/XDELEX, XCFGSET, XREAD/XREADGROUP BLOCK (polling) |
| Bit Operations | 20, 44, 51 | SETBIT/GETBIT/BITCOUNT/BITOP, BITFIELD/BITFIELD_RO, BITPOS |
| Key Management | 21, 23 | TTL, EXPIRE, DUMP, RESTORE, COPY, TOUCH |
| Geospatial | 25, 56, 58 | GEOADD/GEOPOS/GEODIST/GEOHASH/GEORADIUS/GEOSEARCH, BYBOX, GEOSEARCHSTORE, GEORADIUS_RO |
| HyperLogLog | 26 | PFADD, PFCOUNT, PFMERGE |
| RESP3 Protocol | 31-35 | RESP3 protocol support, per-connection negotiation (HELLO) |
| Stubs (Scripting/ACL/Cluster) | 36-38 | EVAL/EVALSHA stubs, ACL stubs, Cluster stubs |
| Utility Commands | 39-41, 57, 61, 65, 77 | ECHO, QUIT, SELECT, SWAPDB, TIME, MONITOR stub, DEBUG, SHUTDOWN stub, SORT/SORT_RO, HMSET, WAITAOF, DIGEST, DELEX |
| Blocking Commands (True Semantics) | 64, 66-68 | XREAD/XREADGROUP BLOCK, BLPOP/BRPOP/BLMOVE, BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP — all use polling (100ms intervals) |
| Client Management (Phase 5) | 78, 80-82, 84, 86-87, 104 | RESET, CLIENT INFO/HELP/KILL/PAUSE/UNPAUSE/UNBLOCK/NO-EVICT/REPLY/NO-TOUCH/SETINFO/TRACKING/TRACKINGINFO/CACHING/GETREDIR (15/15 P0+P1+P2 commands, 100% complete ✅) |
| Server Management (Phase 6) | 88, 90, 92-95, 97-98 | SLOWLOG/MONITOR/LATENCY/MEMORY/DEBUG/SHUTDOWN/FAILOVER/ROLE real implementations (8/8 commands, 100% complete ✅) |
| Generic Key Commands (Phase 1.6) | 102 | WAIT full implementation with per-client replication offset tracking |
| Sailor Migrations | 70-76, 79, 83, 85, 89, 91, 96, 101, 103, 106, 110 | sailor v0.5.0 through v1.16.0 — TUI widgets, data viz, layout, accessibility, text editing, performance optimizations, terminal features |
| Lua Scripting (Phase 2) | 107-113 | Lua 5.1 integration, redis.call/pcall, sandboxing, timeout, SCRIPT KILL, cjson/cmsgpack/struct libraries (100% complete ✅) |
| ACL Enforcement (Phase 3) | 115-117, 120-121 | AUTH command, command/category permissions, dispatcher integration, key pattern structure + matching logic (90% complete, SETUSER pattern rules + enforcement points pending) |

---

## Dependency Migration Tracking

### Sailor Library

- **Current in zoltraak**: v1.18.0 (build.zig.zon)
- **Latest available**: v1.18.0
- **Migration status**: All versions through v1.18.0 migrated.

| Version | Features | Status |
|---------|----------|--------|
| v0.1.0-v0.4.0 | arg, color, REPL, fmt, tui | Done |
| v0.5.0 | Advanced widgets (Tree, LineChart, Dialog, Notification) | Done (Iter 72) |
| v1.0.0-v1.0.3 | Production ready, cross-compile fix, Tree widget fix | Ready (not applied) |
| v1.1.0 | Accessibility, Unicode width, keyboard nav | Done (Iter 73) |
| v1.2.0 | Grid layout, ScrollView, overlay, split panes | Done (Iter 76) |
| v1.3.0 | RenderBudget, LazyBuffer, EventBatcher, DebugOverlay | Done (Iter 74) |
| v1.4.0 | Form widget, Select, Checkbox, RadioGroup, Validators | Done |
| v1.5.0 | MockTerminal, snapshot testing, Event bus | Done (Iter 70) |
| v1.6.0 | ScatterPlot, Histogram, TimeSeriesChart | Done (Iter 71) |
| v1.7.0 | FlexBox, viewport clipping, shadow effects, layout cache | Done (Iter 75) |
| v1.8.0 | HttpClient, WebSocket, AsyncEventLoop, TaskRunner, LogViewer | Done (Iter 79) |
| v1.9.0 | WidgetDebugger, PerformanceProfiler, CompletionPopup | Done (Iter 83) |
| v1.10.0 | Mouse events, gamepad, touch, input mapping | Done (Iter 85) |
| v1.11.0 | Sixel/Kitty graphics, blur, shadows, easing, particles | Done (Iter 89) |
| v1.12.0 | Session recording, audit logging, WCAG themes, screen reader | Done (Iter 91) |
| v1.13.0 | Syntax highlighting, code editor, autocomplete, multi-cursor | Done (Iter 96) |
| v1.13.1 | Integer overflow fix for data viz widgets | Done (Iter 101) |
| v1.14.0 | Memory pooling, render profiling, virtual rendering, incremental layout, buffer compression | Done (Iter 103) |
| v1.15.0 | Thread safety enhancements, XTGETTCAP terminal capability query, memory leak fixes (repl.zig), multi-platform CI (Linux, macOS, Windows), 13 platform tests | Done (Iter 106) |
| v1.16.0 | Advanced terminal features: terminal capability database (termcap module), bracketed paste mode (DEC 2004), synchronized output protocol (DEC 2026), hyperlink support (OSC 8), focus tracking (DEC 1004) | Done (Iter 110) |
| v1.17.0 | (skipped — direct upgrade to v1.18.0) | N/A |
| v1.18.0 | Developer experience: hot reload for themes, widget inspector, benchmark suite, example gallery, documentation generator — backward compatible, zero breaking changes | Done (Iter 122) |

### zuda Library

- **Current**: 2/5 migrations complete (Glob, Haversine done; HyperLogLog BLOCKED, Geohash BLOCKED, Sorted Set DEFERRED) — zuda v1.15.0 integrated
- **Repository**: https://github.com/yusa-imit/zuda
- **Compatibility layers**: `zuda.compat.zoltraak_sortedset` — drop-in SortedSet wrapper
- **Migration guides**: See zuda `docs/migrations/ZOLTRAAK_SORTEDSET.md` for detailed API mapping
- **Open issues**: #1, #2, #3 (all `enhancement`, `from:zuda` label)

| Custom Implementation | File | LOC | zuda Target | Status |
|----------------------|------|-----|-------------|--------|
| Glob Pattern Matching | `src/utils/glob.zig` | 90 | `zuda.algorithms.string.globMatch` | **DONE** (Iter 119) |
| Haversine Distance | `src/commands/geo.zig` | 15 | `zuda.algorithms.geometry.haversineDistanceM` | **DONE** (Iter 118) |
| HyperLogLog | `src/storage/memory.zig` | 80 | `zuda.containers.probabilistic.HyperLogLog` | **BLOCKED** (API mismatch: allocator vs embedded) |
| Geohash encoding | `src/commands/geo.zig` | 1400 | `zuda.algorithms.geometry.geohashEncode` | **BLOCKED** (API mismatch: string vs u64) |
| Sorted Set (HashMap + sorted list) | `src/storage/memory.zig` | 1800 | `zuda.compat.zoltraak_sortedset` or `zuda.containers.lists.SkipList` | **DEFERRED** (pending above) |

**Migration order**: 간단한 것부터 순서대로 (Glob → Haversine → HyperLogLog → Geohash → Sorted Set)

**Blocked migrations** (Iteration 120 analysis):
- **HyperLogLog**: zuda requires allocator + runtime init, Redis needs embedded fixed-size [16384]u8 array in Value union (no error propagation)
- **Geohash**: zuda uses base32 string encoding, Redis uses u64 binary encoding for sorted set storage optimization
- **Recommendation**: Keep custom implementations (optimized for Redis semantics), file zuda enhancement requests for embedded/binary variants

**Excluded from migration** (domain-specific):
- `src/storage/memory.zig` (core storage) — Redis semantics
- `src/storage/pubsub.zig` — Redis Pub/Sub protocol
- `src/storage/blocking.zig` — Redis BLOCK queue
- `src/commands/streams.zig` — Redis Stream logic
- `src/commands/bits.zig` — Redis BITOP logic

> **Sorted Set note**: 가장 복잡한 마이그레이션 대상 (1800 LOC). `zuda.compat.zoltraak_sortedset`가 Redis ZADD/ZRANGE/ZRANK/ZSCORE 호환 래퍼를 제공. 또는 `zuda.containers.lists.SkipList`를 직접 사용하여 `(score: f64, member: []const u8)` 복합 키 정렬 + rank/score range query 구현 가능.

---

## Iteration Log

- **120**: **ACL Key Pattern Structure (Phase 3.4)** — Added foundational structure for ACL key pattern permissions (~pattern/%R~pattern/%W~pattern syntax): extended User struct with 4 ArrayList([]const u8) fields (all_keys_allowed bool flag, allowed_key_patterns/read_only_key_patterns/write_only_key_patterns lists), updated deinit() to free pattern lists, updated clone() to deep-copy pattern lists, updated createDefaultUser() to initialize empty pattern lists with all_keys_allowed=true for default user, Zig 0.15.2 unmanaged ArrayList compatibility (used {} empty struct literal, .deinit(allocator), .append(allocator, item)), zero memory leaks, all tests pass, **Phase 3 ACL Enforcement: 75% → 78% complete** (AUTH + command permissions + dispatcher integration + key pattern structure done, hasKeyPermission() logic + pattern parsing + dispatcher wiring pending), next iteration will implement glob pattern matching logic for key permission checks, commit a2c421c
- **109-119**: See previous entries in docs/milestones.md (Lua scripting, Sailor v1.16.0, zuda migrations)
- **121**: **ACL Key Permission Logic (Phase 3.5)** — Implemented `User.hasKeyPermission(key, access_mode)` with glob pattern matching for ~pattern (full access), %R~pattern (read-only), %W~pattern (write-only) ACL rules: imported glob.matchGlob from zuda (Iteration 119), added 12 comprehensive tests covering all_keys_allowed flag, empty patterns (deny-all), pattern precedence (allowed → read-only → write-only), all glob wildcards (*, ?, [abc], [a-z], [^abc]), realistic Redis key patterns (user:*, session:*, cache:*:data, shard:[0-9]:*, env:[abc]:*, tmp:?:key, data:[^t]*), zero allocations (allocation-free for hot path authorization checks), early exit optimization (all_keys_allowed check avoids pattern iteration), enhanced doc comment with permission check order and assertion validation (debug builds only), **Phase 3 ACL Enforcement: 78% → 90% complete** (AUTH + command permissions + dispatcher integration + key pattern structure + matching logic done, ACL SETUSER pattern rules parsing + command dispatcher enforcement points pending), all tests pass (12 new hasKeyPermission tests + existing ACL tests), zig-quality-reviewer: PASS (after access_mode validation fix), code-reviewer: APPROVED (recommended enum-based access_mode for future optimization), ready for Phase 3.6: ACL SETUSER pattern rule parsing (~pattern/allkeys/resetkeys syntax) and command dispatcher wiring (enforce permissions in server.zig command router), commit 7f40e3b
- **122**: **Sailor v1.18.0 Migration** — Migrated from sailor v1.16.0 to v1.18.0 (backward compatible, zero breaking changes): `zig fetch --save` updated build.zig.zon dependency URL + hash, new features include hot reload for themes (watch theme files for instant visual feedback), widget inspector (runtime widget tree analysis for layout debugging), benchmark suite (performance regression detection with CI integration), example gallery (interactive widget showcase with copy-pasteable code), documentation generator (auto-generate API docs from source comments), all tests pass (full test suite verification), docs/milestones.md updated with v1.18.0 entry in sailor migration table, closed GitHub issue #8, commit 752b228
- **123**: **ACL SETUSER Key Pattern Rules (Phase 3.6)** — Implemented parseKeyPatternRules() function to parse ACL key pattern rules in ACL SETUSER command: added parseKeyPatternRules(allocator, rules) → returns struct with all_keys_allowed bool + 3 ArrayList (allowed_key_patterns, read_only_key_patterns, write_only_key_patterns), supports ~pattern (full access), %R~pattern (read-only), %W~pattern (write-only), allkeys (set all_keys_allowed=true), resetkeys (clear all patterns + all_keys_allowed=false), left-to-right rule precedence (last matching allkeys/resetkeys wins), comprehensive error handling with errdefer cleanup for all ArrayList allocations, integrated into cmdACLSetuser() to parse key pattern rules separately from command permission rules, added 7 comprehensive tests covering all pattern types + edge cases (single pattern type, mixed patterns, allkeys flag, resetkeys flag), added integration test for ACL SETUSER with all pattern types in single command, **Phase 3 ACL Enforcement: 90% → 95% complete** (AUTH + command permissions + dispatcher integration + key pattern structure + matching logic + ACL SETUSER pattern parsing done, dispatcher enforcement points pending), all tests pass (7 new parseKeyPatternRules tests + 1 integration test + existing ACL tests), zero memory leaks, ready for Phase 3.7: wire User.hasKeyPermission() into command dispatcher (server.zig) to enforce key permissions on all key-based commands, commit TBD
