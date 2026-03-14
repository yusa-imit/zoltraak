# zoltraak — Milestones

## Current Status

- **Latest release**: v0.1.0
- **Iterations complete**: 98 (188+ Redis commands implemented)
- **Target**: v1.0 — 100% Redis compatibility (500+ commands)
- **Current phase**: Phase 6 — Server Management (100% complete) ✅
- **Next milestone**: Phase 1 remaining commands (see PRD.md), then Phases 2-7
- **Blockers**: zuda library migrations blocked until zuda releases target modules
- **Known stubs**: Lua scripting (EVAL returns nil), ACL (no enforcement), Cluster (single-node), SELECT (DB 0 only)
- **Real implementations**: SLOWLOG, MONITOR, LATENCY, MEMORY, DEBUG, SHUTDOWN, FAILOVER, ROLE (all have real implementations as of Iteration 95-98)
- **Blocking commands**: All blocking commands have true polling-based semantics (BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD BLOCK, XREADGROUP BLOCK)

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

96 iterations complete. Grouped by feature area:

| Group | Iterations | Summary |
|-------|-----------|---------|
| Core Data Structures | 1-5 | Strings, Lists, Sets, Hashes, Sorted Sets — basic CRUD operations |
| Persistence | 6-7 | RDB snapshots, AOF append-only logging |
| Pub/Sub | 8, 62, 69 | SUBSCRIBE/PUBLISH, pattern subscriptions (PSUBSCRIBE), sharded pub/sub (SSUBSCRIBE/SPUBLISH) |
| Transactions | 9 | MULTI/EXEC/DISCARD/WATCH/UNWATCH |
| Replication | 10 | REPLICAOF, REPLCONF, PSYNC, WAIT |
| Extended Data Types | 11-12, 18-19, 42-43, 45-54 | Extended Set/SortedSet/String/List commands, SCAN family, OBJECT, LPOS, LMOVE, LCS, MGET, HRANDFIELD, LMPOP, ZMPOP, Hash field-level TTL, HGETDEL/HGETEX/HSETEX, ZRANGESTORE |
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
| Client Management (Phase 5) | 78, 80-82, 84, 86-87 | RESET, CLIENT INFO/HELP/KILL/PAUSE/UNPAUSE/UNBLOCK/NO-EVICT/REPLY/NO-TOUCH/SETINFO/TRACKING/TRACKINGINFO/CACHING (14/14 P0 commands, 100%) |
| Server Management (Phase 6) | 88, 90, 92-95, 97-98 | SLOWLOG/MONITOR/LATENCY/MEMORY/DEBUG/SHUTDOWN/FAILOVER/ROLE real implementations (100% complete) ✅ |
| Sailor Migrations | 70-76, 79, 83, 85, 89, 91, 96 | sailor v0.5.0 through v1.13.0 — TUI widgets, data viz, layout, accessibility, text editing |

---

## Dependency Migration Tracking

### Sailor Library

- **Current in zoltraak**: v1.13.0 (build.zig.zon)
- **Latest available**: v1.13.1 (patch — integer overflow fix for data viz widgets)
- **Migration status**: All versions through v1.13.0 migrated. v1.13.1 patch not yet applied.

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
| v1.13.1 | Integer overflow fix for data viz widgets | Not applied (optional) |

### zuda Library

- **Current**: Not yet integrated (all modules PENDING)
- **Repository**: https://github.com/yusa-imit/zuda
- **Trigger**: `from:zuda` label issues will arrive when modules are ready
- **Open issues**: #1, #2, #3 (all `enhancement`, `from:zuda` label)

| Custom Implementation | File | zuda Target | Status |
|----------------------|------|-------------|--------|
| Sorted Set (HashMap + sorted list) | `src/storage/memory.zig` | `zuda.containers.lists.SkipList` | Pending |
| HyperLogLog | `src/storage/memory.zig` | `zuda.containers.probabilistic.HyperLogLog` | Pending |
| Glob Pattern Matching | `src/utils/glob.zig` | `zuda.algorithms.string.glob_match` | Pending |
| Geohash encoding | `src/commands/geo.zig` | `zuda.algorithms.geometry.geohash` | Pending |
| Haversine Distance | `src/commands/geo.zig` | `zuda.algorithms.geometry.haversine` | Pending |

**Excluded from migration** (domain-specific):
- `src/storage/memory.zig` (core storage) — Redis semantics
- `src/storage/pubsub.zig` — Redis Pub/Sub protocol
- `src/storage/blocking.zig` — Redis BLOCK queue
- `src/commands/streams.zig` — Redis Stream logic
- `src/commands/bits.zig` — Redis BITOP logic

> Sorted Set is the most complex custom implementation (1800 LOC). Requires zuda SkipList to support `(score: f64, member: []const u8)` composite key sorting + rank-based and score-based range queries.
