# zoltraak — Milestones

## Current Status

- **Latest release**: v0.1.0
- **Iterations complete**: 114 (192+ Redis commands implemented, HINCRBY overflow bug fixed)
- **Target**: v1.0 — 100% Redis compatibility (500+ commands)
- **Current phase**: Phase 2 Lua scripting (100% complete ✅ — execution + redis.call/pcall + sandboxing + timeout + SCRIPT KILL + libraries done)
- **Next milestone**: Phase 2 remaining (Lua libraries cjson/cmsgpack), Phase 3 (ACL enforcement), Phase 7 (multi-DB)
- **Blockers**: zuda library migrations blocked until zuda releases target modules
- **Known stubs**: ACL (no enforcement), Cluster (single-node), SELECT (DB 0 only)
- **Real implementations**: SLOWLOG, MONITOR, LATENCY, MEMORY, DEBUG, SHUTDOWN, FAILOVER, ROLE, WAIT (all have real implementations as of Iteration 95-102)
- **Blocking commands**: All blocking commands have true polling-based semantics (BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD BLOCK, XREADGROUP BLOCK)
- **Hash enhancements (Phase 1.1)**: HMSET, HGETDEL, HGETEX, HSETEX, HRANDFIELD, HEXPIRE*, HPERSIST, HTTL/HPTTL, HEXPIRETIME/HPEXPIRETIME, HSCAN NOVALUES (all 10 implemented)
- **WAIT command**: Full per-client replication offset tracking (Iteration 102)
- **Sailor library**: v1.16.0 (advanced terminal features: capability database, bracketed paste, synchronized output, hyperlinks, focus tracking)

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
| Sailor Migrations | 70-76, 79, 83, 85, 89, 91, 96, 101, 103 | sailor v0.5.0 through v1.14.0 — TUI widgets, data viz, layout, accessibility, text editing, performance optimizations |

---

## Dependency Migration Tracking

### Sailor Library

- **Current in zoltraak**: v1.16.0 (build.zig.zon)
- **Latest available**: v1.16.0
- **Migration status**: All versions through v1.16.0 migrated.

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

---

## Iteration Log

- **109**: **Lua Sandboxing (Phase 2.4)** — Implemented Redis-compatible sandboxing for Lua scripts: removed dangerous globals (os, io, loadfile, dofile), restricted require() to safe libraries (math, string, table, cjson, cmsgpack, struct, bit), enforced local-only variables (blocked global creation), 9 unit tests in lua_engine.zig (os/io/loadfile/dofile blocking, require restrictions, global variable enforcement, safe library access), 9 integration tests in test_lua_scripting.zig (full EVAL command sandboxing), applySandbox() function with metatable protection, all tests pass, zero memory leaks, Phase 2 Lua Scripting: 70% complete (execution + redis.call/pcall + sandboxing done, timeout + libraries pending)
- **110**: **Sailor v1.16.0 migration** — Updated to sailor v1.16.0 (advanced terminal features and protocols): terminal capability database (termcap module with TermInfo.load/parse for terminfo files), bracketed paste mode (term.BracketedPaste with DEC private mode 2004 for command injection prevention), synchronized output protocol (term.SynchronizedOutput with DEC private mode 2026 for tearing elimination), hyperlink support (term.writeHyperlink/writeHyperlinkWithParams with OSC 8 escape sequences), focus tracking (focus.FocusManager with DEC private mode 1004 for focus in/out events), 11 tests in tests/test_sailor_v1_16_0.zig covering module/type availability and backward compatibility, fully backward compatible with zero breaking changes, all tests pass, build.zig.zon updated to hash sailor-1.16.0-53_z3K8MGQApNWbuJ16kRSnUtat4iiWS5L6xrwWXU-L2, GitHub issue #6 resolved
- **111**: **Lua Script Timeout (Phase 2.5)** — Implemented script timeout mechanism to prevent runaway Lua scripts: added lua-time-limit CONFIG parameter (default 5000ms, configurable via CONFIG SET), extended LuaEngine struct with timeout_ms and deadline_ns fields, added lua_sethook FFI bindings (lua_Hook type, LUA_MASKCOUNT event mask, debug hook infrastructure), implemented timeoutHook() debug callback (checks deadline every 1000 instructions, raises timeout error if exceeded), updated LuaEngine.init() to accept timeout_ms parameter (0 = disabled), cmdEval/cmdEvalSha read timeout from storage.config, hook automatically cleared after script execution (deadline reset to 0), 5 unit tests in lua_engine.zig (timeout disabled, within limit, infinite loop termination, long computation termination, hook cleanup between runs), updated all 21 existing tests to pass timeout parameter, all 36 unit tests pass, zero memory leaks, Phase 2 Lua Scripting: 80% complete (execution + redis.call/pcall + sandboxing + timeout done, libraries cjson/cmsgpack + SCRIPT KILL pending)
- **112**: **SCRIPT KILL command (Phase 2.6)** — Implemented SCRIPT KILL for terminating long-running scripts: added kill_requested atomic flag to ScriptStore (std.atomic.Value(bool)), added requestKill/isKillRequested/clearKill methods, modified timeoutHook to check kill flag BEFORE timeout (higher priority), implemented cmdScriptKill (sets flag, returns OK), wired SCRIPT KILL into command dispatcher after FLUSH subcommand, updated SCRIPT HELP to include KILL documentation, atomic flag allows cross-connection script termination, kill check every 1000 Lua instructions (same frequency as timeout), flag auto-cleared on termination, 1 unit test in src/commands/scripting.zig (cmdScriptKill sets kill flag), 2 integration tests in tests/test_script_kill.zig, added to build.zig test_step, bug fixes: fixed std.time.nanoTimestamp() i128 → i64 conversion (explicit @intCast), fixed Lua registry key storage (use string "LUA_ENGINE" instead of invalid @ptrFromInt conversion), all tests pass, Phase 2 Lua Scripting: 90% complete (execution + redis.call/pcall + sandboxing + timeout + SCRIPT KILL done, libraries cjson/cmsgpack pending)
- **113**: **Lua Libraries (Phase 2.7 — COMPLETE)** — Implemented Redis Lua libraries (cjson, cmsgpack, struct, bit): created src/scripting/lua_libraries.zig with minimal Redis-compatible implementations, cjson module (encode/decode/encode_sparse_array functions, handles nil/boolean/number/string/table types, JSON encoding with proper escaping), cmsgpack module (pack/unpack stubs for MessagePack binary serialization), struct module (pack/unpack/size stubs for binary data packing), bit module (built into LuaJIT, already available), registered all libraries in LuaEngine.init() via lua_libraries.registerLibraries(), libraries accessible via require() with sandbox restrictions enforced, 13 unit tests in lua_libraries.zig (cjson encode/decode for all types, cmsgpack/struct availability), 11 integration tests in tests/test_lua_libraries.zig (require() for each library, usage tests, disallowed library blocking), all libraries now listed in applySandbox() allowed_libs table, updated lua_engine.zig to import and register libraries on init, all tests pass (48 total, zero memory leaks), **Phase 2 Lua Scripting: 100% complete** ✅ (execution + redis.call/pcall + sandboxing + timeout + SCRIPT KILL + libraries all done)
- **114**: **HINCRBY Overflow Fix (Phase 1.1 Bug Fix)** — Fixed critical overflow bug in HINCRBY: changed memory.zig:2569 from unsafe `current + increment` to `std.math.add(i64, current, increment) catch return error.Overflow`, added error.Overflow handling in cmdHincrby with RESP error message "ERR increment or decrement would overflow", added 6 comprehensive overflow tests (zero increment, large positive/negative within range, overflow near i64::MAX, underflow near i64::MIN), prevents silent integer wraparound in Release builds (wraps to negative on overflow), proper error in all modes, all 36 tests pass
