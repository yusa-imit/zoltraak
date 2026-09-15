# Changelog

All notable changes to this project are documented in this file. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
`citadel/protocol/VERSIONING.md`.

Releases before this file existed (up to and including `v0.2.14`) are not backfilled here;
see `git tag -l 'v*'` and the corresponding GitHub releases for that history.

## [Unreleased]

### Added

- `src/protocol/parser.zig` (plan 001 item 10, assertion baseline, scoped this cycle to this
  module): Tiger Style pre/post-condition assertions on every public and private function (35
  asserts, up from 0), plus a new `ParseError.LengthTooLarge` variant enforcing `bulk_len_max`
  (512 MiB) and `multibulk_count_max` (1,048,576) ceilings on declared bulk-string/bulk-error/
  verbatim-string lengths and array/map/set/push element counts — a hostile length prefix is now
  a typed error before any allocation, instead of an unbounded allocation attempt.
- `src/protocol/writer.zig` (plan 001 item 11, assertion baseline continuation): Tiger Style
  pre/post-condition assertions on every public function (~50 asserts, up from 0) — RESP frame
  CRLF-termination and exact/minimum output-length postconditions, buffer-growth invariants
  across recursive `writeValue` calls, and a 3-byte format-code precondition on
  `writeVerbatimString`. The remaining three hot modules (`storage/memory.zig`, `server.zig`,
  `commands/strings.zig`) are deferred to future cycles.
- `src/server.zig` (plan 001 item 11, assertion baseline continuation): Tiger Style pre/post-
  condition assertions on `ServerStats`, `ShutdownState`, `GossipTask`, `Server.init`/`deinit`,
  `performShutdown`, and `detectPsync` — atomic-counter monotonicity, uptime non-negativity,
  requested-shutdown/request-payload consistency, gossip-task running/thread-handle invariants,
  database-count postconditions, and a case-insensitive PSYNC match proof. New unit tests cover
  the four testable pieces this file previously had none for; `start`/`handleConnection` (the
  socket-bound accept loop) stay covered by the existing shell integration suite, not new unit
  tests, and were kept at their `tidy-baseline.zon` line-count ceiling.
- `src/commands/strings.zig` (plan 001 item 11, assertion baseline continuation): Tiger Style
  pre/post-condition assertions (0 → 46 asserts) — NX/XX and KEEPTTL/EX mutual exclusion,
  key-existence postconditions on the SET/INCR/DECR family, i64 negation-overflow proof, NaN/Inf
  guards on `INCRBYFLOAT`, and paired-arg-count invariants on MSET/HSET-style commands.
- `src/storage/memory.zig` (plan 001 item 11, assertion baseline — final module): Tiger Style
  pre/post-condition assertions across `Storage.init`/`deinit`, `set`/`get`/`del`/`exists`,
  `getType`/`setExpiry`/`getTtlMs`, `incrby`/`incrbyfloat`, `checkMemoryLimitAndEvict`, and
  `renamekey`, plus 34 new unit tests covering lifecycle/eviction/stats/introspection and the
  string-counter family. This completes the assertion baseline across all five hot modules
  named in plan 001 item 11: `protocol/{parser,writer}.zig`, `server.zig`,
  `commands/strings.zig`, `storage/memory.zig`.
- `zig build tidy` (gates `zig build test`): Tiger Style mechanical checks over `src/` — line
  length, function length, `std.debug.print`/`std.time.*`/unproven-`catch unreachable`/`usize`-
  in-wire-format ban list, and `//!` module headers — checked against a shrink-only baseline in
  `tidy-baseline.zon`. Regenerate the baseline with `zig build tidy-record` after fixing
  violations. See `tools/tidy.zig`.

### Changed

- Untracked `src/.DS_Store` and dropped stale `.gitignore` entries (`check_existing`, `main`,
  `verify_*`, `/test_*`) left over from the pre-restructure build scripts.
- `build.zig`: replaced all 105 `Step.Compile.linkSystemLibrary`/`.linkLibC`/`.addIncludePath`/
  `.addLibraryPath` call sites (removed in Zig 0.16) with their `root_module`-based equivalents,
  which exist identically on 0.15.2 and 0.16.0. Fixes the first 0.16 build-script wall (plan 001
  item 3); zoltraak's own `build.zig` now compiles under 0.16.0 — remaining 0.16 errors are all
  inside vendored `sailor`/`zuda` dependency build scripts, blocked on their v3.0.0 tags.
- Renamed `std.ArrayList(T){}`/`std.ArrayListUnmanaged(T){}` literal-init call sites to
  `.empty` (471 sites, 52 files) and `std.heap.GeneralPurposeAllocator(.{}){}` to
  `std.heap.DebugAllocator(.{}){}` (3 sites: `src/main.zig`, `src/cli.zig`,
  `src/commands/command_registry.zig`) — plan 001 item 4, mechanical renames A (partial).
  Both aliases already exist in the pinned Zig 0.15.2 stdlib, so this is forward-compatible
  with 0.16.0 without changing behavior on the current toolchain.
