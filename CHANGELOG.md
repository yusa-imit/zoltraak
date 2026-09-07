# Changelog

All notable changes to this project are documented in this file. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
`citadel/protocol/VERSIONING.md`.

Releases before this file existed (up to and including `v0.2.14`) are not backfilled here;
see `git tag -l 'v*'` and the corresponding GitHub releases for that history.

## [Unreleased]

### Added

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
