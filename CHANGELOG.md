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
