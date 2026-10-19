//! Bounds for the polling waits of XREAD BLOCK and XREADGROUP BLOCK.
//!
//! `BLOCK 0` means "wait for data without a timeout", which a polling loop must still bound:
//! `poll_count_max` caps the loop at one year, a wait no client observes as different from
//! forever. Until the event-driven `storage/blocking.zig` is wired, this is the only limit.
//!
//! Allocation: none. The functions are pure; the caller owns the clock and the sleep.

const std = @import("std");
const assert = std.debug.assert;

/// Sleep between two polls of the watched streams.
pub const poll_interval_ms: u32 = 100;

/// Upper bound on polls of one blocking read: one year at `poll_interval_ms`.
pub const poll_count_max: u64 = @as(u64, 365 * 24 * 60 * 60 * 1000) / poll_interval_ms;

/// True once a wait that started `elapsed_ms` ago has outlived `timeout_ms`. A timeout of zero
/// never expires here: that is `BLOCK 0`, bounded by `poll_count_max` instead.
/// Precondition: `timeout_ms` is not negative (the command parser rejects it).
pub fn timed_out(elapsed_ms: i64, timeout_ms: i64) bool {
    assert(timeout_ms >= 0);
    if (timeout_ms == 0) return false;
    return elapsed_ms >= timeout_ms;
}

comptime {
    assert(poll_count_max == 315_360_000);
}

test "timed_out is false before the timeout and true at and after it" {
    try std.testing.expect(!timed_out(0, 1_000));
    try std.testing.expect(!timed_out(999, 1_000));
    try std.testing.expect(timed_out(1_000, 1_000));
    try std.testing.expect(timed_out(1_001, 1_000));
}

test "timed_out never fires for BLOCK 0, however long the wait" {
    try std.testing.expect(!timed_out(0, 0));
    try std.testing.expect(!timed_out(std.math.maxInt(i64), 0));
}

test "timed_out tolerates a clock that moved backwards" {
    try std.testing.expect(!timed_out(-5_000, 1_000));
    try std.testing.expect(!timed_out(std.math.minInt(i64), 1));
}

test "timed_out handles the largest timeout without overflow" {
    const timeout_max = std.math.maxInt(i64);
    try std.testing.expect(!timed_out(timeout_max - 1, timeout_max));
    try std.testing.expect(timed_out(timeout_max, timeout_max));
}

test "poll_count_max covers one year of polling and the loop range is finite" {
    const year_ms: u64 = 365 * 24 * 60 * 60 * 1000;
    try std.testing.expectEqual(year_ms, poll_count_max * poll_interval_ms);
    var polls: u64 = 0;
    for (0..poll_count_max) |_| {
        polls += 1;
        if (polls == 3) break;
    }
    try std.testing.expectEqual(@as(u64, 3), polls);
}
