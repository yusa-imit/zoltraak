//! Deadline arithmetic for the polling waits of WAIT and WAITAOF.
//!
//! The timeout of those commands is client data: `WAIT 1 18446744073709551615` is a valid
//! request and must cost us a long wait, not an overflow trap that kills the server. All
//! arithmetic here saturates, and `poll_count_max` bounds the polling loop itself.
//!
//! Allocation: none. The functions are pure; the caller owns the clock and the sleep.

const std = @import("std");
const assert = std.debug.assert;

/// Sleep between two polls of the replica offsets.
pub const poll_interval_ms: u32 = 10;

/// Upper bound on polls of one wait: one year at `poll_interval_ms`. A longer timeout is
/// treated as a year, which no client observes as different.
pub const poll_count_max: u64 = @as(u64, 365 * 24 * 60 * 60 * 1000) / poll_interval_ms;

/// Absolute deadline, in milliseconds on the same clock as `now_ms`, saturating at `u64` max.
/// Precondition: `now_ms` is a wall-clock time at or after the epoch.
pub fn deadline_ms(now_ms: i64, timeout_ms: u64) u64 {
    assert(now_ms >= 0);
    const now: u64 = @intCast(now_ms);
    const deadline = now +| timeout_ms;
    assert(deadline >= now);
    return deadline;
}

/// True once `now_ms` has reached `deadline`. Precondition: `now_ms` is not negative.
pub fn expired(now_ms: i64, deadline: u64) bool {
    assert(now_ms >= 0);
    return @as(u64, @intCast(now_ms)) >= deadline;
}

comptime {
    assert(poll_count_max == 3_153_600_000);
}

test "deadline_ms adds the timeout to now" {
    try std.testing.expectEqual(@as(u64, 1_500), deadline_ms(1_000, 500));
}

test "deadline_ms with a zero timeout is now" {
    try std.testing.expectEqual(@as(u64, 1_000), deadline_ms(1_000, 0));
}

test "deadline_ms saturates instead of overflowing on the largest timeout" {
    const timeout_max = std.math.maxInt(u64);
    try std.testing.expectEqual(timeout_max, deadline_ms(1_700_000_000_000, timeout_max));
    try std.testing.expectEqual(timeout_max, deadline_ms(0, timeout_max));
}

test "deadline_ms is exact up to the saturation boundary" {
    const timeout: u64 = std.math.maxInt(u64) - 1_000;
    try std.testing.expectEqual(std.math.maxInt(u64), deadline_ms(1_000, timeout));
    try std.testing.expectEqual(std.math.maxInt(u64) - 1, deadline_ms(999, timeout));
}

test "expired is false before the deadline and true at and after it" {
    try std.testing.expect(!expired(1_499, 1_500));
    try std.testing.expect(expired(1_500, 1_500));
    try std.testing.expect(expired(1_501, 1_500));
}

test "expired is never true for a saturated deadline" {
    const deadline = deadline_ms(1_700_000_000_000, std.math.maxInt(u64));
    try std.testing.expect(!expired(std.math.maxInt(i64), deadline));
}
