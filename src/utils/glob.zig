const std = @import("std");
const zuda = @import("zuda");

/// Match a glob pattern against a string (Redis KEYS pattern syntax).
/// Supports:
///   *       — matches any sequence of characters (including empty)
///   ?       — matches exactly one character
///   [abc]   — matches any of the listed characters
///   [a-z]   — matches any character in the range
///   [^abc]  — negated character class
///
/// Migrated to zuda.algorithms.string.globMatch in Iteration 119.
pub fn matchGlob(pattern: []const u8, str: []const u8) bool {
    return zuda.algorithms.string.globMatch(pattern, str);
}

// ── Unit tests ────────────────────────────────────────────────────────────────

test "glob - exact match" {
    try std.testing.expect(matchGlob("hello", "hello"));
    try std.testing.expect(!matchGlob("hello", "hell"));
    try std.testing.expect(!matchGlob("hello", "helloo"));
}

test "glob - star wildcard" {
    try std.testing.expect(matchGlob("*", "anything"));
    try std.testing.expect(matchGlob("*", ""));
    try std.testing.expect(matchGlob("h*llo", "hello"));
    try std.testing.expect(matchGlob("h*llo", "hllo"));
    try std.testing.expect(matchGlob("h*llo", "heeeello"));
    try std.testing.expect(!matchGlob("h*llo", "hworld"));
    try std.testing.expect(matchGlob("*llo", "hello"));
    try std.testing.expect(matchGlob("hel*", "hello"));
    try std.testing.expect(matchGlob("h*l*o", "hello"));
}

test "glob - question mark wildcard" {
    try std.testing.expect(matchGlob("h?llo", "hello"));
    try std.testing.expect(matchGlob("h?llo", "hallo"));
    try std.testing.expect(!matchGlob("h?llo", "hllo"));
    try std.testing.expect(!matchGlob("h?llo", "heello"));
}

test "glob - character class" {
    try std.testing.expect(matchGlob("h[ae]llo", "hello"));
    try std.testing.expect(matchGlob("h[ae]llo", "hallo"));
    try std.testing.expect(!matchGlob("h[ae]llo", "hillo"));
}

test "glob - character range" {
    try std.testing.expect(matchGlob("h[a-e]llo", "hello"));
    try std.testing.expect(matchGlob("h[a-e]llo", "hallo"));
    try std.testing.expect(!matchGlob("h[a-e]llo", "hillo"));
}

test "glob - negated character class" {
    try std.testing.expect(matchGlob("h[^ae]llo", "hillo"));
    try std.testing.expect(!matchGlob("h[^ae]llo", "hello"));
    try std.testing.expect(!matchGlob("h[^ae]llo", "hallo"));
}

test "glob - empty pattern and string" {
    try std.testing.expect(matchGlob("", ""));
    try std.testing.expect(!matchGlob("", "a"));
    try std.testing.expect(!matchGlob("a", ""));
}

test "glob - KEYS patterns" {
    // Common Redis KEYS usage
    try std.testing.expect(matchGlob("*", "user:1"));
    try std.testing.expect(matchGlob("user:*", "user:1"));
    try std.testing.expect(matchGlob("user:*", "user:1000"));
    try std.testing.expect(!matchGlob("user:*", "session:1"));
    try std.testing.expect(matchGlob("?ello", "hello"));
    try std.testing.expect(matchGlob("?ello", "jello"));
}
