const std = @import("std");

/// Match a glob pattern against a string (Redis KEYS pattern syntax).
/// Supports:
///   *       — matches any sequence of characters (including empty)
///   ?       — matches exactly one character
///   [abc]   — matches any of the listed characters
///   [a-z]   — matches any character in the range
///   [^abc]  — negated character class
pub fn matchGlob(pattern: []const u8, str: []const u8) bool {
    return matchAt(pattern, 0, str, 0);
}

/// Recursive helper that tracks positions in both pattern and string.
fn matchAt(pattern: []const u8, pi: usize, str: []const u8, si: usize) bool {
    var p = pi;
    var s = si;

    while (p < pattern.len) {
        const pc = pattern[p];

        switch (pc) {
            '*' => {
                // Skip consecutive stars
                while (p < pattern.len and pattern[p] == '*') p += 1;
                // If star is at end of pattern, match everything remaining
                if (p == pattern.len) return true;
                // Try matching the rest of the pattern at every position in str
                var si2 = s;
                while (si2 <= str.len) : (si2 += 1) {
                    if (matchAt(pattern, p, str, si2)) return true;
                }
                return false;
            },
            '?' => {
                // Must have at least one character to consume
                if (s >= str.len) return false;
                p += 1;
                s += 1;
            },
            '[' => {
                if (s >= str.len) return false;
                const ch = str[s];
                p += 1; // skip '['
                const negate = p < pattern.len and pattern[p] == '^';
                if (negate) p += 1;

                var matched = false;
                var first = true;
                while (p < pattern.len and (first or pattern[p] != ']')) {
                    first = false;
                    if (p + 2 < pattern.len and pattern[p + 1] == '-' and pattern[p + 2] != ']') {
                        // Range: e.g. a-z
                        if (ch >= pattern[p] and ch <= pattern[p + 2]) matched = true;
                        p += 3;
                    } else {
                        if (ch == pattern[p]) matched = true;
                        p += 1;
                    }
                }
                // skip closing ']'
                if (p < pattern.len and pattern[p] == ']') p += 1;

                if (matched == negate) return false; // negate XOR matched must be true
                s += 1;
            },
            '\\' => {
                // Escape: treat next character literally
                p += 1;
                if (p >= pattern.len) return false;
                if (s >= str.len) return false;
                if (pattern[p] != str[s]) return false;
                p += 1;
                s += 1;
            },
            else => {
                if (s >= str.len or pc != str[s]) return false;
                p += 1;
                s += 1;
            },
        }
    }

    // Pattern exhausted — must have consumed all of str too
    return s == str.len;
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
