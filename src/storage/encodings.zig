/// Encoding optimizations configuration
///
/// Redis uses different internal encodings to save memory for small values:
/// - embstr: Embed short strings directly (saves allocation overhead)
/// - intset: Compact sorted integer arrays for sets
/// - listpack: Compact encoding for lists/hashes/sorted sets
///
/// This module defines thresholds and helpers for automatic encoding transitions.

const std = @import("std");

/// Embedded string threshold (Redis default: 44 bytes)
/// Strings <= this size are embedded directly in Value struct
pub const EMBSTR_MAX_LEN: usize = 44;

/// Maximum intset entries (Redis default: 512)
/// Sets with <= this many integer entries use intset encoding
pub const INTSET_MAX_ENTRIES: u32 = 512;

/// Maximum listpack size (Redis default: 8 KB = 8192 bytes)
/// Lists/hashes/sorted sets <= this size use listpack encoding
pub const LISTPACK_MAX_SIZE: u32 = 8192;

/// Maximum listpack entries for lists (Redis default: 512)
pub const LIST_MAX_LISTPACK_ENTRIES: u32 = 512;

/// Maximum listpack entries for hashes (Redis default: 512)
pub const HASH_MAX_LISTPACK_ENTRIES: u32 = 512;

/// Maximum listpack entries for sorted sets (Redis default: 128)
pub const ZSET_MAX_LISTPACK_ENTRIES: u32 = 128;

/// Maximum listpack value size for lists (Redis default: 64 bytes)
pub const LIST_MAX_LISTPACK_VALUE: u32 = 64;

/// Maximum listpack value size for hashes (Redis default: 64 bytes)
pub const HASH_MAX_LISTPACK_VALUE: u32 = 64;

/// Maximum listpack value size for sorted sets (Redis default: 64 bytes)
pub const ZSET_MAX_LISTPACK_VALUE: u32 = 64;

/// Encoding configuration (can be modified via CONFIG SET)
pub const EncodingConfig = struct {
    embstr_max_len: usize,
    intset_max_entries: u32,
    list_max_listpack_entries: u32,
    list_max_listpack_value: u32,
    hash_max_listpack_entries: u32,
    hash_max_listpack_value: u32,
    zset_max_listpack_entries: u32,
    zset_max_listpack_value: u32,

    pub fn init() EncodingConfig {
        return EncodingConfig{
            .embstr_max_len = EMBSTR_MAX_LEN,
            .intset_max_entries = INTSET_MAX_ENTRIES,
            .list_max_listpack_entries = LIST_MAX_LISTPACK_ENTRIES,
            .list_max_listpack_value = LIST_MAX_LISTPACK_VALUE,
            .hash_max_listpack_entries = HASH_MAX_LISTPACK_ENTRIES,
            .hash_max_listpack_value = HASH_MAX_LISTPACK_VALUE,
            .zset_max_listpack_entries = ZSET_MAX_LISTPACK_ENTRIES,
            .zset_max_listpack_value = ZSET_MAX_LISTPACK_VALUE,
        };
    }
};

/// Check if string should use embstr encoding
pub fn shouldUseEmbstr(len: usize, config: *const EncodingConfig) bool {
    return len <= config.embstr_max_len;
}

/// Check if set should use intset encoding
pub fn shouldUseIntset(entries: u32, all_integers: bool, config: *const EncodingConfig) bool {
    return all_integers and entries <= config.intset_max_entries;
}

/// Check if list should use listpack encoding
pub fn shouldUseListpack_List(entries: u32, max_value_size: u32, config: *const EncodingConfig) bool {
    return entries <= config.list_max_listpack_entries and max_value_size <= config.list_max_listpack_value;
}

/// Check if hash should use listpack encoding
pub fn shouldUseListpack_Hash(entries: u32, max_value_size: u32, config: *const EncodingConfig) bool {
    return entries <= config.hash_max_listpack_entries and max_value_size <= config.hash_max_listpack_value;
}

/// Check if sorted set should use listpack encoding
pub fn shouldUseListpack_Zset(entries: u32, max_value_size: u32, config: *const EncodingConfig) bool {
    return entries <= config.zset_max_listpack_entries and max_value_size <= config.zset_max_listpack_value;
}

// ============================================================================
// Tests
// ============================================================================

test "encoding: embstr threshold" {
    const config = EncodingConfig.init();
    try std.testing.expect(shouldUseEmbstr(10, &config));
    try std.testing.expect(shouldUseEmbstr(44, &config));
    try std.testing.expect(!shouldUseEmbstr(45, &config));
    try std.testing.expect(!shouldUseEmbstr(1000, &config));
}

test "encoding: intset threshold" {
    const config = EncodingConfig.init();
    try std.testing.expect(shouldUseIntset(10, true, &config));
    try std.testing.expect(shouldUseIntset(512, true, &config));
    try std.testing.expect(!shouldUseIntset(513, true, &config));
    try std.testing.expect(!shouldUseIntset(10, false, &config)); // Not all integers
}

test "encoding: listpack threshold for lists" {
    const config = EncodingConfig.init();
    try std.testing.expect(shouldUseListpack_List(100, 32, &config));
    try std.testing.expect(shouldUseListpack_List(512, 64, &config));
    try std.testing.expect(!shouldUseListpack_List(513, 64, &config)); // Too many entries
    try std.testing.expect(!shouldUseListpack_List(100, 65, &config)); // Value too large
}

test "encoding: custom config" {
    var config = EncodingConfig.init();
    config.embstr_max_len = 20;

    try std.testing.expect(shouldUseEmbstr(20, &config));
    try std.testing.expect(!shouldUseEmbstr(21, &config));
}
