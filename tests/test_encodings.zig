const std = @import("std");
const listpack_mod = @import("../src/storage/listpack.zig");
const intset_mod = @import("../src/storage/intset.zig");
const encodings_mod = @import("../src/storage/encodings.zig");

const Listpack = listpack_mod.Listpack;
const Intset = intset_mod.Intset;
const EncodingConfig = encodings_mod.EncodingConfig;

// ============================================================================
// Listpack Integration Tests
// ============================================================================

test "listpack: memory efficiency for small lists" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    // Add 100 small integers
    var i: i64 = 0;
    while (i < 100) : (i += 1) {
        try lp.appendInt(i);
    }

    try std.testing.expectEqual(@as(u16, 100), lp.len());

    // Verify all values
    i = 0;
    while (i < 100) : (i += 1) {
        const entry = try lp.get(@intCast(i));
        try std.testing.expectEqual(i, entry.integer);
    }
}

test "listpack: string and integer mix" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    // Simulate hash field-value pairs
    try lp.appendString("name");
    try lp.appendString("Alice");
    try lp.appendString("age");
    try lp.appendInt(30);
    try lp.appendString("score");
    try lp.appendInt(95);

    try std.testing.expectEqual(@as(u16, 6), lp.len());

    // Verify entries
    var e0 = try lp.get(0);
    defer e0.deinit(allocator);
    try std.testing.expectEqualStrings("name", e0.string);

    var e1 = try lp.get(1);
    defer e1.deinit(allocator);
    try std.testing.expectEqualStrings("Alice", e1.string);

    var e2 = try lp.get(2);
    defer e2.deinit(allocator);
    try std.testing.expectEqualStrings("age", e2.string);

    const e3 = try lp.get(3);
    try std.testing.expectEqual(@as(i64, 30), e3.integer);
}

test "listpack: iteration pattern" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    // Add 20 entries
    var i: i64 = 1;
    while (i <= 20) : (i += 1) {
        try lp.appendInt(i * 10);
    }

    // Iterate and collect
    var iter = lp.iterator();
    var sum: i64 = 0;
    var count: u16 = 0;

    while (try iter.next()) |entry| {
        count += 1;
        switch (entry) {
            .integer => |v| sum += v,
            .string => unreachable,
        }
    }

    try std.testing.expectEqual(@as(u16, 20), count);
    try std.testing.expectEqual(@as(i64, 2100), sum); // 10+20+...+200 = 2100
}

test "listpack: boundary value encodings" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    // Test all encoding thresholds
    try lp.appendInt(0); // 7-bit
    try lp.appendInt(127); // 7-bit max
    try lp.appendInt(128); // Requires 13-bit
    try lp.appendInt(-4096); // 13-bit min
    try lp.appendInt(4095); // 13-bit max
    try lp.appendInt(5000); // Requires 16-bit

    try std.testing.expectEqual(@as(u16, 6), lp.len());

    const values = [_]i64{ 0, 127, 128, -4096, 4095, 5000 };
    for (values, 0..) |expected, idx| {
        const entry = try lp.get(@intCast(idx));
        try std.testing.expectEqual(expected, entry.integer);
    }
}

test "listpack: string length variations" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    // 6-bit string (0-63 bytes)
    try lp.appendString("short");

    // Exactly 63 bytes
    const s63 = "x" ** 63;
    try lp.appendString(s63);

    // 12-bit string (64+ bytes)
    const s64 = "y" ** 64;
    try lp.appendString(s64);

    try std.testing.expectEqual(@as(u16, 3), lp.len());

    var e0 = try lp.get(0);
    defer e0.deinit(allocator);
    try std.testing.expectEqualStrings("short", e0.string);

    var e1 = try lp.get(1);
    defer e1.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 63), e1.string.len);

    var e2 = try lp.get(2);
    defer e2.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 64), e2.string.len);
}

// ============================================================================
// Intset Integration Tests
// ============================================================================

test "intset: memory efficiency for integer sets" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Add 512 integers (Redis default threshold)
    var i: i64 = 0;
    while (i < 512) : (i += 1) {
        try std.testing.expect(try is.add(i));
    }

    try std.testing.expectEqual(@as(u32, 512), is.len());

    // Verify all exist
    i = 0;
    while (i < 512) : (i += 1) {
        try std.testing.expect(is.contains(i));
    }
}

test "intset: encoding upgrade scenarios" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Start with small values (i16)
    try std.testing.expect(try is.add(100));
    try std.testing.expect(try is.add(200));
    try std.testing.expect(try is.add(300));
    try std.testing.expectEqual(@as(u32, 2), is.encoding); // i16

    // Add value requiring i32 upgrade
    const large: i64 = 100000;
    try std.testing.expect(try is.add(large));
    try std.testing.expectEqual(@as(u32, 4), is.encoding); // i32

    // Verify all values still exist after upgrade
    try std.testing.expect(is.contains(100));
    try std.testing.expect(is.contains(200));
    try std.testing.expect(is.contains(300));
    try std.testing.expect(is.contains(large));

    // Values should still be sorted
    try std.testing.expectEqual(@as(i64, 100), try is.get(0));
    try std.testing.expectEqual(@as(i64, 200), try is.get(1));
    try std.testing.expectEqual(@as(i64, 300), try is.get(2));
    try std.testing.expectEqual(large, try is.get(3));
}

test "intset: removal maintains sorted order" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Add: 10, 20, 30, 40, 50
    const values = [_]i64{ 10, 20, 30, 40, 50 };
    for (values) |v| {
        try std.testing.expect(try is.add(v));
    }

    // Remove middle element
    try std.testing.expect(is.remove(30));
    try std.testing.expectEqual(@as(u32, 4), is.len());

    // Verify sorted order: 10, 20, 40, 50
    try std.testing.expectEqual(@as(i64, 10), try is.get(0));
    try std.testing.expectEqual(@as(i64, 20), try is.get(1));
    try std.testing.expectEqual(@as(i64, 40), try is.get(2));
    try std.testing.expectEqual(@as(i64, 50), try is.get(3));
}

test "intset: negative and positive mix" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Add in random order
    try std.testing.expect(try is.add(50));
    try std.testing.expect(try is.add(-100));
    try std.testing.expect(try is.add(0));
    try std.testing.expect(try is.add(-50));
    try std.testing.expect(try is.add(100));

    // Should be sorted: -100, -50, 0, 50, 100
    try std.testing.expectEqual(@as(i64, -100), try is.get(0));
    try std.testing.expectEqual(@as(i64, -50), try is.get(1));
    try std.testing.expectEqual(@as(i64, 0), try is.get(2));
    try std.testing.expectEqual(@as(i64, 50), try is.get(3));
    try std.testing.expectEqual(@as(i64, 100), try is.get(4));
}

test "intset: large negative prepend on upgrade" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Add positive values first
    try std.testing.expect(try is.add(1000));
    try std.testing.expect(try is.add(2000));
    try std.testing.expect(try is.add(3000));

    // Add large negative (forces upgrade and prepend)
    const large_neg: i64 = -100000;
    try std.testing.expect(try is.add(large_neg));

    // Should be sorted with large negative first
    try std.testing.expectEqual(large_neg, try is.get(0));
    try std.testing.expectEqual(@as(i64, 1000), try is.get(1));
    try std.testing.expectEqual(@as(i64, 2000), try is.get(2));
    try std.testing.expectEqual(@as(i64, 3000), try is.get(3));
}

// ============================================================================
// EncodingConfig Integration Tests
// ============================================================================

test "encoding config: threshold checks" {
    var config = EncodingConfig.init();

    // Test embstr threshold
    try std.testing.expect(encodings_mod.shouldUseEmbstr(44, &config));
    try std.testing.expect(!encodings_mod.shouldUseEmbstr(45, &config));

    // Test intset threshold
    try std.testing.expect(encodings_mod.shouldUseIntset(512, true, &config));
    try std.testing.expect(!encodings_mod.shouldUseIntset(513, true, &config));
    try std.testing.expect(!encodings_mod.shouldUseIntset(10, false, &config));

    // Test listpack thresholds
    try std.testing.expect(encodings_mod.shouldUseListpack_List(512, 64, &config));
    try std.testing.expect(!encodings_mod.shouldUseListpack_List(513, 64, &config));
    try std.testing.expect(!encodings_mod.shouldUseListpack_List(512, 65, &config));

    try std.testing.expect(encodings_mod.shouldUseListpack_Hash(512, 64, &config));
    try std.testing.expect(!encodings_mod.shouldUseListpack_Hash(513, 64, &config));

    try std.testing.expect(encodings_mod.shouldUseListpack_Zset(128, 64, &config));
    try std.testing.expect(!encodings_mod.shouldUseListpack_Zset(129, 64, &config));
}

test "encoding config: custom thresholds" {
    var config = EncodingConfig.init();

    // Modify thresholds
    config.intset_max_entries = 256;
    config.list_max_listpack_entries = 1024;

    // Test modified thresholds
    try std.testing.expect(encodings_mod.shouldUseIntset(256, true, &config));
    try std.testing.expect(!encodings_mod.shouldUseIntset(257, true, &config));

    try std.testing.expect(encodings_mod.shouldUseListpack_List(1024, 64, &config));
    try std.testing.expect(!encodings_mod.shouldUseListpack_List(1025, 64, &config));
}

test "encoding: realistic hash simulation" {
    const allocator = std.testing.allocator;

    // Simulate a hash with 100 field-value pairs using listpack
    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        // Field
        const field = try std.fmt.allocPrint(allocator, "field{d}", .{i});
        defer allocator.free(field);
        try lp.appendString(field);

        // Value
        const value = try std.fmt.allocPrint(allocator, "value{d}", .{i});
        defer allocator.free(value);
        try lp.appendString(value);
    }

    try std.testing.expectEqual(@as(u16, 200), lp.len()); // 100 fields + 100 values

    // Verify size is reasonable (should be < 5 KB for this data)
    const size = lp.size();
    try std.testing.expect(size < 5000);
}

test "encoding: realistic sorted set simulation" {
    const allocator = std.testing.allocator;

    // Simulate a sorted set with score-member pairs using listpack
    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    var i: i64 = 0;
    while (i < 50) : (i += 1) {
        // Score (as integer for compact encoding)
        try lp.appendInt(i * 10);

        // Member
        const member = try std.fmt.allocPrint(allocator, "member{d}", .{i});
        defer allocator.free(member);
        try lp.appendString(member);
    }

    try std.testing.expectEqual(@as(u16, 100), lp.len()); // 50 scores + 50 members

    // Verify we can iterate and extract pairs
    var iter = lp.iterator();
    var pair_count: u16 = 0;

    while (pair_count < 50) : (pair_count += 1) {
        // Score
        const score_entry = (try iter.next()).?;
        try std.testing.expect(score_entry == .integer);

        // Member
        var member_entry = (try iter.next()).?;
        defer member_entry.deinit(allocator);
        try std.testing.expect(member_entry == .string);
    }

    try std.testing.expectEqual(@as(u16, 50), pair_count);
}
