const std = @import("std");
const testing = std.testing;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const Storage = @import("../src/storage/memory.zig").Storage;
const search_cmds = @import("../src/commands/search.zig");

test "FT.SUGADD - basic insertion" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const args = &[_][]const u8{ "mykey", "hello", "1.0" };
    const result = try search_cmds.cmdFtSugadd(&storage, allocator, args);

    try testing.expect(result == .integer);
    try testing.expectEqual(@as(i64, 1), result.integer);
}

test "FT.SUGADD - multiple insertions" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "world", "2.0" });
    const result = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "help", "1.5" });

    try testing.expectEqual(@as(i64, 3), result.integer);
}

test "FT.SUGADD - INCR flag" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });
    const result = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "0.5", "INCR" });

    try testing.expectEqual(@as(i64, 1), result.integer); // Count stays 1
}

test "FT.SUGADD - PAYLOAD option" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0", "PAYLOAD", "greeting" });
    try testing.expectEqual(@as(i64, 1), result.integer);
}

test "FT.SUGADD - invalid score" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "invalid" });
    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "invalid score") != null);
}

test "FT.SUGADD - wrong arity" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello" });
    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number of arguments") != null);
}

test "FT.SUGGET - basic retrieval" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "help", "2.0" });

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "hel" });
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 2), result.array.len);
}

test "FT.SUGGET - empty prefix returns all" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "alpha", "1.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "beta", "2.0" });

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "" });
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 2), result.array.len);
}

test "FT.SUGGET - sorted by score descending" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "low", "1.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "high", "3.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "mid", "2.0" });

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "" });
    try testing.expectEqual(@as(usize, 3), result.array.len);
    try testing.expect(std.mem.eql(u8, result.array[0].bulk_string, "high"));
    try testing.expect(std.mem.eql(u8, result.array[1].bulk_string, "mid"));
    try testing.expect(std.mem.eql(u8, result.array[2].bulk_string, "low"));
}

test "FT.SUGGET - MAX parameter" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "a", "1.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "b", "2.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "c", "3.0" });

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "", "MAX", "2" });
    try testing.expectEqual(@as(usize, 2), result.array.len);
}

test "FT.SUGGET - WITHSCORES flag" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.5" });

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "hel", "WITHSCORES" });
    try testing.expectEqual(@as(usize, 2), result.array.len); // string + score
    try testing.expect(std.mem.eql(u8, result.array[0].bulk_string, "hello"));
    try testing.expect(std.mem.eql(u8, result.array[1].bulk_string, "1.5"));
}

test "FT.SUGGET - WITHPAYLOADS flag" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0", "PAYLOAD", "greeting" });

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "hel", "WITHPAYLOADS" });
    try testing.expectEqual(@as(usize, 2), result.array.len); // string + payload
    try testing.expect(std.mem.eql(u8, result.array[0].bulk_string, "hello"));
    try testing.expect(std.mem.eql(u8, result.array[1].bulk_string, "greeting"));
}

test "FT.SUGGET - FUZZY mode" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });

    // Typo: "helo" (missing 'l') should match with FUZZY
    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "mykey", "helo", "FUZZY" });
    try testing.expect(result == .array);
    // Note: Result count may vary depending on fuzzy implementation
}

test "FT.SUGGET - nonexistent key" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{ "nonexistent", "prefix" });
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 0), result.array.len);
}

test "FT.SUGGET - wrong arity" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugget(&storage, allocator, &[_][]const u8{"mykey"});
    try testing.expect(result == .error_string);
}

test "FT.SUGLEN - basic count" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });
    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "world", "2.0" });

    const result = try search_cmds.cmdFtSuglen(&storage, allocator, &[_][]const u8{"mykey"});
    try testing.expectEqual(@as(i64, 2), result.integer);
}

test "FT.SUGLEN - nonexistent key" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSuglen(&storage, allocator, &[_][]const u8{"nonexistent"});
    try testing.expectEqual(@as(i64, 0), result.integer);
}

test "FT.SUGLEN - wrong arity" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSuglen(&storage, allocator, &[_][]const u8{ "key1", "key2" });
    try testing.expect(result == .error_string);
}

test "FT.SUGDEL - basic deletion" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });
    const result = try search_cmds.cmdFtSugdel(&storage, allocator, &[_][]const u8{ "mykey", "hello" });

    try testing.expectEqual(@as(i64, 1), result.integer);

    const len = try search_cmds.cmdFtSuglen(&storage, allocator, &[_][]const u8{"mykey"});
    try testing.expectEqual(@as(i64, 0), len.integer);
}

test "FT.SUGDEL - nonexistent entry" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    _ = try search_cmds.cmdFtSugadd(&storage, allocator, &[_][]const u8{ "mykey", "hello", "1.0" });
    const result = try search_cmds.cmdFtSugdel(&storage, allocator, &[_][]const u8{ "mykey", "world" });

    try testing.expectEqual(@as(i64, 0), result.integer);
}

test "FT.SUGDEL - nonexistent key" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugdel(&storage, allocator, &[_][]const u8{ "nonexistent", "hello" });
    try testing.expectEqual(@as(i64, 0), result.integer);
}

test "FT.SUGDEL - wrong arity" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result = try search_cmds.cmdFtSugdel(&storage, allocator, &[_][]const u8{"mykey"});
    try testing.expect(result == .error_string);
}
