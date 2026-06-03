const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const XAddOptions = zoltraak.storage.XAddOptions;

test "stream - XRANGE COUNT 0 returns all entries (no limit)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "b" };
    _ = try storage.xadd("s", "1000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("s", "2000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("s", "3000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("s", "4000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("s", "5000-0", &fields, null, XAddOptions{});

    // COUNT 0 should return all 5 entries (Redis: COUNT 0 = no limit)
    const result = (try storage.xrange(allocator, "s", "-", "+", 0)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 5), result.len);
}

test "stream - XRANGE COUNT 2 limits entries correctly" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "y" };
    _ = try storage.xadd("t", "1000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("t", "2000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("t", "3000-0", &fields, null, XAddOptions{});

    // COUNT 2 should limit to 2 entries
    const result = (try storage.xrange(allocator, "t", "-", "+", 2)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 2), result.len);
}

test "stream - XREVRANGE COUNT 0 returns all entries (no limit)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "p", "q" };
    _ = try storage.xadd("r", "1000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("r", "2000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("r", "3000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("r", "4000-0", &fields, null, XAddOptions{});

    // COUNT 0 should return all 4 entries in reverse order
    const result = (try storage.xrevrange(allocator, "r", "+", "-", 0)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 4), result.len);
    // Verify reverse order: highest ms first
    try std.testing.expectEqual(@as(i64, 4000), result[0].id.ms);
    try std.testing.expectEqual(@as(i64, 1000), result[3].id.ms);
}

test "stream - XREVRANGE COUNT 1 limits to single entry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "k", "v" };
    _ = try storage.xadd("q", "1000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("q", "2000-0", &fields, null, XAddOptions{});
    _ = try storage.xadd("q", "3000-0", &fields, null, XAddOptions{});

    // COUNT 1 returns only the latest entry (XREVRANGE reads newest first)
    const result = (try storage.xrevrange(allocator, "q", "+", "-", 1)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(i64, 3000), result[0].id.ms);
}
