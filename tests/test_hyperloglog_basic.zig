const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const PubSub = zoltraak.pubsub.PubSub;
const RespValue = zoltraak.protocol.RespValue;
const hll = zoltraak.hyperloglog_commands;

// Regression test: PFADD must store key at args[1], not args[0].
// Before the fix, args[0] (the command name "PFADD") was used as the key,
// causing EXISTS to return 0 and PFCOUNT to return 0 for the intended key.
test "PFADD - key stored at correct name (not command name)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "myhll" },
        .{ .bulk_string = "a" },
        .{ .bulk_string = "b" },
    };
    const result = try hll.cmdPfadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    // PFADD should return :1\r\n (register updated)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // "myhll" must exist — "PFADD" must NOT exist
    try std.testing.expect(storage.exists("myhll"));
    try std.testing.expect(!storage.exists("PFADD"));
}

test "PFADD - returns 0 when no registers updated (duplicate elements)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args1 = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "k" }, .{ .bulk_string = "x" },
    };
    const r1 = try hll.cmdPfadd(allocator, storage, &args1, &pubsub, 0);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    const args2 = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "k" }, .{ .bulk_string = "x" },
    };
    const r2 = try hll.cmdPfadd(allocator, storage, &args2, &pubsub, 0);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":0\r\n", r2);
}

test "PFCOUNT - returns non-zero count for populated HLL" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const add_args = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "hll" },
        .{ .bulk_string = "a" },     .{ .bulk_string = "b" },    .{ .bulk_string = "c" },
    };
    const ar = try hll.cmdPfadd(allocator, storage, &add_args, &pubsub, 0);
    defer allocator.free(ar);

    const count_args = [_]RespValue{
        .{ .bulk_string = "PFCOUNT" },
        .{ .bulk_string = "hll" },
    };
    const cr = try hll.cmdPfcount(allocator, storage, &count_args);
    defer allocator.free(cr);

    try std.testing.expect(std.mem.startsWith(u8, cr, ":"));
    const num_str = cr[1 .. cr.len - 2];
    const count = try std.fmt.parseInt(i64, num_str, 10);
    try std.testing.expect(count > 0);
}

test "PFCOUNT - returns 0 for nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PFCOUNT" },
        .{ .bulk_string = "no_such_key" },
    };
    const result = try hll.cmdPfcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "PFMERGE - merges source HLLs into destination" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const add1 = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "src1" },
        .{ .bulk_string = "a" },     .{ .bulk_string = "b" },
    };
    const r1 = try hll.cmdPfadd(allocator, storage, &add1, &pubsub, 0);
    defer allocator.free(r1);

    const add2 = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "src2" },
        .{ .bulk_string = "c" },     .{ .bulk_string = "d" },
    };
    const r2 = try hll.cmdPfadd(allocator, storage, &add2, &pubsub, 0);
    defer allocator.free(r2);

    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "dst" },
        .{ .bulk_string = "src1" },
        .{ .bulk_string = "src2" },
    };
    const mr = try hll.cmdPfmerge(allocator, storage, &merge_args, &pubsub, 0);
    defer allocator.free(mr);
    try std.testing.expectEqualStrings("+OK\r\n", mr);

    // dst must exist
    try std.testing.expect(storage.exists("dst"));

    // PFCOUNT dst should be >= 2 (approx union of 4 unique elements)
    const count_args = [_]RespValue{
        .{ .bulk_string = "PFCOUNT" },
        .{ .bulk_string = "dst" },
    };
    const cr = try hll.cmdPfcount(allocator, storage, &count_args);
    defer allocator.free(cr);
    const num_str = cr[1 .. cr.len - 2];
    const count = try std.fmt.parseInt(i64, num_str, 10);
    try std.testing.expect(count >= 2);
}

test "PFADD - WRONGTYPE error on non-HLL key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    _ = try storage.set("strkey", "hello", null);

    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "strkey" },
        .{ .bulk_string = "elem" },
    };
    const result = try hll.cmdPfadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "-WRONGTYPE") != null);
}
