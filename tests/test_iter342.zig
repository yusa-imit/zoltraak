const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const RespProtocol = zoltraak.client.RespProtocol;
const ss_cmds = zoltraak.sorted_sets;

// Iteration 342: Sorted set +inf score formatting fix
//
// Redis returns "+inf" for positive infinity scores (not "inf").
// Zig's {d} format produces "inf" for std.math.inf(f64), which does not match
// Redis protocol expectations. All sorted set commands that return scores now
// use a formatScore() helper that maps +inf → "+inf", -inf → "-inf".

test "ZSCORE returns +inf for positive infinity score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // ZADD key +inf member
    const args_zadd = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "alpha" },
    };
    const r1 = try ss_cmds.cmdZadd(allocator, storage, &args_zadd, &pubsub, 0);
    defer allocator.free(r1);

    const args_zscore = [_]RespValue{
        .{ .bulk_string = "ZSCORE" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "alpha" },
    };
    const result = try ss_cmds.cmdZscore(allocator, storage, &args_zscore);
    defer allocator.free(result);

    // Redis returns "+inf" not "inf"
    try std.testing.expectEqualStrings("$4\r\n+inf\r\n", result);
}

test "ZSCORE returns -inf for negative infinity score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args_zadd = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "beta" },
    };
    const r1 = try ss_cmds.cmdZadd(allocator, storage, &args_zadd, &pubsub, 0);
    defer allocator.free(r1);

    const args_zscore = [_]RespValue{
        .{ .bulk_string = "ZSCORE" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "beta" },
    };
    const result = try ss_cmds.cmdZscore(allocator, storage, &args_zscore);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$4\r\n-inf\r\n", result);
}

test "ZINCRBY returns +inf when incrementing by +inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Set up a member with score 5
    const args_zadd = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "m" },
    };
    const r1 = try ss_cmds.cmdZadd(allocator, storage, &args_zadd, &pubsub, 0);
    defer allocator.free(r1);

    // ZINCRBY zs +inf m → "+inf"
    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "m" },
    };
    const result = try ss_cmds.cmdZincrby(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$4\r\n+inf\r\n", result);
}

test "ZPOPMIN returns +inf score formatted as +inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args_zadd = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "member1" },
    };
    const r1 = try ss_cmds.cmdZadd(allocator, storage, &args_zadd, &pubsub, 0);
    defer allocator.free(r1);

    const args = [_]RespValue{
        .{ .bulk_string = "ZPOPMIN" },
        .{ .bulk_string = "zs" },
    };
    const result = try ss_cmds.cmdZpopmin(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    // Should contain "member1" and "+inf"
    try std.testing.expect(std.mem.indexOf(u8, result, "+inf") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "member1") != null);
}

test "ZADD INCR with +inf returns +inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // ZADD key INCR +inf member (create new with +inf score)
    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "INCR" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "m" },
    };
    const result = try ss_cmds.cmdZadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$4\r\n+inf\r\n", result);
}

test "ZRANGE WITHSCORES returns +inf for infinity scores" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Add two members: one with finite score, one with +inf
    const args_zadd = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "low" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "high" },
    };
    const r1 = try ss_cmds.cmdZadd(allocator, storage, &args_zadd, &pubsub, 0);
    defer allocator.free(r1);

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANGE" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "-1" },
        .{ .bulk_string = "WITHSCORES" },
    };
    const result = try ss_cmds.cmdZrange(allocator, storage, &args, RespProtocol.RESP2);
    defer allocator.free(result);

    // Result should contain "+inf" (not "inf")
    try std.testing.expect(std.mem.indexOf(u8, result, "+inf") != null);
}

test "ZRANGEBYSCORE WITHSCORES returns correct score for +inf member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args_zadd = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "top" },
    };
    const r1 = try ss_cmds.cmdZadd(allocator, storage, &args_zadd, &pubsub, 0);
    defer allocator.free(r1);

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANGEBYSCORE" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "WITHSCORES" },
    };
    const result = try ss_cmds.cmdZrangebyscore(allocator, storage, &args);
    defer allocator.free(result);

    // Score should be "+inf" not "inf"
    try std.testing.expect(std.mem.indexOf(u8, result, "+inf") != null);
}
