const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const sorted_sets = zoltraak.sorted_sets;

// Iteration 340: ZADD/ZINCRBY NaN score validation
//
// Redis rejects NaN scores with an error:
//   ZADD key nan member -> ERR score is not a valid float
//   ZINCRBY key nan member -> ERR value is not a valid float
//   ZINCRBY key +inf member (member score = -inf) -> ERR resulting score is not a number (NaN)
//
// Previously: NaN scores were silently stored, corrupting the sorted set.

test "ZADD - rejects NaN score lowercase" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "nan" },
        .{ .bulk_string = "m1" },
    };
    const result = try sorted_sets.cmdZadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR score is not a valid float\r\n", result);
    // NaN must not have been stored
    try std.testing.expectEqual(@as(usize, 0), storage.zcard("zs") orelse 0);
}

test "ZADD - rejects NaN score uppercase NaN" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "NaN" },
        .{ .bulk_string = "m1" },
    };
    const result = try sorted_sets.cmdZadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR score is not a valid float\r\n", result);
}

test "ZADD - accepts +inf score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "highscore" },
    };
    const result = try sorted_sets.cmdZadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);
    // Verify the score is actually +inf
    const score = storage.zscore("zs", "highscore") orelse return error.ExpectedScore;
    try std.testing.expect(std.math.isInf(score) and score > 0);
}

test "ZADD - accepts -inf score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "lowscore" },
    };
    const result = try sorted_sets.cmdZadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);
    const score = storage.zscore("zs", "lowscore") orelse return error.ExpectedScore;
    try std.testing.expect(std.math.isInf(score) and score < 0);
}

test "ZADD INCR - rejects NaN result from pos-inf incremented by neg-inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Seed member with +inf score
    const setup = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "m" },
    };
    const r_setup = try sorted_sets.cmdZadd(allocator, storage, &setup, &pubsub, 0);
    defer allocator.free(r_setup);
    try std.testing.expectEqualStrings(":1\r\n", r_setup);

    // INCR by -inf: +inf + (-inf) = NaN
    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "INCR" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "m" },
    };
    const result = try sorted_sets.cmdZadd(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR resulting score is not a number (NaN)\r\n", result);

    // Member score must remain +inf (not corrupted by NaN attempt)
    const score = storage.zscore("zs", "m") orelse return error.MemberGone;
    try std.testing.expect(std.math.isInf(score) and score > 0);
}

test "ZINCRBY - rejects NaN increment" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "nan" },
        .{ .bulk_string = "m" },
    };
    const result = try sorted_sets.cmdZincrby(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR value is not a valid float\r\n", result);
}

test "ZINCRBY - rejects NaN result from pos-inf incremented by neg-inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Seed member with +inf
    const setup = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "m" },
    };
    const r_setup = try sorted_sets.cmdZadd(allocator, storage, &setup, &pubsub, 0);
    defer allocator.free(r_setup);

    // ZINCRBY by -inf: +inf + (-inf) = NaN
    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "m" },
    };
    const result = try sorted_sets.cmdZincrby(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR resulting score is not a number (NaN)\r\n", result);

    // Score must be unchanged (still +inf)
    const score = storage.zscore("zs", "m") orelse return error.MemberGone;
    try std.testing.expect(std.math.isInf(score) and score > 0);
}

test "ZINCRBY - valid +inf increment on finite score gives +inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Start at 5
    const setup = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "m" },
    };
    const r_setup = try sorted_sets.cmdZadd(allocator, storage, &setup, &pubsub, 0);
    defer allocator.free(r_setup);

    // Increment by +inf → result is +inf
    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "m" },
    };
    const result = try sorted_sets.cmdZincrby(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$3\r\ninf\r\n", result);
}

test "ZINCRBY - neg-inf incremented by pos-inf gives NaN error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    // Seed member with -inf
    const setup = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "m" },
    };
    const r_setup = try sorted_sets.cmdZadd(allocator, storage, &setup, &pubsub, 0);
    defer allocator.free(r_setup);

    // ZINCRBY by +inf: -inf + (+inf) = NaN
    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "zs" },
        .{ .bulk_string = "+inf" },
        .{ .bulk_string = "m" },
    };
    const result = try sorted_sets.cmdZincrby(allocator, storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR resulting score is not a number (NaN)\r\n", result);
}
