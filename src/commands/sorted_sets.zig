const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// Parse score from string, supporting +inf and -inf
fn parseScore(s: []const u8) !f64 {
    if (std.mem.eql(u8, s, "+inf") or std.mem.eql(u8, s, "inf")) {
        return std.math.inf(f64);
    } else if (std.mem.eql(u8, s, "-inf")) {
        return -std.math.inf(f64);
    } else {
        return std.fmt.parseFloat(f64, s) catch error.InvalidScore;
    }
}

/// ZADD key [NX|XX] [CH] score member [score member ...]
/// Sets members with scores in a sorted set
/// Returns integer - count of new members added (or changed if CH flag set)
pub fn cmdZadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zadd' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse options (NX, XX, CH)
    var options: u8 = 0;
    var arg_idx: usize = 2;

    while (arg_idx < args.len) {
        const arg_str = switch (args[arg_idx]) {
            .bulk_string => |s| s,
            else => break,
        };

        const upper_arg = try std.ascii.allocUpperString(allocator, arg_str);
        defer allocator.free(upper_arg);

        if (std.mem.eql(u8, upper_arg, "NX")) {
            options |= 1;
            arg_idx += 1;
        } else if (std.mem.eql(u8, upper_arg, "XX")) {
            options |= 2;
            arg_idx += 1;
        } else if (std.mem.eql(u8, upper_arg, "CH")) {
            options |= 4;
            arg_idx += 1;
        } else {
            break; // Not an option, must be score
        }
    }

    // Validate NX and XX are not both set
    if ((options & 1) != 0 and (options & 2) != 0) {
        return w.writeError("ERR XX and NX options at the same time are not compatible");
    }

    // Validate score-member pairs
    const remaining = args.len - arg_idx;
    if (remaining == 0 or remaining % 2 != 0) {
        return w.writeError("ERR syntax error");
    }

    const pair_count = remaining / 2;
    var scores = try std.ArrayList(f64).initCapacity(allocator, pair_count);
    defer scores.deinit(allocator);
    var members = try std.ArrayList([]const u8).initCapacity(allocator, pair_count);
    defer members.deinit(allocator);

    var i: usize = arg_idx;
    while (i < args.len) : (i += 2) {
        const score_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR score is not a valid float"),
        };
        const score = parseScore(score_str) catch {
            return w.writeError("ERR score is not a valid float");
        };

        const member = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };

        try scores.append(allocator, score);
        try members.append(allocator, member);
    }

    // Execute ZADD
    const result = storage.zadd(key, scores.items, members.items, options, null) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    // Return added or changed count depending on CH flag
    const ch_flag = (options & 4) != 0;
    const count = if (ch_flag) result.changed else result.added;
    return w.writeInteger(@intCast(count));
}

/// ZREM key member [member ...]
/// Removes members from a sorted set
/// Returns integer - count of members removed
pub fn cmdZrem(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zrem' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var members = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer members.deinit(allocator);

    for (args[2..]) |arg| {
        const member = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };
        try members.append(allocator, member);
    }

    const removed_count = storage.zrem(key, members.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(removed_count));
}

/// ZRANGE key start stop [WITHSCORES]
/// Get members by rank range
/// Returns array of members (interleaved with scores if WITHSCORES)
pub fn cmdZrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zrange' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const stop_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    const stop = std.fmt.parseInt(i64, stop_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Check for WITHSCORES option
    var with_scores = false;
    if (args.len > 4) {
        const option_str = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, option_str);
        defer allocator.free(upper_opt);

        if (std.mem.eql(u8, upper_opt, "WITHSCORES")) {
            with_scores = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const result = try storage.zrange(allocator, key, start, stop, with_scores);

    if (result) |items| {
        defer {
            if (with_scores) {
                // Free score strings (odd indices - allocated by zrange)
                var idx: usize = 1;
                while (idx < items.len) : (idx += 2) {
                    allocator.free(items[idx]);
                }
            }
            allocator.free(items);
        }

        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, items.len);
        defer resp_values.deinit(allocator);

        for (items) |item| {
            try resp_values.append(allocator, RespValue{ .bulk_string = item });
        }

        return w.writeArray(resp_values.items);
    } else {
        return w.writeArray(&[_]RespValue{});
    }
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
/// Get members with scores in range [min, max]
/// Supports exclusive intervals with ( prefix, and -inf/+inf
pub fn cmdZrangebyscore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zrangebyscore' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse min score
    const min_arg = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR min is not a valid float"),
    };
    var min_exclusive = false;
    var min_str = min_arg;
    if (min_str.len > 0 and min_str[0] == '(') {
        min_exclusive = true;
        min_str = min_str[1..];
    }
    const min_score = parseScore(min_str) catch {
        return w.writeError("ERR min is not a valid float");
    };

    // Parse max score
    const max_arg = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR max is not a valid float"),
    };
    var max_exclusive = false;
    var max_str = max_arg;
    if (max_str.len > 0 and max_str[0] == '(') {
        max_exclusive = true;
        max_str = max_str[1..];
    }
    const max_score = parseScore(max_str) catch {
        return w.writeError("ERR max is not a valid float");
    };

    // Parse optional WITHSCORES and LIMIT
    var with_scores = false;
    var limit_offset: ?usize = null;
    var limit_count: ?usize = null;

    var opt_idx: usize = 4;
    while (opt_idx < args.len) {
        const opt_str = switch (args[opt_idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt_str);
        defer allocator.free(upper_opt);

        if (std.mem.eql(u8, upper_opt, "WITHSCORES")) {
            with_scores = true;
            opt_idx += 1;
        } else if (std.mem.eql(u8, upper_opt, "LIMIT")) {
            if (opt_idx + 2 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const offset_str = switch (args[opt_idx + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const count_str = switch (args[opt_idx + 2]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            limit_offset = std.fmt.parseUnsigned(usize, offset_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            const count_val = std.fmt.parseInt(i64, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            // Negative count means no limit (take all)
            limit_count = if (count_val < 0) null else @intCast(count_val);
            opt_idx += 3;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const result = try storage.zrangebyscore(
        allocator,
        key,
        min_score,
        max_score,
        min_exclusive,
        max_exclusive,
        with_scores,
        limit_offset,
        limit_count,
    );

    if (result) |items| {
        defer {
            if (with_scores) {
                // Free score strings (odd indices - allocated by zrangebyscore)
                var idx: usize = 1;
                while (idx < items.len) : (idx += 2) {
                    allocator.free(items[idx]);
                }
            }
            allocator.free(items);
        }

        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, items.len);
        defer resp_values.deinit(allocator);

        for (items) |item| {
            try resp_values.append(allocator, RespValue{ .bulk_string = item });
        }

        return w.writeArray(resp_values.items);
    } else {
        return w.writeArray(&[_]RespValue{});
    }
}

/// ZSCORE key member
/// Get score of member in sorted set
/// Returns bulk string (score) or null if not found
pub fn cmdZscore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'zscore' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const member = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };

    const score = storage.zscore(key, member);

    if (score) |s| {
        var score_buf: [64]u8 = undefined;
        const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{s});
        return w.writeBulkString(score_str);
    } else {
        return w.writeNull();
    }
}

/// ZCARD key
/// Get number of members in sorted set
/// Returns integer - 0 for non-existent key
pub fn cmdZcard(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'zcard' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const count = storage.zcard(key);
    return w.writeInteger(@intCast(count orelse 0));
}

/// ZRANK key member [WITHSCORE]
/// Returns rank (0-based) of member in sorted set (ascending order)
/// Returns null bulk string if member not found
/// If WITHSCORE is given, returns two-element array [rank, score]
pub fn cmdZrank(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zrank' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const member = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };

    var with_score = false;
    if (args.len >= 4) {
        const opt_str = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt_str);
        defer allocator.free(upper_opt);
        if (std.mem.eql(u8, upper_opt, "WITHSCORE")) {
            with_score = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const rank = storage.zrank(key, member, false);
    if (rank == null) {
        return w.writeNull();
    }

    if (with_score) {
        const score = storage.zrankScore(key, member) orelse return w.writeNull();
        var score_buf: [64]u8 = undefined;
        const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{score});
        const owned_score = try allocator.dupe(u8, score_str);
        defer allocator.free(owned_score);
        const items = [_]RespValue{
            .{ .integer = @intCast(rank.?) },
            .{ .bulk_string = owned_score },
        };
        return w.writeArray(&items);
    }

    return w.writeInteger(@intCast(rank.?));
}

/// ZREVRANK key member [WITHSCORE]
/// Returns rank (0-based) of member in sorted set (descending order)
/// Returns null bulk string if member not found
pub fn cmdZrevrank(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zrevrank' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const member = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };

    var with_score = false;
    if (args.len >= 4) {
        const opt_str = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt_str);
        defer allocator.free(upper_opt);
        if (std.mem.eql(u8, upper_opt, "WITHSCORE")) {
            with_score = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const rank = storage.zrank(key, member, true);
    if (rank == null) {
        return w.writeNull();
    }

    if (with_score) {
        const score = storage.zrankScore(key, member) orelse return w.writeNull();
        var score_buf: [64]u8 = undefined;
        const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{score});
        const owned_score = try allocator.dupe(u8, score_str);
        defer allocator.free(owned_score);
        const items = [_]RespValue{
            .{ .integer = @intCast(rank.?) },
            .{ .bulk_string = owned_score },
        };
        return w.writeArray(&items);
    }

    return w.writeInteger(@intCast(rank.?));
}

/// ZINCRBY key increment member
/// Increment score of member in sorted set by increment
/// Returns new score as bulk string
pub fn cmdZincrby(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'zincrby' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const incr_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not a valid float"),
    };

    const increment = parseScore(incr_str) catch {
        return w.writeError("ERR value is not a valid float");
    };

    const member = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };

    const new_score = storage.zincrby(allocator, key, increment, member) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    var score_buf: [64]u8 = undefined;
    const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{new_score});
    return w.writeBulkString(score_str);
}

/// ZCOUNT key min max
/// Count members in sorted set with scores in [min, max]
/// Supports exclusive bounds with ( prefix, and -inf/+inf
/// Returns integer count
pub fn cmdZcount(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'zcount' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const min_arg = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR min is not a valid float"),
    };
    var min_excl = false;
    var min_str = min_arg;
    if (min_str.len > 0 and min_str[0] == '(') {
        min_excl = true;
        min_str = min_str[1..];
    }
    const min_score = parseScore(min_str) catch {
        return w.writeError("ERR min is not a valid float");
    };

    const max_arg = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR max is not a valid float"),
    };
    var max_excl = false;
    var max_str = max_arg;
    if (max_str.len > 0 and max_str[0] == '(') {
        max_excl = true;
        max_str = max_str[1..];
    }
    const max_score = parseScore(max_str) catch {
        return w.writeError("ERR max is not a valid float");
    };

    const count = storage.zcount(key, min_score, max_score, min_excl, max_excl);
    return w.writeInteger(@intCast(count));
}

// Embedded unit tests

test "cmdZadd - basic add" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
    };

    const response = try cmdZadd(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdZadd - multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };

    const response = try cmdZadd(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":3\r\n", response);
}

test "cmdZadd - with NX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add initial member
    const args1 = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
    };
    _ = try cmdZadd(allocator, storage, &args1);

    // NX: skip existing member
    const args2 = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "NX" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "one" },
    };
    const response = try cmdZadd(allocator, storage, &args2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdZadd - with CH option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add initial member
    const args1 = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
    };
    _ = try cmdZadd(allocator, storage, &args1);

    // CH: return changed count (existing updated + new added)
    const args2 = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "CH" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "one" }, // Updated (changed)
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" }, // New (changed)
    };
    const response = try cmdZadd(allocator, storage, &args2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}

test "cmdZadd - invalid score returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "notanumber" },
        .{ .bulk_string = "member" },
    };

    const response = try cmdZadd(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "cmdZadd - WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);

    const args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "member" },
    };

    const response = try cmdZadd(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "cmdZrem - remove members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add members
    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    // Remove one member
    const zrem_args = [_]RespValue{
        .{ .bulk_string = "ZREM" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "one" },
    };
    const response = try cmdZrem(allocator, storage, &zrem_args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdZrange - basic range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add members
    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    // Range all
    const args = [_]RespValue{
        .{ .bulk_string = "ZRANGE" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "-1" },
    };
    const response = try cmdZrange(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", response);
}

test "cmdZrange - with WITHSCORES" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANGE" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "-1" },
        .{ .bulk_string = "WITHSCORES" },
    };
    const response = try cmdZrange(allocator, storage, &args);
    defer allocator.free(response);

    // Should return array with 2 elements: "one" and "1"
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
}

test "cmdZrangebyscore - score range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANGEBYSCORE" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "2" },
    };
    const response = try cmdZrangebyscore(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n", response);
}

test "cmdZrangebyscore - with -inf and +inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANGEBYSCORE" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "+inf" },
    };
    const response = try cmdZrangebyscore(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n", response);
}

test "cmdZscore - get score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1.5" },
        .{ .bulk_string = "member" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZSCORE" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "member" },
    };
    const response = try cmdZscore(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "1.5") != null);
}

test "cmdZscore - non-existent member returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZSCORE" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "nosuchmember" },
    };
    const response = try cmdZscore(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$-1\r\n", response);
}

test "cmdZcard - get count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZCARD" },
        .{ .bulk_string = "myzset" },
    };
    const response = try cmdZcard(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}

test "cmdZcard - non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZCARD" },
        .{ .bulk_string = "nosuchkey" },
    };
    const response = try cmdZcard(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdZrank - get rank ascending" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANK" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "two" },
    };
    const response = try cmdZrank(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdZrank - non-existent member returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZRANK" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "nosuch" },
    };
    const response = try cmdZrank(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$-1\r\n", response);
}

test "cmdZrevrank - get rank descending" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    // "three" has score 3, so in descending order it's rank 0
    const args = [_]RespValue{
        .{ .bulk_string = "ZREVRANK" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "three" },
    };
    const response = try cmdZrevrank(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdZincrby - increment new member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "2.5" },
        .{ .bulk_string = "member" },
    };
    const response = try cmdZincrby(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "2.5") != null);
}

test "cmdZincrby - increment existing member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZINCRBY" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "4" },
        .{ .bulk_string = "one" },
    };
    const response = try cmdZincrby(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "5") != null);
}

test "cmdZcount - count members in range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZCOUNT" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "2" },
    };
    const response = try cmdZcount(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}

test "cmdZcount - with -inf and +inf" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    const args = [_]RespValue{
        .{ .bulk_string = "ZCOUNT" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "-inf" },
        .{ .bulk_string = "+inf" },
    };
    const response = try cmdZcount(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}

test "cmdZcount - exclusive bounds" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const zadd_args = [_]RespValue{
        .{ .bulk_string = "ZADD" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "one" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "two" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "three" },
    };
    _ = try cmdZadd(allocator, storage, &zadd_args);

    // (1, 3] -> scores >1 and <=3, should be two and three
    const args = [_]RespValue{
        .{ .bulk_string = "ZCOUNT" },
        .{ .bulk_string = "myzset" },
        .{ .bulk_string = "(1" },
        .{ .bulk_string = "3" },
    };
    const response = try cmdZcount(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}
