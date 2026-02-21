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

/// ZPOPMIN key [count]
/// Remove and return lowest-score members.
/// Returns interleaved array of [member, score, ...].
pub fn cmdZpopmin(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'zpopmin' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const count: usize = if (args.len == 3) blk: {
        const cs = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        const n = std.fmt.parseInt(i64, cs, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
        if (n < 0) return w.writeError("ERR value is not an integer or out of range");
        break :blk @as(usize, @intCast(n));
    } else 1;

    const popped = storage.zpopmin(allocator, key, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (popped) |members| {
        defer {
            for (members) |sm| allocator.free(sm.member);
            allocator.free(members);
        }
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len * 2);
        defer resp_values.deinit(allocator);
        // Collect allocated score strings so we can free them after writeArray
        var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, members.len);
        defer {
            for (score_strings.items) |s| allocator.free(s);
            score_strings.deinit(allocator);
        }
        for (members) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
            var score_buf: [64]u8 = undefined;
            const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
            const owned_score = try allocator.dupe(u8, score_str);
            try score_strings.append(allocator, owned_score);
            try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });
        }
        return w.writeArray(resp_values.items);
    }
    return w.writeArray(&[_]RespValue{});
}

/// ZPOPMAX key [count]
/// Remove and return highest-score members.
/// Returns interleaved array of [member, score, ...].
pub fn cmdZpopmax(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'zpopmax' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const count: usize = if (args.len == 3) blk: {
        const cs = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        const n = std.fmt.parseInt(i64, cs, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
        if (n < 0) return w.writeError("ERR value is not an integer or out of range");
        break :blk @as(usize, @intCast(n));
    } else 1;

    const popped = storage.zpopmax(allocator, key, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (popped) |members| {
        defer {
            for (members) |sm| allocator.free(sm.member);
            allocator.free(members);
        }
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len * 2);
        defer resp_values.deinit(allocator);
        var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, members.len);
        defer {
            for (score_strings.items) |s| allocator.free(s);
            score_strings.deinit(allocator);
        }
        for (members) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
            var score_buf: [64]u8 = undefined;
            const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
            const owned_score = try allocator.dupe(u8, score_str);
            try score_strings.append(allocator, owned_score);
            try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });
        }
        return w.writeArray(resp_values.items);
    }
    return w.writeArray(&[_]RespValue{});
}

/// ZMSCORE key member [member ...]
/// Bulk ZSCORE. Returns array of scores (null bulk string for missing members).
pub fn cmdZmscore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zmscore' command");
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

    const scores = storage.zmscore(allocator, key, members.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(scores);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, scores.len);
    defer resp_values.deinit(allocator);

    var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, scores.len);
    defer {
        for (score_strings.items) |s| allocator.free(s);
        score_strings.deinit(allocator);
    }
    for (scores) |maybe_score| {
        if (maybe_score) |score| {
            var score_buf: [64]u8 = undefined;
            const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{score}) catch "0";
            const owned_score = try allocator.dupe(u8, score_str);
            try score_strings.append(allocator, owned_score);
            try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });
        } else {
            try resp_values.append(allocator, RespValue{ .null_bulk_string = {} });
        }
    }

    return w.writeArray(resp_values.items);
}

/// ZREVRANGE key start stop [WITHSCORES]
/// Reverse-order range by index (deprecated but needed for compatibility).
pub fn cmdZrevrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zrevrange' command");
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

    var with_scores = false;
    if (args.len >= 5) {
        const opt = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "WITHSCORES")) {
            with_scores = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const members = storage.zrevrange(allocator, key, start, stop) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (members) |ms| {
        defer {
            for (ms) |sm| allocator.free(sm.member);
            allocator.free(ms);
        }
        const result_len = if (with_scores) ms.len * 2 else ms.len;
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, result_len);
        defer resp_values.deinit(allocator);
        var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, ms.len);
        defer {
            for (score_strings.items) |s| allocator.free(s);
            score_strings.deinit(allocator);
        }
        for (ms) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
            if (with_scores) {
                var score_buf: [64]u8 = undefined;
                const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
                const owned_score = try allocator.dupe(u8, score_str);
                try score_strings.append(allocator, owned_score);
                try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });
            }
        }
        return w.writeArray(resp_values.items);
    }
    return w.writeArray(&[_]RespValue{});
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
/// Score range descending (max first).
pub fn cmdZrevrangebyscore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zrevrangebyscore' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const max_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR max is not a valid float"),
    };
    const min_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR min is not a valid float"),
    };

    const max_score = parseScore(max_str) catch {
        return w.writeError("ERR max is not a valid float");
    };
    const min_score = parseScore(min_str) catch {
        return w.writeError("ERR min is not a valid float");
    };

    var with_scores = false;
    var offset: usize = 0;
    var limit: i64 = -1;

    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "WITHSCORES")) {
            with_scores = true;
        } else if (std.mem.eql(u8, opt_upper, "LIMIT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const off_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            offset = std.fmt.parseInt(usize, off_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const cnt_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            limit = std.fmt.parseInt(i64, cnt_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const members = storage.zrevrangebyscore(allocator, key, max_score, min_score, offset, limit) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (members) |ms| {
        defer {
            for (ms) |sm| allocator.free(sm.member);
            allocator.free(ms);
        }
        const result_len = if (with_scores) ms.len * 2 else ms.len;
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, result_len);
        defer resp_values.deinit(allocator);
        var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, ms.len);
        defer {
            for (score_strings.items) |s| allocator.free(s);
            score_strings.deinit(allocator);
        }
        for (ms) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
            if (with_scores) {
                var score_buf: [64]u8 = undefined;
                const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
                const owned_score = try allocator.dupe(u8, score_str);
                try score_strings.append(allocator, owned_score);
                try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });
            }
        }
        return w.writeArray(resp_values.items);
    }
    return w.writeArray(&[_]RespValue{});
}

/// ZRANDMEMBER key [count [WITHSCORES]]
/// Return random member(s) from sorted set.
pub fn cmdZrandmember(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 4) {
        return w.writeError("ERR wrong number of arguments for 'zrandmember' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const has_count = args.len >= 3;
    const count: i64 = if (has_count) blk: {
        const cs = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        break :blk std.fmt.parseInt(i64, cs, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
    } else 1;

    var with_scores = false;
    if (args.len == 4) {
        const opt = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "WITHSCORES")) {
            with_scores = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const members = storage.zrandmember(allocator, key, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (!has_count) {
        if (members) |ms| {
            defer {
                for (ms) |sm| allocator.free(sm.member);
                allocator.free(ms);
            }
            if (ms.len > 0) return w.writeBulkString(ms[0].member);
        }
        return w.writeBulkString(null);
    } else {
        if (members) |ms| {
            defer {
                for (ms) |sm| allocator.free(sm.member);
                allocator.free(ms);
            }
            const result_len = if (with_scores) ms.len * 2 else ms.len;
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, result_len);
            defer resp_values.deinit(allocator);
            var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, ms.len);
            defer {
                for (score_strings.items) |s| allocator.free(s);
                score_strings.deinit(allocator);
            }
            for (ms) |sm| {
                try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
                if (with_scores) {
                    var score_buf: [64]u8 = undefined;
                    const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
                    const owned_score = try allocator.dupe(u8, score_str);
                    try score_strings.append(allocator, owned_score);
                    try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });
                }
            }
            return w.writeArray(resp_values.items);
        }
        return w.writeArray(&[_]RespValue{});
    }
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

/// BZPOPMIN key [key ...] timeout
/// Blocking version of ZPOPMIN - pops lowest-score member from first non-empty sorted set.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns array [key, member, score] or null if all sorted sets are empty.
pub fn cmdBzpopmin(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'bzpopmin' command");
    }

    // Last argument is timeout (we parse but ignore in this implementation)
    const timeout_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };
    _ = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Try to pop from each key in order
    for (args[1 .. args.len - 1]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        // Try ZPOPMIN from this key
        const popped = storage.zpopmin(allocator, key, 1) catch |err| {
            if (err == error.WrongType) {
                return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            return err;
        };

        if (popped) |members| {
            defer {
                for (members) |sm| allocator.free(sm.member);
                allocator.free(members);
            }

            if (members.len > 0) {
                // Return [key, member, score]
                var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, 3);
                defer resp_values.deinit(allocator);

                try resp_values.append(allocator, RespValue{ .bulk_string = key });
                try resp_values.append(allocator, RespValue{ .bulk_string = members[0].member });

                var score_buf: [64]u8 = undefined;
                const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{members[0].score}) catch "0";
                const owned_score = try allocator.dupe(u8, score_str);
                defer allocator.free(owned_score);

                try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });

                return w.writeArray(resp_values.items);
            }
        }
    }

    // All sorted sets empty - return null
    return w.writeNull();
}

/// BZPOPMAX key [key ...] timeout
/// Blocking version of ZPOPMAX - pops highest-score member from first non-empty sorted set.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns array [key, member, score] or null if all sorted sets are empty.
pub fn cmdBzpopmax(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'bzpopmax' command");
    }

    // Last argument is timeout (we parse but ignore in this implementation)
    const timeout_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };
    _ = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Try to pop from each key in order
    for (args[1 .. args.len - 1]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        // Try ZPOPMAX from this key
        const popped = storage.zpopmax(allocator, key, 1) catch |err| {
            if (err == error.WrongType) {
                return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            return err;
        };

        if (popped) |members| {
            defer {
                for (members) |sm| allocator.free(sm.member);
                allocator.free(members);
            }

            if (members.len > 0) {
                // Return [key, member, score]
                var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, 3);
                defer resp_values.deinit(allocator);

                try resp_values.append(allocator, RespValue{ .bulk_string = key });
                try resp_values.append(allocator, RespValue{ .bulk_string = members[0].member });

                var score_buf: [64]u8 = undefined;
                const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{members[0].score}) catch "0";
                const owned_score = try allocator.dupe(u8, score_str);
                defer allocator.free(owned_score);

                try resp_values.append(allocator, RespValue{ .bulk_string = owned_score });

                return w.writeArray(resp_values.items);
            }
        }
    }

    // All sorted sets empty - return null
    return w.writeNull();
}

// ── Unit tests for blocking sorted set commands ──────────────────────────────

test "sorted_sets - BZPOPMIN returns from first non-empty sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: zset2 has data
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "b" },
    };
    const sr = try cmdZadd(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMIN" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBzpopmin(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [zset2, a, 1]
    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "zset2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\na") != null);
}

test "sorted_sets - BZPOPMAX returns from first non-empty sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: zset2 has data
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "b" },
    };
    const sr = try cmdZadd(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMAX" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBzpopmax(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [zset2, b, 2]
    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "zset2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nb") != null);
}

test "sorted_sets - BZPOPMIN on all empty sorted sets returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BZPOPMIN" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBzpopmin(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}
