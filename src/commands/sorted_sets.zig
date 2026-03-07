const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const client_mod = @import("./client.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const MapPair = writer_mod.MapPair;
const Storage = storage_mod.Storage;
const RespProtocol = client_mod.RespProtocol;

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
/// RESP3: returns map when WITHSCORES is used
pub fn cmdZrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
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

        // RESP3 with WITHSCORES: return as map (member -> score)
        if (protocol_version == .RESP3 and with_scores and items.len > 0) {
            const map_len = items.len / 2;
            const map_pairs = try allocator.alloc(MapPair, map_len);
            defer allocator.free(map_pairs);

            var i: usize = 0;
            var pair_idx: usize = 0;
            while (i < items.len) : (i += 2) {
                map_pairs[pair_idx] = MapPair{
                    .key = RespValue{ .bulk_string = items[i] },
                    .value = RespValue{ .bulk_string = items[i + 1] },
                };
                pair_idx += 1;
            }

            return w.writeMap(map_pairs);
        } else {
            // RESP2 or no WITHSCORES: flat array
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, items.len);
            defer resp_values.deinit(allocator);

            for (items) |item| {
                try resp_values.append(allocator, RespValue{ .bulk_string = item });
            }

            return w.writeArray(resp_values.items);
        }
    } else {
        // Empty result
        if (protocol_version == .RESP3 and with_scores) {
            return w.writeMap(&[_]MapPair{});
        } else {
            return w.writeArray(&[_]RespValue{});
        }
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
pub fn cmdZrevrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
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

        // RESP3 with WITHSCORES: return as map (member -> score)
        if (protocol_version == .RESP3 and with_scores and ms.len > 0) {
            const map_pairs = try allocator.alloc(MapPair, ms.len);
            defer allocator.free(map_pairs);
            var score_strings = try std.ArrayList([]const u8).initCapacity(allocator, ms.len);
            defer {
                for (score_strings.items) |s| allocator.free(s);
                score_strings.deinit(allocator);
            }

            for (ms, 0..) |sm, i| {
                var score_buf: [64]u8 = undefined;
                const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
                const owned_score = try allocator.dupe(u8, score_str);
                try score_strings.append(allocator, owned_score);

                map_pairs[i] = MapPair{
                    .key = RespValue{ .bulk_string = sm.member },
                    .value = RespValue{ .bulk_string = owned_score },
                };
            }

            return w.writeMap(map_pairs);
        } else {
            // RESP2 or no WITHSCORES: flat array
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
    }

    // Empty result
    if (protocol_version == .RESP3 and with_scores) {
        return w.writeMap(&[_]MapPair{});
    } else {
        return w.writeArray(&[_]RespValue{});
    }
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

/// ZRANGESTORE dest source start stop [WITHSCORES]
/// Store a range of members from a sorted set into a destination sorted set
/// Returns integer - count of members stored
pub fn cmdZrangestore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'zrangestore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination key"),
    };

    const source = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid source key"),
    };

    const start_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start index"),
    };
    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const stop_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid stop index"),
    };
    const stop = std.fmt.parseInt(i64, stop_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Check for WITHSCORES option (though it doesn't affect storage, just parsing)
    var with_scores = false;
    if (args.len >= 6) {
        const opt = switch (args[5]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(upper_opt);
        if (std.mem.eql(u8, upper_opt, "WITHSCORES")) {
            with_scores = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const count = storage.zrangestore(allocator, dest, source, start, stop, with_scores) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };

    return w.writeInteger(@intCast(count));
}

/// ZINTERCARD numkeys key [key ...] [LIMIT limit]
/// Return the cardinality of the intersection of multiple sorted sets
/// Returns integer - intersection count (up to limit if specified)
pub fn cmdZintercard(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zintercard' command");
    }

    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a number");
    };

    if (numkeys == 0) {
        return w.writeError("ERR at least 1 input key is needed for 'zintercard' command");
    }

    if (args.len < 2 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    // Collect keys
    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (0..numkeys) |i| {
        const key = switch (args[2 + i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Parse LIMIT option if present
    var limit: usize = 0; // 0 means no limit
    if (args.len > 2 + numkeys) {
        if (args.len < 2 + numkeys + 2) {
            return w.writeError("ERR syntax error");
        }
        const limit_keyword = switch (args[2 + numkeys]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_keyword = try std.ascii.allocUpperString(allocator, limit_keyword);
        defer allocator.free(upper_keyword);

        if (!std.mem.eql(u8, upper_keyword, "LIMIT")) {
            return w.writeError("ERR syntax error");
        }

        const limit_str = switch (args[2 + numkeys + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR limit must be a number"),
        };
        limit = std.fmt.parseInt(usize, limit_str, 10) catch {
            return w.writeError("ERR limit must be a number");
        };
    }

    const count = storage.zintercard(allocator, keys.items, limit) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };

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

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
/// Pops members with the highest or lowest scores from the first non-empty sorted set.
/// Returns array [key, [member, score, ...]] or null if all sorted sets are empty.
pub fn cmdZmpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zmpop' command");
    }

    // Parse numkeys
    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys is not an integer or out of range"),
    };
    const numkeys = std.fmt.parseInt(i64, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys is not an integer or out of range");
    };
    if (numkeys <= 0) {
        return w.writeError("ERR numkeys should be greater than 0");
    }

    const numkeys_usize = @as(usize, @intCast(numkeys));
    if (args.len < 2 + numkeys_usize + 1) {
        return w.writeError("ERR syntax error");
    }

    // Parse modifier (MIN or MAX)
    const modifier_idx = 2 + numkeys_usize;
    const modifier = switch (args[modifier_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const pop_min = if (std.mem.eql(u8, modifier, "MIN"))
        true
    else if (std.mem.eql(u8, modifier, "MAX"))
        false
    else
        return w.writeError("ERR syntax error");

    // Parse optional COUNT
    var count: usize = 1;
    if (args.len > modifier_idx + 1) {
        if (args.len < modifier_idx + 3) {
            return w.writeError("ERR syntax error");
        }
        const count_keyword = switch (args[modifier_idx + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (!std.mem.eql(u8, count_keyword, "COUNT")) {
            return w.writeError("ERR syntax error");
        }
        const count_str = switch (args[modifier_idx + 2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR count is not an integer or out of range"),
        };
        const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
            return w.writeError("ERR count is not an integer or out of range");
        };
        if (count_i64 <= 0) {
            return w.writeError("ERR count should be greater than 0");
        }
        count = @as(usize, @intCast(count_i64));
    }

    // Try to pop from each key in order
    for (args[2 .. 2 + numkeys_usize]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        // Try popping from this key
        const popped = if (pop_min)
            storage.zpopmin(allocator, key, count)
        else
            storage.zpopmax(allocator, key, count);

        const members = (popped catch |err| {
            if (err == error.WrongType) {
                return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            return err;
        }) orelse continue;

        defer {
            for (members) |sm| allocator.free(sm.member);
            allocator.free(members);
        }

        if (members.len > 0) {
            // Return [key, [member, score, ...]]
            // Build member-score array
            var member_score_buf = std.ArrayList(u8){};
            defer member_score_buf.deinit(allocator);
            const ms_writer = member_score_buf.writer(allocator);

            // Array of member-score pairs
            try ms_writer.print("*{d}\r\n", .{members.len * 2});
            for (members) |sm| {
                try ms_writer.print("${d}\r\n{s}\r\n", .{ sm.member.len, sm.member });

                var score_buf: [64]u8 = undefined;
                const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
                try ms_writer.print("${d}\r\n{s}\r\n", .{ score_str.len, score_str });
            }

            const ms_str = try member_score_buf.toOwnedSlice(allocator);
            defer allocator.free(ms_str);

            // Build final response: *2\r\n$<keylen>\r\n<key>\r\n<member_score_array>
            var final_buf = std.ArrayList(u8){};
            defer final_buf.deinit(allocator);
            const final_writer = final_buf.writer(allocator);
            try final_writer.print("*2\r\n${d}\r\n{s}\r\n{s}", .{ key.len, key, ms_str });

            return try final_buf.toOwnedSlice(allocator);
        }
    }

    // All sorted sets were empty
    return w.writeNull();
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
/// Blocking version of ZMPOP - pops members from the first non-empty sorted set.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns array [key, [member, score, ...]] or null if all sorted sets are empty.
/// BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
/// Blocking version of ZMPOP - blocks until an element is available.
/// Uses polling with 100ms sleep interval to implement blocking semantics.
/// Returns [key, [member, score, ...]] or null if timeout expires.
pub fn cmdBzmpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'bzmpop' command");
    }

    // Parse timeout
    const timeout_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };
    const timeout_f = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Validate timeout >= 0
    if (timeout_f < 0) {
        return w.writeError("ERR timeout is negative");
    }

    // Parse numkeys
    const numkeys_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys is not an integer or out of range"),
    };
    const numkeys = std.fmt.parseInt(i64, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys is not an integer or out of range");
    };
    if (numkeys <= 0) {
        return w.writeError("ERR numkeys should be greater than 0");
    }

    const numkeys_usize = @as(usize, @intCast(numkeys));
    if (args.len < 3 + numkeys_usize + 1) {
        return w.writeError("ERR syntax error");
    }

    // Parse modifier (MIN or MAX)
    const modifier_idx = 3 + numkeys_usize;
    const modifier = switch (args[modifier_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const pop_min = if (std.mem.eql(u8, modifier, "MIN"))
        true
    else if (std.mem.eql(u8, modifier, "MAX"))
        false
    else
        return w.writeError("ERR syntax error");

    // Parse optional COUNT
    var count: usize = 1;
    if (args.len > modifier_idx + 1) {
        if (args.len < modifier_idx + 3) {
            return w.writeError("ERR syntax error");
        }
        const count_keyword = switch (args[modifier_idx + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (!std.mem.eql(u8, count_keyword, "COUNT")) {
            return w.writeError("ERR syntax error");
        }
        const count_str = switch (args[modifier_idx + 2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR count is not an integer or out of range"),
        };
        const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
            return w.writeError("ERR count is not an integer or out of range");
        };
        if (count_i64 <= 0) {
            return w.writeError("ERR count should be greater than 0");
        }
        count = @as(usize, @intCast(count_i64));
    }

    // Convert timeout to milliseconds (0 = block indefinitely)
    const timeout_ms: u64 = if (timeout_f == 0)
        std.math.maxInt(u64)
    else
        @as(u64, @intFromFloat(timeout_f * 1000));

    const poll_interval_ms: u64 = 100; // Poll every 100ms
    var elapsed_ms: u64 = 0;

    while (elapsed_ms < timeout_ms) {
        // Try to pop from each key in order
        for (args[3 .. 3 + numkeys_usize]) |arg| {
            const key = switch (arg) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid key"),
            };

            // Try popping from this key
            const popped = if (pop_min)
                storage.zpopmin(allocator, key, count)
            else
                storage.zpopmax(allocator, key, count);

            const members = (popped catch |err| {
                if (err == error.WrongType) {
                    return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                return err;
            }) orelse continue;

            defer {
                for (members) |sm| allocator.free(sm.member);
                allocator.free(members);
            }

            if (members.len > 0) {
                // Return [key, [member, score, ...]]
                // Build member-score array
                var member_score_buf = std.ArrayList(u8){};
                defer member_score_buf.deinit(allocator);
                const ms_writer = member_score_buf.writer(allocator);

                // Array of member-score pairs
                try ms_writer.print("*{d}\r\n", .{members.len * 2});
                for (members) |sm| {
                    try ms_writer.print("${d}\r\n{s}\r\n", .{ sm.member.len, sm.member });

                    var score_buf: [64]u8 = undefined;
                    const score_str = std.fmt.bufPrint(&score_buf, "{d}", .{sm.score}) catch "0";
                    try ms_writer.print("${d}\r\n{s}\r\n", .{ score_str.len, score_str });
                }

                const ms_str = try member_score_buf.toOwnedSlice(allocator);
                defer allocator.free(ms_str);

                // Build final response: *2\r\n$<keylen>\r\n<key>\r\n<member_score_array>
                var final_buf = std.ArrayList(u8){};
                defer final_buf.deinit(allocator);
                const final_writer = final_buf.writer(allocator);
                try final_writer.print("*2\r\n${d}\r\n{s}\r\n{s}", .{ key.len, key, ms_str });

                return try final_buf.toOwnedSlice(allocator);
            }
        }

        // All sorted sets empty - sleep and retry
        if (elapsed_ms + poll_interval_ms >= timeout_ms) break;
        std.Thread.sleep(poll_interval_ms * std.time.ns_per_ms);
        elapsed_ms += poll_interval_ms;
    }

    // Timeout expired - return null
    return w.writeNull();
}

/// BZPOPMIN key [key ...] timeout
/// Blocking version of ZPOPMIN - blocks until an element is available.
/// Uses polling with 100ms sleep interval to implement blocking semantics.
/// Returns array [key, member, score] or null if timeout expires.
pub fn cmdBzpopmin(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'bzpopmin' command");
    }

    // Last argument is timeout
    const timeout_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };
    const timeout_f = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Validate timeout >= 0
    if (timeout_f < 0) {
        return w.writeError("ERR timeout is negative");
    }

    // Convert timeout to milliseconds (0 = block indefinitely)
    const timeout_ms: u64 = if (timeout_f == 0)
        std.math.maxInt(u64)
    else
        @as(u64, @intFromFloat(timeout_f * 1000));

    const poll_interval_ms: u64 = 100; // Poll every 100ms
    var elapsed_ms: u64 = 0;

    while (elapsed_ms < timeout_ms) {
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

        // All sorted sets empty - sleep and retry
        if (elapsed_ms + poll_interval_ms >= timeout_ms) break;
        std.Thread.sleep(poll_interval_ms * std.time.ns_per_ms);
        elapsed_ms += poll_interval_ms;
    }

    // Timeout expired - return null
    return w.writeNull();
}

/// BZPOPMAX key [key ...] timeout
/// Blocking version of ZPOPMAX - blocks until an element is available.
/// Uses polling with 100ms sleep interval to implement blocking semantics.
/// Returns array [key, member, score] or null if timeout expires.
pub fn cmdBzpopmax(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'bzpopmax' command");
    }

    // Last argument is timeout
    const timeout_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };
    const timeout_f = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Validate timeout >= 0
    if (timeout_f < 0) {
        return w.writeError("ERR timeout is negative");
    }

    // Convert timeout to milliseconds (0 = block indefinitely)
    const timeout_ms: u64 = if (timeout_f == 0)
        std.math.maxInt(u64)
    else
        @as(u64, @intFromFloat(timeout_f * 1000));

    const poll_interval_ms: u64 = 100; // Poll every 100ms
    var elapsed_ms: u64 = 0;

    while (elapsed_ms < timeout_ms) {
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

        // All sorted sets empty - sleep and retry
        if (elapsed_ms + poll_interval_ms >= timeout_ms) break;
        std.Thread.sleep(poll_interval_ms * std.time.ns_per_ms);
        elapsed_ms += poll_interval_ms;
    }

    // Timeout expired - return null
    return w.writeNull();
}

// ── Unit tests for ZMPOP/BZMPOP commands ──────────────────────────────────────

test "sorted_sets - ZMPOP pops min from first non-empty sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "2.0" },
        RespValue{ .bulk_string = "two" },
        RespValue{ .bulk_string = "3.0" },
        RespValue{ .bulk_string = "three" },
    };
    const setup_result = try cmdZadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test ZMPOP MIN
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZMPOP" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "MIN" },
    };

    const result = try cmdZmpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [myzset, [one, 1]]
    try std.testing.expect(std.mem.indexOf(u8, result, "myzset") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "one") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1") != null);
}

test "sorted_sets - ZMPOP pops max with COUNT" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2.0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "3.0" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "4.0" },
        RespValue{ .bulk_string = "d" },
    };
    const setup_result = try cmdZadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test ZMPOP MAX COUNT 2
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZMPOP" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "MAX" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdZmpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [myzset, [d, 4, c, 3]]
    try std.testing.expect(std.mem.indexOf(u8, result, "myzset") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "d") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "c") != null);
}

test "sorted_sets - ZMPOP on all empty sorted sets returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZMPOP" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "empty1" },
        RespValue{ .bulk_string = "empty2" },
        RespValue{ .bulk_string = "MIN" },
    };

    const result = try cmdZmpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "sorted_sets - BZMPOP pops min from first non-empty sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "10.0" },
        RespValue{ .bulk_string = "ten" },
        RespValue{ .bulk_string = "20.0" },
        RespValue{ .bulk_string = "twenty" },
    };
    const setup_result = try cmdZadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test BZMPOP (should behave like ZMPOP with timeout ignored)
    const args = [_]RespValue{
        RespValue{ .bulk_string = "BZMPOP" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "MIN" },
    };

    const result = try cmdBzmpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [zset2, [ten, 10]]
    try std.testing.expect(std.mem.indexOf(u8, result, "zset2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "ten") != null);
}

test "sorted_sets - BZMPOP with MAX and COUNT" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "1.0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2.0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "3.0" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdZadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test BZMPOP MAX COUNT 2
    const args = [_]RespValue{
        RespValue{ .bulk_string = "BZMPOP" },
        RespValue{ .bulk_string = "1.5" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "myzset" },
        RespValue{ .bulk_string = "MAX" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdBzmpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [myzset, [c, 3, b, 2]]
    try std.testing.expect(std.mem.indexOf(u8, result, "myzset") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nc\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nb\r\n") != null);
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

// ── Unit tests for ZRANGESTORE and ZINTERCARD ────────────────────────────────

test "sorted_sets - ZRANGESTORE basic range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: Add sorted set
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "3" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdZadd(allocator, storage, &setup);

    // Test ZRANGESTORE
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZRANGESTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "1" },
    };
    const result = try cmdZrangestore(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 2
    try std.testing.expectEqualStrings(":2\r\n", result);

    // Verify destination has correct members
    const check_args = [_]RespValue{
        RespValue{ .bulk_string = "ZRANGE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "-1" },
    };
    const check_result = try cmdZrange(allocator, storage, &check_args, .RESP2);
    defer allocator.free(check_result);
    try std.testing.expect(std.mem.indexOf(u8, check_result, "$1\r\na") != null);
    try std.testing.expect(std.mem.indexOf(u8, check_result, "$1\r\nb") != null);
}

test "sorted_sets - ZRANGESTORE empty range deletes destination" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: Add sorted set and destination
    const setup1 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
    };
    _ = try cmdZadd(allocator, storage, &setup1);

    const setup2 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "old" },
    };
    _ = try cmdZadd(allocator, storage, &setup2);

    // Test ZRANGESTORE with empty range
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZRANGESTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "10" },
    };
    const result = try cmdZrangestore(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 0
    try std.testing.expectEqualStrings(":0\r\n", result);

    // Verify destination was deleted by checking ZCARD returns 0
    const check_args = [_]RespValue{
        RespValue{ .bulk_string = "ZCARD" },
        RespValue{ .bulk_string = "dest" },
    };
    const check_result = try cmdZcard(allocator, storage, &check_args);
    defer allocator.free(check_result);
    try std.testing.expectEqualStrings(":0\r\n", check_result);
}

test "sorted_sets - ZINTERCARD basic intersection" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: Add two sorted sets
    const setup1 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "3" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdZadd(allocator, storage, &setup1);

    const setup2 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "20" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "30" },
        RespValue{ .bulk_string = "d" },
    };
    _ = try cmdZadd(allocator, storage, &setup2);

    // Test ZINTERCARD
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZINTERCARD" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
    };
    const result = try cmdZintercard(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 2 (b and c are common)
    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "sorted_sets - ZINTERCARD with LIMIT" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: Add two sorted sets with 3 common members
    const setup1 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "3" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdZadd(allocator, storage, &setup1);

    const setup2 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "20" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "30" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdZadd(allocator, storage, &setup2);

    // Test ZINTERCARD with LIMIT 2
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZINTERCARD" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "LIMIT" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdZintercard(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 2 (limited to 2 even though 3 are common)
    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "sorted_sets - ZINTERCARD no intersection" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: Add two sorted sets with no common members
    const setup1 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
    };
    _ = try cmdZadd(allocator, storage, &setup1);

    const setup2 = [_]RespValue{
        RespValue{ .bulk_string = "ZADD" },
        RespValue{ .bulk_string = "zset2" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "b" },
    };
    _ = try cmdZadd(allocator, storage, &setup2);

    // Test ZINTERCARD
    const args = [_]RespValue{
        RespValue{ .bulk_string = "ZINTERCARD" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "zset1" },
        RespValue{ .bulk_string = "zset2" },
    };
    const result = try cmdZintercard(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 0
    try std.testing.expectEqualStrings(":0\r\n", result);
}

/// ZUNION numkeys key [key ...] [WITHSCORES]
/// Return the union of multiple sorted sets
pub fn cmdZunion(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zunion' command");
    }

    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (args.len < 2 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[2 .. 2 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Check for WITHSCORES option
    var with_scores = false;
    if (args.len > 2 + numkeys) {
        const opt = switch (args[2 + numkeys]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(upper_opt);
        if (std.mem.eql(u8, upper_opt, "WITHSCORES")) {
            with_scores = true;
        }
    }

    const members = storage.zunion(allocator, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer {
        for (members) |m| allocator.free(m.member);
        allocator.free(members);
    }

    if (with_scores) {
        if (protocol_version == .RESP3) {
            // Return as RESP3 map: member -> score
            var pairs = try std.ArrayList(MapPair).initCapacity(allocator, members.len);
            defer pairs.deinit(allocator);
            for (members) |sm| {
                var score_buf: [32]u8 = undefined;
                const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{sm.score});
                try pairs.append(allocator, MapPair{
                    .key = RespValue{ .bulk_string = sm.member },
                    .value = RespValue{ .bulk_string = score_str },
                });
            }
            return w.writeMap(pairs.items);
        } else {
            // Return flat array: [member1, score1, member2, score2, ...]
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len * 2);
            defer resp_values.deinit(allocator);
            for (members) |sm| {
                var score_buf: [32]u8 = undefined;
                const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{sm.score});
                try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
                try resp_values.append(allocator, RespValue{ .bulk_string = score_str });
            }
            return w.writeArray(resp_values.items);
        }
    } else {
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
        defer resp_values.deinit(allocator);
        for (members) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
        }
        return w.writeArray(resp_values.items);
    }
}

/// ZINTER numkeys key [key ...] [WITHSCORES]
/// Return the intersection of multiple sorted sets
pub fn cmdZinter(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zinter' command");
    }

    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (args.len < 2 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[2 .. 2 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Check for WITHSCORES option
    var with_scores = false;
    if (args.len > 2 + numkeys) {
        const opt = switch (args[2 + numkeys]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(upper_opt);
        if (std.mem.eql(u8, upper_opt, "WITHSCORES")) {
            with_scores = true;
        }
    }

    const members = storage.zinter(allocator, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer {
        for (members) |m| allocator.free(m.member);
        allocator.free(members);
    }

    if (with_scores) {
        if (protocol_version == .RESP3) {
            // Return as RESP3 map: member -> score
            var pairs = try std.ArrayList(MapPair).initCapacity(allocator, members.len);
            defer pairs.deinit(allocator);
            for (members) |sm| {
                var score_buf: [32]u8 = undefined;
                const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{sm.score});
                try pairs.append(allocator, MapPair{
                    .key = RespValue{ .bulk_string = sm.member },
                    .value = RespValue{ .bulk_string = score_str },
                });
            }
            return w.writeMap(pairs.items);
        } else {
            // Return flat array: [member1, score1, member2, score2, ...]
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len * 2);
            defer resp_values.deinit(allocator);
            for (members) |sm| {
                var score_buf: [32]u8 = undefined;
                const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{sm.score});
                try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
                try resp_values.append(allocator, RespValue{ .bulk_string = score_str });
            }
            return w.writeArray(resp_values.items);
        }
    } else {
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
        defer resp_values.deinit(allocator);
        for (members) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
        }
        return w.writeArray(resp_values.items);
    }
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
/// Return the difference of multiple sorted sets
pub fn cmdZdiff(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zdiff' command");
    }

    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (args.len < 2 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[2 .. 2 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Check for WITHSCORES option
    var with_scores = false;
    if (args.len > 2 + numkeys) {
        const opt = switch (args[2 + numkeys]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const upper_opt = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(upper_opt);
        if (std.mem.eql(u8, upper_opt, "WITHSCORES")) {
            with_scores = true;
        }
    }

    const members = storage.zdiff(allocator, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer {
        for (members) |m| allocator.free(m.member);
        allocator.free(members);
    }

    if (with_scores) {
        if (protocol_version == .RESP3) {
            // Return as RESP3 map: member -> score
            var pairs = try std.ArrayList(MapPair).initCapacity(allocator, members.len);
            defer pairs.deinit(allocator);
            for (members) |sm| {
                var score_buf: [32]u8 = undefined;
                const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{sm.score});
                try pairs.append(allocator, MapPair{
                    .key = RespValue{ .bulk_string = sm.member },
                    .value = RespValue{ .bulk_string = score_str },
                });
            }
            return w.writeMap(pairs.items);
        } else {
            // Return flat array: [member1, score1, member2, score2, ...]
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len * 2);
            defer resp_values.deinit(allocator);
            for (members) |sm| {
                var score_buf: [32]u8 = undefined;
                const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{sm.score});
                try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
                try resp_values.append(allocator, RespValue{ .bulk_string = score_str });
            }
            return w.writeArray(resp_values.items);
        }
    } else {
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
        defer resp_values.deinit(allocator);
        for (members) |sm| {
            try resp_values.append(allocator, RespValue{ .bulk_string = sm.member });
        }
        return w.writeArray(resp_values.items);
    }
}

/// ZUNIONSTORE destination numkeys key [key ...]
/// Store union of sorted sets in destination
pub fn cmdZunionstore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zunionstore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };

    const numkeys_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (args.len < 3 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[3 .. 3 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.zunionstore(allocator, dest, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
}

/// ZINTERSTORE destination numkeys key [key ...]
/// Store intersection of sorted sets in destination
pub fn cmdZinterstore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zinterstore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };

    const numkeys_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (args.len < 3 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[3 .. 3 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.zinterstore(allocator, dest, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
}

/// ZDIFFSTORE destination numkeys key [key ...]
/// Store difference of sorted sets in destination
pub fn cmdZdiffstore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zdiffstore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };

    const numkeys_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (args.len < 3 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[3 .. 3 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.zdiffstore(allocator, dest, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
}

/// ZREMRANGEBYRANK key start stop
/// Remove all members in a sorted set within the given ranks
pub fn cmdZremrangebyrank(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'zremrangebyrank' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start index"),
    };
    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const stop_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid stop index"),
    };
    const stop = std.fmt.parseInt(i64, stop_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const removed = storage.zremrangebyrank(key, start, stop) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(removed));
}

/// ZREMRANGEBYSCORE key min max
/// Remove all members in a sorted set within the given scores
pub fn cmdZremrangebyscore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'zremrangebyscore' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const min_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min score"),
    };

    const max_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid max score"),
    };

    // Parse min/max with exclusive support
    const min_exclusive = min_str.len > 0 and min_str[0] == '(';
    const max_exclusive = max_str.len > 0 and max_str[0] == '(';

    const min_val = if (min_exclusive) min_str[1..] else min_str;
    const max_val = if (max_exclusive) max_str[1..] else max_str;

    const min = parseScore(min_val) catch {
        return w.writeError("ERR min or max is not a float");
    };
    const max = parseScore(max_val) catch {
        return w.writeError("ERR min or max is not a float");
    };

    const removed = storage.zremrangebyscore(key, min, max, min_exclusive, max_exclusive) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(removed));
}

/// ZREMRANGEBYLEX key min max
/// Remove all members in a sorted set between the given lexicographical range
pub fn cmdZremrangebylex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'zremrangebylex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const min = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min"),
    };

    const max = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid max"),
    };

    const removed = storage.zremrangebylex(key, min, max) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if (err == error.InvalidLexRange) {
            return w.writeError("ERR min or max not valid string range item");
        }
        return err;
    };

    return w.writeInteger(@intCast(removed));
}

/// ZRANGEBYLEX key min max [LIMIT offset count]
/// Return members in a sorted set within the given lexicographical range
pub fn cmdZrangebylex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zrangebylex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const min = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min"),
    };

    const max = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid max"),
    };

    var offset: ?usize = null;
    var count: ?usize = null;

    // Parse LIMIT option
    var i: usize = 4;
    while (i < args.len) {
        const arg_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const upper_arg = try std.ascii.allocUpperString(allocator, arg_str);
        defer allocator.free(upper_arg);

        if (std.mem.eql(u8, upper_arg, "LIMIT")) {
            if (i + 2 >= args.len) {
                return w.writeError("ERR syntax error");
            }

            const offset_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };

            const count_str = switch (args[i + 2]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            count = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };

            i += 3;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const members = storage.zrangebylex(allocator, key, min, max, offset, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if (err == error.InvalidLexRange) {
            return w.writeError("ERR min or max not valid string range item");
        }
        return err;
    };
    defer {
        for (members) |m| allocator.free(m);
        allocator.free(members);
    }

    // Convert to RespValue array
    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
    defer resp_values.deinit(allocator);

    for (members) |member| {
        try resp_values.append(allocator, RespValue{ .bulk_string = member });
    }

    return w.writeArray(resp_values.items);
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count]
/// Return members in a sorted set within the given lexicographical range in reverse order
pub fn cmdZrevrangebylex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'zrevrangebylex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const max = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid max"),
    };

    const min = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min"),
    };

    var offset: ?usize = null;
    var count: ?usize = null;

    // Parse LIMIT option
    var i: usize = 4;
    while (i < args.len) {
        const arg_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const upper_arg = try std.ascii.allocUpperString(allocator, arg_str);
        defer allocator.free(upper_arg);

        if (std.mem.eql(u8, upper_arg, "LIMIT")) {
            if (i + 2 >= args.len) {
                return w.writeError("ERR syntax error");
            }

            const offset_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };

            const count_str = switch (args[i + 2]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            count = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };

            i += 3;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const members = storage.zrevrangebylex(allocator, key, max, min, offset, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if (err == error.InvalidLexRange) {
            return w.writeError("ERR min or max not valid string range item");
        }
        return err;
    };
    defer {
        for (members) |m| allocator.free(m);
        allocator.free(members);
    }

    // Convert to RespValue array
    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
    defer resp_values.deinit(allocator);

    for (members) |member| {
        try resp_values.append(allocator, RespValue{ .bulk_string = member });
    }

    return w.writeArray(resp_values.items);
}

/// ZLEXCOUNT key min max
/// Count members in a sorted set within the given lexicographical range
pub fn cmdZlexcount(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'zlexcount' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const min = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min"),
    };

    const max = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid max"),
    };

    const count = storage.zlexcount(key, min, max) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if (err == error.InvalidLexRange) {
            return w.writeError("ERR min or max not valid string range item");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
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
