const std = @import("std");

/// Redis command metadata for introspection
pub const CommandInfo = struct {
    name: []const u8,
    arity: i32, // negative = minimum args, positive = exact count
    flags: []const []const u8,
    first_key: i32,
    last_key: i32,
    step: i32,
};

/// All supported commands with their metadata
pub const ALL_COMMANDS = [_]CommandInfo{
    // String commands
    .{ .name = "ping", .arity = -1, .flags = &.{ "fast", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "set", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "get", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "del", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "exists", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "getrange", .arity = 4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "setrange", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },

    // List commands
    .{ .name = "lpush", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "rpush", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lpop", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "rpop", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lrange", .arity = 4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "llen", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lindex", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lset", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ltrim", .arity = 4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lrem", .arity = 4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lpushx", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "rpushx", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "linsert", .arity = 5, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lpos", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lmove", .arity = 5, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "rpoplpush", .arity = 3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 2, .step = 1 },

    // Set commands
    .{ .name = "sadd", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "srem", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "sismember", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "smismember", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "smembers", .arity = 2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "scard", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "spop", .arity = -2, .flags = &.{ "write", "fast", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "srandmember", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "smove", .arity = 4, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "sintercard", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sunion", .arity = -2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sinter", .arity = -2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sdiff", .arity = -2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sunionstore", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sinterstore", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sdiffstore", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sscan", .arity = -3, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Hash commands
    .{ .name = "hset", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hget", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hdel", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hgetall", .arity = 2, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hkeys", .arity = 2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hvals", .arity = 2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hexists", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hlen", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hmget", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hincrby", .arity = 4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hincrbyfloat", .arity = 4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hsetnx", .arity = 4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hscan", .arity = -3, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Sorted set commands
    .{ .name = "zadd", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrem", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrange", .arity = -4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrevrange", .arity = -4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrangebyscore", .arity = -4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrevrangebyscore", .arity = -4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zscore", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zmscore", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zcard", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrank", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrevrank", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zcount", .arity = 4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zincrby", .arity = 4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zpopmin", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zpopmax", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrandmember", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zscan", .arity = -3, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Persistence commands
    .{ .name = "save", .arity = 1, .flags = &.{ "admin", "noscript" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "bgsave", .arity = 1, .flags = &.{ "admin", "noscript" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "bgrewriteaof", .arity = 1, .flags = &.{ "admin", "noscript" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "dbsize", .arity = 1, .flags = &.{ "readonly", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "flushdb", .arity = 1, .flags = &.{ "write", "admin" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "flushall", .arity = 1, .flags = &.{ "write", "admin" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Pub/Sub commands
    .{ .name = "subscribe", .arity = -2, .flags = &.{ "pubsub", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "unsubscribe", .arity = -1, .flags = &.{ "pubsub", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "publish", .arity = 3, .flags = &.{ "pubsub", "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "pubsub", .arity = -2, .flags = &.{ "pubsub", "random", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Transaction commands
    .{ .name = "multi", .arity = 1, .flags = &.{ "noscript", "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "exec", .arity = 1, .flags = &.{ "noscript", "loading", "stale", "skip_monitor", "no_multi" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "discard", .arity = 1, .flags = &.{ "noscript", "loading", "stale", "fast", "no_multi" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "watch", .arity = -2, .flags = &.{ "noscript", "loading", "stale", "fast", "no_multi" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "unwatch", .arity = 1, .flags = &.{ "noscript", "loading", "stale", "fast", "no_multi" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Replication commands
    .{ .name = "replicaof", .arity = 3, .flags = &.{ "admin", "noscript", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "replconf", .arity = -3, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "psync", .arity = 3, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "wait", .arity = 3, .flags = &.{ "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "info", .arity = -1, .flags = &.{ "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Scan commands
    .{ .name = "scan", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Object commands
    .{ .name = "object", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 2, .last_key = 2, .step = 1 },

    // Client commands
    .{ .name = "client", .arity = -2, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Config commands
    .{ .name = "config", .arity = -2, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Command introspection
    .{ .name = "command", .arity = -1, .flags = &.{ "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
};

/// COMMAND - Return all commands
pub fn cmdCommand(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    // Write array header
    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{ALL_COMMANDS.len});
    try buf.appendSlice(allocator, "\r\n");

    for (ALL_COMMANDS) |cmd| {
        try writeCommandInfo(allocator, &buf, cmd);
    }

    return buf.toOwnedSlice(allocator);
}

/// COMMAND COUNT - Return number of commands
pub fn cmdCommandCount(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try buf.append(allocator, ':');
    try std.fmt.format(buf.writer(allocator), "{d}", .{ALL_COMMANDS.len});
    try buf.appendSlice(allocator, "\r\n");

    return buf.toOwnedSlice(allocator);
}

/// COMMAND INFO - Return specific command info
pub fn cmdCommandInfo(allocator: std.mem.Allocator, args: []const []const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{args.len});
    try buf.appendSlice(allocator, "\r\n");

    for (args) |name| {
        var found = false;
        for (ALL_COMMANDS) |cmd| {
            if (std.ascii.eqlIgnoreCase(cmd.name, name)) {
                try writeCommandInfo(allocator, &buf, cmd);
                found = true;
                break;
            }
        }
        if (!found) {
            try buf.appendSlice(allocator, "$-1\r\n");
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// COMMAND GETKEYS - Extract key positions from command
pub fn cmdCommandGetKeys(allocator: std.mem.Allocator, args: []const []const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (args.len == 0) {
        try buf.append(allocator, '-');
        try buf.appendSlice(allocator, "ERR wrong number of arguments for 'command|getkeys' command");
        try buf.appendSlice(allocator, "\r\n");
        return buf.toOwnedSlice(allocator);
    }

    const cmd_name = args[0];
    for (ALL_COMMANDS) |cmd| {
        if (std.ascii.eqlIgnoreCase(cmd.name, cmd_name)) {
            // Calculate number of keys
            if (cmd.first_key == 0) {
                // No keys
                try buf.appendSlice(allocator, "*0\r\n");
                return buf.toOwnedSlice(allocator);
            }

            var key_count: usize = 0;
            var i: i32 = cmd.first_key;
            while (i <= cmd.last_key or cmd.last_key == -1) : (i += cmd.step) {
                const idx: usize = @intCast(i);
                if (idx >= args.len) break;
                key_count += 1;
                if (cmd.last_key != -1 and i >= cmd.last_key) break;
            }

            try buf.append(allocator, '*');
            try std.fmt.format(buf.writer(allocator), "{d}", .{key_count});
            try buf.appendSlice(allocator, "\r\n");

            i = cmd.first_key;
            while (i <= cmd.last_key or cmd.last_key == -1) : (i += cmd.step) {
                const idx: usize = @intCast(i);
                if (idx >= args.len) break;
                try writeBulkString(allocator, &buf, args[idx]);
                if (cmd.last_key != -1 and i >= cmd.last_key) break;
            }
            return buf.toOwnedSlice(allocator);
        }
    }

    try buf.append(allocator, '-');
    try buf.appendSlice(allocator, "ERR Invalid command specified");
    try buf.appendSlice(allocator, "\r\n");
    return buf.toOwnedSlice(allocator);
}

/// COMMAND LIST - List command names (Redis 7.0+)
pub fn cmdCommandList(allocator: std.mem.Allocator, filter_by: ?[]const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    // Count matching commands
    var count: usize = 0;
    for (ALL_COMMANDS) |cmd| {
        if (filter_by) |filter| {
            if (std.mem.indexOf(u8, filter, "module") != null) continue;
            if (std.mem.indexOf(u8, filter, "aclcat:") != null) continue;
            if (std.mem.indexOf(u8, filter, "pattern:") != null) {
                const pattern = filter[8..];
                if (!matchesPattern(cmd.name, pattern)) continue;
            }
        }
        count += 1;
    }

    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{count});
    try buf.appendSlice(allocator, "\r\n");

    for (ALL_COMMANDS) |cmd| {
        if (filter_by) |filter| {
            if (std.mem.indexOf(u8, filter, "module") != null) continue;
            if (std.mem.indexOf(u8, filter, "aclcat:") != null) continue;
            if (std.mem.indexOf(u8, filter, "pattern:") != null) {
                const pattern = filter[8..];
                if (!matchesPattern(cmd.name, pattern)) continue;
            }
        }
        try writeBulkString(allocator, &buf, cmd.name);
    }

    return buf.toOwnedSlice(allocator);
}

/// COMMAND HELP - Show help for COMMAND
pub fn cmdCommandHelp(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const help_lines = [_][]const u8{
        "COMMAND - Return all commands",
        "COMMAND COUNT - Return number of commands",
        "COMMAND INFO <command> [<command> ...] - Return command details",
        "COMMAND GETKEYS <command> [<arg> ...] - Extract keys from command",
        "COMMAND LIST [FILTERBY <filter>] - List command names (Redis 7.0+)",
        "COMMAND HELP - This help message",
    };

    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{help_lines.len});
    try buf.appendSlice(allocator, "\r\n");

    for (help_lines) |line| {
        try writeBulkString(allocator, &buf, line);
    }

    return buf.toOwnedSlice(allocator);
}

/// Write command info in Redis format
fn writeCommandInfo(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), cmd: CommandInfo) !void {
    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{6});
    try buf.appendSlice(allocator, "\r\n");

    // 1. Command name
    try writeBulkString(allocator, buf, cmd.name);

    // 2. Arity
    try buf.append(allocator, ':');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.arity});
    try buf.appendSlice(allocator, "\r\n");

    // 3. Flags
    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.flags.len});
    try buf.appendSlice(allocator, "\r\n");
    for (cmd.flags) |flag| {
        try writeBulkString(allocator, buf, flag);
    }

    // 4. First key position
    try buf.append(allocator, ':');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.first_key});
    try buf.appendSlice(allocator, "\r\n");

    // 5. Last key position
    try buf.append(allocator, ':');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.last_key});
    try buf.appendSlice(allocator, "\r\n");

    // 6. Step
    try buf.append(allocator, ':');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.step});
    try buf.appendSlice(allocator, "\r\n");
}

/// Write a bulk string to buffer
fn writeBulkString(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), str: []const u8) !void {
    try buf.append(allocator, '$');
    try std.fmt.format(buf.writer(allocator), "{d}", .{str.len});
    try buf.appendSlice(allocator, "\r\n");
    try buf.appendSlice(allocator, str);
    try buf.appendSlice(allocator, "\r\n");
}

/// Simple glob pattern matching (supports * only)
fn matchesPattern(name: []const u8, pattern: []const u8) bool {
    if (std.mem.indexOf(u8, pattern, "*") == null) {
        return std.ascii.eqlIgnoreCase(name, pattern);
    }

    var pat_idx: usize = 0;
    var name_idx: usize = 0;

    while (pat_idx < pattern.len and name_idx < name.len) {
        if (pattern[pat_idx] == '*') {
            if (pat_idx == pattern.len - 1) return true;
            pat_idx += 1;
            while (name_idx < name.len) {
                if (matchesPattern(name[name_idx..], pattern[pat_idx..])) return true;
                name_idx += 1;
            }
            return false;
        }
        if (std.ascii.toLower(pattern[pat_idx]) != std.ascii.toLower(name[name_idx])) {
            return false;
        }
        pat_idx += 1;
        name_idx += 1;
    }

    return pat_idx == pattern.len and name_idx == name.len;
}

// Unit tests
test "command metadata" {
    const expect = std.testing.expect;

    // Verify all commands have valid metadata
    for (ALL_COMMANDS) |cmd| {
        try expect(cmd.name.len > 0);
        try expect(cmd.arity != 0);
    }
}

test "pattern matching" {
    const expect = std.testing.expect;

    try expect(matchesPattern("set", "set"));
    try expect(matchesPattern("SET", "set"));
    try expect(matchesPattern("set", "s*"));
    try expect(matchesPattern("set", "*et"));
    try expect(matchesPattern("sadd", "s*"));
    try expect(!matchesPattern("get", "s*"));
    try expect(matchesPattern("hset", "*set"));
}
