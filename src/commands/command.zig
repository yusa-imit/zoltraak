const std = @import("std");

/// Redis command metadata for introspection
pub const CommandInfo = struct {
    name: []const u8,
    arity: i32, // negative = minimum args, positive = exact count
    flags: []const []const u8,
    first_key: i32,
    last_key: i32,
    step: i32,
    acl_categories: []const []const u8 = &.{},
};

/// All supported commands with their metadata
pub const ALL_COMMANDS = [_]CommandInfo{
    // String commands
    .{ .name = "ping", .arity = -1, .flags = &.{ "fast", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "set", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "get", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "mget", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "mset", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -1, .step = 2 },
    .{ .name = "msetnx", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -1, .step = 2 },
    .{ .name = "append", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "incr", .arity = 2, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "decr", .arity = 2, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "incrby", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "decrby", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "incrbyfloat", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "setnx", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "setex", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "psetex", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "getset", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "getdel", .arity = 2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "getex", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "strlen", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "del", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "unlink", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "exists", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "getrange", .arity = 4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "setrange", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Bit operations
    .{ .name = "setbit", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "getbit", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bitcount", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bitop", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 3, .last_key = -1, .step = 1 },
    .{ .name = "bitpos", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bitfield", .arity = -2, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bitfield_ro", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },

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
    .{ .name = "blpop", .arity = -3, .flags = &.{ "write", "noscript" }, .first_key = 1, .last_key = -2, .step = 1 },
    .{ .name = "brpop", .arity = -3, .flags = &.{ "write", "noscript" }, .first_key = 1, .last_key = -2, .step = 1 },
    .{ .name = "blmove", .arity = 6, .flags = &.{ "write", "denyoom", "noscript" }, .first_key = 1, .last_key = 2, .step = 1 },

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
    .{ .name = "waitaof", .arity = 4, .flags = &.{ "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "info", .arity = -1, .flags = &.{ "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Scan commands
    .{ .name = "scan", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 0, .last_key = 0, .step = 0 },

    // Sort command
    .{ .name = "sort", .arity = -2, .flags = &.{ "readonly", "movablekeys" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Object commands
    .{ .name = "object", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 2, .last_key = 2, .step = 1 },

    // Client commands
    .{ .name = "client", .arity = -2, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "reset", .arity = 1, .flags = &.{ "noscript", "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },

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

/// Documentation entry for COMMAND DOCS
pub const CommandDoc = struct {
    name: []const u8,
    summary: []const u8,
    since: []const u8,
    group: []const u8,
    complexity: []const u8,
    doc_flags: []const u8 = "",
    replaced_by: []const u8 = "",
};

/// Command documentation database (Redis 7.0+ COMMAND DOCS)
pub const COMMAND_DOCS = [_]CommandDoc{
    // String commands
    .{ .name = "set", .summary = "Set the string value of a key", .since = "1.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "get", .summary = "Get the value of a key", .since = "1.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "del", .summary = "Delete a key", .since = "1.0.0", .group = "generic", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "exists", .summary = "Determine if a key exists", .since = "1.0.0", .group = "generic", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "append", .summary = "Append a value to a key", .since = "2.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "strlen", .summary = "Get the length of the value stored in a key", .since = "2.2.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "incr", .summary = "Increment the integer value of a key by one", .since = "1.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "incrby", .summary = "Increment the integer value of a key by the given amount", .since = "1.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "incrbyfloat", .summary = "Increment the float value of a key by the given amount", .since = "2.6.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "decr", .summary = "Decrement the integer value of a key by one", .since = "1.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "decrby", .summary = "Decrement the integer value of a key by the given number", .since = "1.0.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "mget", .summary = "Get the values of all the given keys", .since = "1.0.0", .group = "string", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "mset", .summary = "Set multiple keys to multiple values", .since = "1.0.1", .group = "string", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "msetnx", .summary = "Set multiple keys to multiple values, only if none of the keys exist", .since = "1.0.1", .group = "string", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "getset", .summary = "Set the string value of a key and return its old value", .since = "1.0.0", .group = "string", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "set with get" },
    .{ .name = "getdel", .summary = "Get the value of a key and delete the key", .since = "6.2.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "getex", .summary = "Get the value of a key and optionally set its expiration", .since = "6.2.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "setnx", .summary = "Set the value of a key, only if the key does not exist", .since = "1.0.0", .group = "string", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "set with nx" },
    .{ .name = "setex", .summary = "Set the value and expiration of a key", .since = "1.0.0", .group = "string", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "set with ex" },
    .{ .name = "psetex", .summary = "Set the value and expiration in milliseconds of a key", .since = "2.6.0", .group = "string", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "set with px" },
    .{ .name = "getrange", .summary = "Get a substring of the string stored at a key", .since = "2.4.0", .group = "string", .complexity = "O(N) where N is the length of the returned string" },
    .{ .name = "setrange", .summary = "Overwrite part of a string at key starting at the specified offset", .since = "2.2.0", .group = "string", .complexity = "O(1)" },
    .{ .name = "substr", .summary = "Get a substring of the string stored at a key", .since = "1.0.0", .group = "string", .complexity = "O(N)", .doc_flags = "deprecated", .replaced_by = "getrange" },
    .{ .name = "lcs", .summary = "Find longest common substring", .since = "7.0.0", .group = "string", .complexity = "O(N*M) where N and M are the lengths of s1 and s2" },
    // List commands
    .{ .name = "lpush", .summary = "Prepend one or multiple elements to a list", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the number of elements" },
    .{ .name = "rpush", .summary = "Append one or multiple elements to a list", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the number of elements" },
    .{ .name = "lpop", .summary = "Remove and get the first elements in a list", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the number of elements returned" },
    .{ .name = "rpop", .summary = "Remove and get the last elements in a list", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the number of elements returned" },
    .{ .name = "lrange", .summary = "Get a range of elements from a list", .since = "1.0.0", .group = "list", .complexity = "O(S+N) where S is the distance of start and N is the number of elements" },
    .{ .name = "llen", .summary = "Get the length of a list", .since = "1.0.0", .group = "list", .complexity = "O(1)" },
    .{ .name = "lindex", .summary = "Get an element from a list by its index", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the number of elements to traverse" },
    .{ .name = "lset", .summary = "Set the value of an element in a list by its index", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the length of the list" },
    .{ .name = "linsert", .summary = "Insert an element before or after another element in a list", .since = "2.2.0", .group = "list", .complexity = "O(N) where N is the number of elements to traverse" },
    .{ .name = "lrem", .summary = "Remove elements from a list", .since = "1.0.0", .group = "list", .complexity = "O(N+M) where N is the length of the list and M is the number of elements removed" },
    .{ .name = "ltrim", .summary = "Trim a list to the specified range", .since = "1.0.0", .group = "list", .complexity = "O(N) where N is the number of elements removed" },
    .{ .name = "lpos", .summary = "Return the index of matching elements on a list", .since = "6.0.6", .group = "list", .complexity = "O(N) where N is the number of elements in the list" },
    .{ .name = "lmove", .summary = "Pop an element from one list, push and return it to another list", .since = "6.2.0", .group = "list", .complexity = "O(1)" },
    .{ .name = "lmpop", .summary = "Pop elements from a list", .since = "7.0.0", .group = "list", .complexity = "O(K)+(N) where K is the number of provided keys and N is the number of elements returned" },
    .{ .name = "blpop", .summary = "Remove and get the first element in a list, or block until one is available", .since = "2.0.0", .group = "list", .complexity = "O(N) where N is the number of provided keys" },
    .{ .name = "brpop", .summary = "Remove and get the last element in a list, or block until one is available", .since = "2.0.0", .group = "list", .complexity = "O(N) where N is the number of provided keys" },
    .{ .name = "blmove", .summary = "Pop an element from one list, push and return it to another list, block until the source is non-empty", .since = "6.2.0", .group = "list", .complexity = "O(1)" },
    .{ .name = "blmpop", .summary = "Pop elements from a list, or block until one is available", .since = "7.0.0", .group = "list", .complexity = "O(K)+(N) where K is the number of provided keys" },
    .{ .name = "rpoplpush", .summary = "Remove the last element in a list, prepend it to another list and return it", .since = "1.2.0", .group = "list", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "lmove" },
    .{ .name = "brpoplpush", .summary = "Pop an element from a list, push it to another list and return it; or block until one is available", .since = "2.2.0", .group = "list", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "blmove" },
    // Hash commands
    .{ .name = "hset", .summary = "Set the string value of a hash field", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields being set" },
    .{ .name = "hget", .summary = "Get the value of a hash field", .since = "2.0.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hdel", .summary = "Delete one or more hash fields", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields to be deleted" },
    .{ .name = "hexists", .summary = "Determine if a hash field exists", .since = "2.0.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hgetall", .summary = "Get all the fields and values in a hash", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the size of the hash" },
    .{ .name = "hkeys", .summary = "Get all the fields in a hash", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the size of the hash" },
    .{ .name = "hvals", .summary = "Get all the values in a hash", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the size of the hash" },
    .{ .name = "hlen", .summary = "Get the number of fields in a hash", .since = "2.0.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hmget", .summary = "Get the values of all the given hash fields", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields" },
    .{ .name = "hmset", .summary = "Set multiple hash fields to multiple values", .since = "2.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields", .doc_flags = "deprecated", .replaced_by = "hset" },
    .{ .name = "hincrby", .summary = "Increment the integer value of a hash field by the given number", .since = "2.0.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hincrbyfloat", .summary = "Increment the float value of a hash field by the given amount", .since = "2.6.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hsetnx", .summary = "Set the value of a hash field, only if the field does not exist", .since = "2.0.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hscan", .summary = "Incrementally iterate hash fields and associated values", .since = "2.8.0", .group = "hash", .complexity = "O(1) for every call. O(N) for a complete iteration" },
    .{ .name = "hrandfield", .summary = "Get one or multiple random fields from a hash", .since = "6.2.0", .group = "hash", .complexity = "O(N) where N is the number of fields returned" },
    .{ .name = "hgetdel", .summary = "Get the value of a field and delete it", .since = "8.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields" },
    .{ .name = "hgetex", .summary = "Get the value of a field and set its expiration", .since = "8.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields" },
    .{ .name = "hsetex", .summary = "Set the value of a field with expiration", .since = "8.0.0", .group = "hash", .complexity = "O(N) where N is the number of fields" },
    // Set commands
    .{ .name = "sadd", .summary = "Add one or more members to a set", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the number of elements added" },
    .{ .name = "srem", .summary = "Remove one or more members from a set", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the number of members" },
    .{ .name = "sismember", .summary = "Determine if a given value is a member of a set", .since = "1.0.0", .group = "set", .complexity = "O(1)" },
    .{ .name = "smismember", .summary = "Returns the membership associated with the given elements for a set", .since = "6.2.0", .group = "set", .complexity = "O(N) where N is the number of elements" },
    .{ .name = "smembers", .summary = "Get all the members in a set", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the set cardinality" },
    .{ .name = "scard", .summary = "Get the number of members in a set", .since = "1.0.0", .group = "set", .complexity = "O(1)" },
    .{ .name = "sunion", .summary = "Add multiple sets", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the total number of elements in all sets" },
    .{ .name = "sinter", .summary = "Intersect multiple sets", .since = "1.0.0", .group = "set", .complexity = "O(N*M) where N is smallest set and M is the number of sets" },
    .{ .name = "sdiff", .summary = "Subtract multiple sets", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the total number of elements in all sets" },
    .{ .name = "sunionstore", .summary = "Add multiple sets and store the resulting set in a key", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the total number of elements in all sets" },
    .{ .name = "sinterstore", .summary = "Intersect multiple sets and store the resulting set in a key", .since = "1.0.0", .group = "set", .complexity = "O(N*M) where N is smallest set and M is the number of sets" },
    .{ .name = "sdiffstore", .summary = "Subtract multiple sets and store the resulting set in a key", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the total number of elements in all sets" },
    .{ .name = "smove", .summary = "Move a member from one set to another", .since = "1.0.0", .group = "set", .complexity = "O(1)" },
    .{ .name = "spop", .summary = "Remove and return one or multiple random members from a set", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the count value" },
    .{ .name = "srandmember", .summary = "Get one or multiple random members from a set", .since = "1.0.0", .group = "set", .complexity = "O(N) where N is the count" },
    .{ .name = "sscan", .summary = "Incrementally iterate Set elements", .since = "2.8.0", .group = "set", .complexity = "O(1) for every call. O(N) for a complete iteration" },
    .{ .name = "sintercard", .summary = "Intersect multiple sets and return the cardinality of the result", .since = "7.0.0", .group = "set", .complexity = "O(N*M) where N is smallest set and M is the number of sets" },
    // Sorted set commands
    .{ .name = "zadd", .summary = "Add one or more members to a sorted set, or update its score if it already exists", .since = "1.2.0", .group = "sorted_set", .complexity = "O(log(N)) for each item added" },
    .{ .name = "zrem", .summary = "Remove one or more members from a sorted set", .since = "1.2.0", .group = "sorted_set", .complexity = "O(M*log(N)) where N is the number of elements and M is the number removed" },
    .{ .name = "zscore", .summary = "Get the score associated with the given member in a sorted set", .since = "1.2.0", .group = "sorted_set", .complexity = "O(1)" },
    .{ .name = "zrank", .summary = "Determine the index of a member in a sorted set", .since = "2.0.0", .group = "sorted_set", .complexity = "O(log(N))" },
    .{ .name = "zrevrank", .summary = "Determine the index of a member in a sorted set, with scores ordered from high to low", .since = "2.0.0", .group = "sorted_set", .complexity = "O(log(N))" },
    .{ .name = "zcard", .summary = "Get the number of members in a sorted set", .since = "1.2.0", .group = "sorted_set", .complexity = "O(1)" },
    .{ .name = "zcount", .summary = "Count the members in a sorted set with scores within the given values", .since = "2.0.0", .group = "sorted_set", .complexity = "O(log(N)) where N is the number of elements" },
    .{ .name = "zlexcount", .summary = "Count the number of members in a sorted set between a given lexicographical range", .since = "2.8.9", .group = "sorted_set", .complexity = "O(log(N)) where N is the number of elements" },
    .{ .name = "zincrby", .summary = "Increment the score of a member in a sorted set", .since = "1.2.0", .group = "sorted_set", .complexity = "O(log(N)) where N is the number of elements" },
    .{ .name = "zrange", .summary = "Return a range of members in a sorted set", .since = "1.2.0", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number returned" },
    .{ .name = "zrangebyscore", .summary = "Return a range of members in a sorted set, by score", .since = "1.0.5", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number returned", .doc_flags = "deprecated", .replaced_by = "zrange with byscore" },
    .{ .name = "zrevrangebyscore", .summary = "Return a range of members in a sorted set, by score, with scores ordered from high to low", .since = "2.2.0", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number returned", .doc_flags = "deprecated", .replaced_by = "zrange with byscore rev" },
    .{ .name = "zrangebylex", .summary = "Return a range of members in a sorted set, by lexicographical range", .since = "2.8.9", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number returned", .doc_flags = "deprecated", .replaced_by = "zrange with bylex" },
    .{ .name = "zrevrangebylex", .summary = "Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings", .since = "2.8.9", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number returned", .doc_flags = "deprecated", .replaced_by = "zrange with bylex rev" },
    .{ .name = "zrevrange", .summary = "Return a range of members in a sorted set, by index, with scores ordered from high to low", .since = "1.2.0", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number returned", .doc_flags = "deprecated", .replaced_by = "zrange with rev" },
    .{ .name = "zrangestore", .summary = "Store a range of members from sorted set into another key", .since = "6.2.0", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number of elements returned" },
    .{ .name = "zmscore", .summary = "Get the score associated with the given members in a sorted set", .since = "6.2.0", .group = "sorted_set", .complexity = "O(N) where N is the number of members" },
    .{ .name = "zpopmin", .summary = "Remove and return members with the lowest scores in a sorted set", .since = "5.0.0", .group = "sorted_set", .complexity = "O(log(N)*M) where N is the number of elements and M is the number popped" },
    .{ .name = "zpopmax", .summary = "Remove and return members with the highest scores in a sorted set", .since = "5.0.0", .group = "sorted_set", .complexity = "O(log(N)*M) where N is the number of elements and M is the number popped" },
    .{ .name = "bzpopmin", .summary = "Remove and return the member with the lowest score from one or more sorted sets, or block until one is available", .since = "5.0.0", .group = "sorted_set", .complexity = "O(log(N)) where N is the number of elements" },
    .{ .name = "bzpopmax", .summary = "Remove and return the member with the highest score from one or more sorted sets, or block until one is available", .since = "5.0.0", .group = "sorted_set", .complexity = "O(log(N)) where N is the number of elements" },
    .{ .name = "bzmpop", .summary = "Remove and return members with scores in a sorted set or block until one is available", .since = "7.0.0", .group = "sorted_set", .complexity = "O(K)+(M*log(N)) where K is the number of provided keys" },
    .{ .name = "zmpop", .summary = "Remove and return members with scores in a sorted set", .since = "7.0.0", .group = "sorted_set", .complexity = "O(K)+(M*log(N)) where K is the number of provided keys" },
    .{ .name = "zunionstore", .summary = "Add multiple sorted sets and store the resulting sorted set in a new key", .since = "2.0.0", .group = "sorted_set", .complexity = "O(N)+O(M log(M)) where N is the sum of sizes of the input sorted sets and M is the number of elements in the resulting sorted set" },
    .{ .name = "zinterstore", .summary = "Intersect multiple sorted sets and store the resulting sorted set in a new key", .since = "2.0.0", .group = "sorted_set", .complexity = "O(N*K)+O(M*log(M)) where N is the smallest input sorted set and M is the number of elements in the resulting sorted set and K is the number of sorted sets" },
    .{ .name = "zdiffstore", .summary = "Subtract multiple sorted sets and store the resulting sorted set in a new key", .since = "6.2.0", .group = "sorted_set", .complexity = "O(L + (K-1)*M*log(M)) where L is the total number of elements in all the sets, K is the number of sets, M is the number of elements in the result" },
    .{ .name = "zunion", .summary = "Add multiple sorted sets", .since = "6.2.0", .group = "sorted_set", .complexity = "O(N)+O(M*log(M)) where N is the sum of sizes of the input sorted sets and M is the number of elements in the resulting sorted set" },
    .{ .name = "zinter", .summary = "Intersect multiple sorted sets", .since = "6.2.0", .group = "sorted_set", .complexity = "O(N*K)+O(M*log(M)) where N is the smallest input sorted set, K is the number of input sorted sets, and M is the number of elements in the resulting sorted set" },
    .{ .name = "zdiff", .summary = "Subtract multiple sorted sets", .since = "6.2.0", .group = "sorted_set", .complexity = "O(L + (K-1)*M*log(M)) where L is the total number of elements in all the sets, K is the number of sets, M is the number of elements in the result" },
    .{ .name = "zintercard", .summary = "Intersect multiple sorted sets and return the cardinality of the result", .since = "7.0.0", .group = "sorted_set", .complexity = "O(N*K) worst case where N is the smallest input sorted set, K is the number of input sorted sets" },
    .{ .name = "zrandmember", .summary = "Get one or multiple random elements from a sorted set", .since = "6.2.0", .group = "sorted_set", .complexity = "O(N) where N is the number of elements returned" },
    .{ .name = "zscan", .summary = "Incrementally iterate sorted sets elements and associated scores", .since = "2.8.0", .group = "sorted_set", .complexity = "O(1) for every call. O(N) for a complete iteration" },
    // Key management
    .{ .name = "expire", .summary = "Set a key's time to live in seconds", .since = "1.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "expireat", .summary = "Set the expiration for a key as a UNIX timestamp", .since = "1.2.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "pexpire", .summary = "Set a key's time to live in milliseconds", .since = "2.6.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "pexpireat", .summary = "Set the expiration for a key as a UNIX timestamp specified in milliseconds", .since = "2.6.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "ttl", .summary = "Get the time to live for a key in seconds", .since = "1.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "pttl", .summary = "Get the time to live for a key in milliseconds", .since = "2.6.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "expiretime", .summary = "Get the expiration Unix timestamp for a key", .since = "7.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "pexpiretime", .summary = "Get the expiration Unix timestamp for a key in milliseconds", .since = "7.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "persist", .summary = "Remove the expiration from a key", .since = "2.2.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "keys", .summary = "Find all keys matching the given pattern", .since = "1.0.0", .group = "generic", .complexity = "O(N) where N is the number of keys in the database" },
    .{ .name = "scan", .summary = "Incrementally iterate the keys space", .since = "2.8.0", .group = "generic", .complexity = "O(1) for every call. O(N) for a complete iteration" },
    .{ .name = "type", .summary = "Determine the type stored at key", .since = "1.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "rename", .summary = "Rename a key", .since = "1.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "renamenx", .summary = "Rename a key, only if the new key does not exist", .since = "1.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "randomkey", .summary = "Return a random key from the keyspace", .since = "1.0.0", .group = "generic", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "unlink", .summary = "Delete a key asynchronously in another thread. Otherwise it is just as DEL, but non blocking", .since = "4.0.0", .group = "generic", .complexity = "O(1) for each key removed" },
    .{ .name = "copy", .summary = "Copy a key", .since = "6.2.0", .group = "generic", .complexity = "O(N) where N is the number of nested values" },
    .{ .name = "move", .summary = "Move a key to another database", .since = "1.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "dump", .summary = "Return a serialized version of the value stored at the specified key", .since = "2.6.0", .group = "generic", .complexity = "O(1) to access the key and additional O(N*M) to serialize" },
    .{ .name = "restore", .summary = "Create a key using the provided serialized value, previously obtained using DUMP", .since = "2.6.0", .group = "generic", .complexity = "O(1) to create the new key and additional O(N*M) to reconstruct the serialized value" },
    .{ .name = "touch", .summary = "Alters the last access time of a key(s)", .since = "3.2.1", .group = "generic", .complexity = "O(N) where N is the number of keys" },
    .{ .name = "object", .summary = "A container for object introspection commands", .since = "2.2.3", .group = "generic", .complexity = "O(1)" },
    .{ .name = "sort", .summary = "Sort the elements in a list, set or sorted set", .since = "1.0.0", .group = "generic", .complexity = "O(N+M*log(M)) where N is the number of elements and M the number of returned elements" },
    .{ .name = "sort_ro", .summary = "Sort the elements in a list, set or sorted set. Read-only variant of SORT", .since = "7.0.0", .group = "generic", .complexity = "O(N+M*log(M)) where N is the number of elements and M the number of returned elements" },
    .{ .name = "wait", .summary = "Wait for the synchronous replication of all the write commands sent in the context of the current connection", .since = "3.0.0", .group = "generic", .complexity = "O(1)" },
    .{ .name = "waitaof", .summary = "Wait until writes are synced to AOF and fsync'd to the disk", .since = "7.2.0", .group = "generic", .complexity = "O(1)" },
    // Pub/Sub
    .{ .name = "subscribe", .summary = "Listen for messages published to the given channels", .since = "2.0.0", .group = "pubsub", .complexity = "O(N) where N is the number of channels to subscribe" },
    .{ .name = "unsubscribe", .summary = "Stop listening for messages posted to the given channels", .since = "2.0.0", .group = "pubsub", .complexity = "O(N) where N is the number of clients to be unsubscribed" },
    .{ .name = "publish", .summary = "Post a message to a channel", .since = "2.0.0", .group = "pubsub", .complexity = "O(N+M) where N is the number of clients subscribed to the receiving channel and M is the total number of subscribed patterns" },
    .{ .name = "psubscribe", .summary = "Listen for messages published to channels matching the given patterns", .since = "2.0.0", .group = "pubsub", .complexity = "O(N) where N is the number of patterns the client is already subscribed to" },
    .{ .name = "punsubscribe", .summary = "Stop listening for messages posted to channels matching the given patterns", .since = "2.0.0", .group = "pubsub", .complexity = "O(N+M) where N is the number of patterns the client is already subscribed to and M is the number of total patterns subscribed in the system" },
    .{ .name = "pubsub", .summary = "A container for Pub/Sub commands", .since = "2.8.0", .group = "pubsub", .complexity = "O(N)" },
    .{ .name = "ssubscribe", .summary = "Listen for messages published to the given shard channels", .since = "7.0.0", .group = "pubsub", .complexity = "O(N) where N is the number of shard channels to subscribe" },
    .{ .name = "sunsubscribe", .summary = "Stop listening for messages posted to the given shard channels", .since = "7.0.0", .group = "pubsub", .complexity = "O(N) where N is the number of shard channels to unsubscribe" },
    .{ .name = "spublish", .summary = "Post a message to a shard channel", .since = "7.0.0", .group = "pubsub", .complexity = "O(N) where N is the number of clients subscribed to the receiving shard channel" },
    // Transactions
    .{ .name = "multi", .summary = "Mark the start of a transaction block", .since = "1.2.0", .group = "transactions", .complexity = "O(1)" },
    .{ .name = "exec", .summary = "Execute all commands issued after MULTI", .since = "1.2.0", .group = "transactions", .complexity = "Depends on commands in the transaction" },
    .{ .name = "discard", .summary = "Discard all commands issued after MULTI", .since = "2.0.0", .group = "transactions", .complexity = "O(N) where N is the number of commands in the transaction" },
    .{ .name = "watch", .summary = "Watch the given keys to determine execution of the MULTI/EXEC block", .since = "2.2.0", .group = "transactions", .complexity = "O(1) for every key" },
    .{ .name = "unwatch", .summary = "Forget about all watched keys", .since = "2.2.0", .group = "transactions", .complexity = "O(1)" },
    // Connection
    .{ .name = "ping", .summary = "Ping the server", .since = "1.0.0", .group = "connection", .complexity = "O(1)" },
    .{ .name = "echo", .summary = "Echo the given string", .since = "1.0.0", .group = "connection", .complexity = "O(1)" },
    .{ .name = "quit", .summary = "Close the connection", .since = "1.0.0", .group = "connection", .complexity = "O(1)", .doc_flags = "deprecated" },
    .{ .name = "select", .summary = "Change the selected database for the current connection", .since = "1.0.0", .group = "connection", .complexity = "O(1)" },
    .{ .name = "auth", .summary = "Authenticate to the server", .since = "1.0.0", .group = "connection", .complexity = "O(N) where N is the password length" },
    .{ .name = "reset", .summary = "Reset the connection", .since = "6.2.0", .group = "connection", .complexity = "O(1)" },
    .{ .name = "hello", .summary = "Handshake with Redis", .since = "6.0.0", .group = "connection", .complexity = "O(1)" },
    // Server
    .{ .name = "info", .summary = "Get information and statistics about the server", .since = "1.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "dbsize", .summary = "Return the number of keys in the selected database", .since = "1.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "flushdb", .summary = "Remove all keys from the current database", .since = "1.0.0", .group = "server", .complexity = "O(N) where N is the number of keys in the database" },
    .{ .name = "flushall", .summary = "Remove all keys from all databases", .since = "1.0.0", .group = "server", .complexity = "O(N) where N is the total number of keys in all databases" },
    .{ .name = "bgsave", .summary = "Asynchronously save the dataset to disk", .since = "1.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "bgrewriteaof", .summary = "Asynchronously rewrite the append-only file", .since = "1.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "save", .summary = "Synchronously save the dataset to disk", .since = "1.0.0", .group = "server", .complexity = "O(N)" },
    .{ .name = "lastsave", .summary = "Get the UNIX time stamp of the last successful save to disk", .since = "1.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "shutdown", .summary = "Synchronously save the dataset to disk and then shut down the server", .since = "1.0.0", .group = "server", .complexity = "O(N)" },
    .{ .name = "time", .summary = "Return the current server time", .since = "2.6.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "command", .summary = "Get array of Redis command details", .since = "2.8.13", .group = "server", .complexity = "O(N) where N is the total number of Redis commands" },
    .{ .name = "client", .summary = "A container for client connection commands", .since = "2.4.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "config", .summary = "A container for server configuration commands", .since = "2.0.0", .group = "server", .complexity = "O(N) when called with a value" },
    .{ .name = "debug", .summary = "A container for debugging commands", .since = "1.0.0", .group = "server", .complexity = "Depends on subcommand" },
    .{ .name = "slowlog", .summary = "A container for slow log commands", .since = "2.2.12", .group = "server", .complexity = "O(1)" },
    .{ .name = "memory", .summary = "A container for memory introspection commands", .since = "4.0.0", .group = "server", .complexity = "Depends on subcommand" },
    .{ .name = "latency", .summary = "A container for latency diagnostics commands", .since = "2.8.13", .group = "server", .complexity = "O(1)" },
    .{ .name = "monitor", .summary = "Listen for all requests received by the server in real time", .since = "1.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "acl", .summary = "A container for Access Control List commands", .since = "6.0.0", .group = "server", .complexity = "Depends on subcommand" },
    .{ .name = "module", .summary = "A container for module commands", .since = "4.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "lolwut", .summary = "Display some computer art and the Redis version", .since = "5.0.0", .group = "server", .complexity = "Depends on version" },
    .{ .name = "swapdb", .summary = "Swaps two Redis databases", .since = "4.0.0", .group = "server", .complexity = "O(N) where N is the count of clients watching or blocking on keys from both databases" },
    .{ .name = "replicaof", .summary = "Make the server a replica of another instance, or promote it as master", .since = "5.0.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "slaveof", .summary = "Make the server a replica of another instance, or promote it as master", .since = "1.0.0", .group = "server", .complexity = "O(1)", .doc_flags = "deprecated", .replaced_by = "replicaof" },
    .{ .name = "failover", .summary = "Start a coordinated failover between this server and one of its replicas", .since = "6.2.0", .group = "server", .complexity = "O(1)" },
    .{ .name = "role", .summary = "Return the role of the server in the context of replication", .since = "2.8.12", .group = "server", .complexity = "O(1)" },
    .{ .name = "cluster", .summary = "A container for cluster commands", .since = "3.0.0", .group = "cluster", .complexity = "O(1)" },
    .{ .name = "sentinel", .summary = "A container for Sentinel commands", .since = "2.8.4", .group = "server", .complexity = "O(1)" },
    // Scripting
    .{ .name = "eval", .summary = "Execute a Lua script server side", .since = "2.6.0", .group = "scripting", .complexity = "Depends on the script" },
    .{ .name = "evalsha", .summary = "Execute a Lua script server side", .since = "2.6.0", .group = "scripting", .complexity = "Depends on the script" },
    .{ .name = "eval_ro", .summary = "Execute a read-only Lua script server side", .since = "7.0.0", .group = "scripting", .complexity = "Depends on the script" },
    .{ .name = "evalsha_ro", .summary = "Execute a read-only Lua script server side", .since = "7.0.0", .group = "scripting", .complexity = "Depends on the script" },
    .{ .name = "script", .summary = "A container for Lua scripts management commands", .since = "2.6.0", .group = "scripting", .complexity = "Depends on subcommand" },
    .{ .name = "fcall", .summary = "Invoke a function", .since = "7.0.0", .group = "scripting", .complexity = "Depends on the function" },
    .{ .name = "fcall_ro", .summary = "Read-only variant of FCALL that cannot execute commands that modify data", .since = "7.0.0", .group = "scripting", .complexity = "Depends on the function" },
    .{ .name = "function", .summary = "A container for function commands", .since = "7.0.0", .group = "scripting", .complexity = "Depends on subcommand" },
    // Streams
    .{ .name = "xadd", .summary = "Appends a new entry to a stream", .since = "5.0.0", .group = "stream", .complexity = "O(1) when adding a new entry" },
    .{ .name = "xlen", .summary = "Return the number of entries in a stream", .since = "5.0.0", .group = "stream", .complexity = "O(1)" },
    .{ .name = "xrange", .summary = "Return a range of elements in a stream, with IDs matching the specified IDs interval", .since = "5.0.0", .group = "stream", .complexity = "O(N) where N is the number of elements" },
    .{ .name = "xrevrange", .summary = "Return a range of elements in a stream, with IDs matching the specified IDs interval, in reverse order", .since = "5.0.0", .group = "stream", .complexity = "O(N) where N is the number of elements returned" },
    .{ .name = "xread", .summary = "Return never seen elements in multiple streams, with IDs greater than the ones reported by the caller for each stream", .since = "5.0.0", .group = "stream", .complexity = "O(N) where N is the total number of elements returned" },
    .{ .name = "xdel", .summary = "Removes the specified entries from the stream", .since = "5.0.0", .group = "stream", .complexity = "O(1) for each single item to delete in the stream" },
    .{ .name = "xtrim", .summary = "Trims the stream to a given number of items, evicting older items", .since = "5.0.0", .group = "stream", .complexity = "O(N) where N is the number of evicted entries" },
    .{ .name = "xinfo", .summary = "A container for stream introspection commands", .since = "5.0.0", .group = "stream", .complexity = "O(N) where N is the number of entries for some subcommands" },
    .{ .name = "xgroup", .summary = "A container for consumer groups commands", .since = "5.0.0", .group = "stream", .complexity = "Depends on subcommand" },
    .{ .name = "xack", .summary = "Marks a pending message as correctly processed, effectively removing it from the pending entries list of the consumer group", .since = "5.0.0", .group = "stream", .complexity = "O(1) for each message ID processed" },
    .{ .name = "xclaim", .summary = "Changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument", .since = "5.0.0", .group = "stream", .complexity = "O(log(N)) with N being the number of messages in the PEL" },
    .{ .name = "xautoclaim", .summary = "Changes the ownership of pending messages, so that the new owner is the consumer specified as the command argument", .since = "6.2.0", .group = "stream", .complexity = "O(1) if COUNT is small" },
    .{ .name = "xpending", .summary = "Return information and entries from a stream consumer group pending entries list", .since = "5.0.0", .group = "stream", .complexity = "O(N) with N being the number of elements returned" },
    .{ .name = "xreadgroup", .summary = "Return new or history entries from a stream consumer group", .since = "5.0.0", .group = "stream", .complexity = "O(M) with M being the number of elements returned per call" },
    .{ .name = "xsetid", .summary = "An internal command for replicating stream values", .since = "5.0.0", .group = "stream", .complexity = "O(1)" },
    // Bitmap
    .{ .name = "setbit", .summary = "Sets or clears the bit at offset in the string value stored at key", .since = "2.2.0", .group = "bitmap", .complexity = "O(1)" },
    .{ .name = "getbit", .summary = "Returns the bit value at offset in the string value stored at key", .since = "2.2.0", .group = "bitmap", .complexity = "O(1)" },
    .{ .name = "bitcount", .summary = "Count set bits in a string", .since = "2.6.0", .group = "bitmap", .complexity = "O(N)" },
    .{ .name = "bitop", .summary = "Perform bitwise operations between strings", .since = "2.6.0", .group = "bitmap", .complexity = "O(N)" },
    .{ .name = "bitpos", .summary = "Find first bit set or clear in a string", .since = "2.8.7", .group = "bitmap", .complexity = "O(N)" },
    .{ .name = "bitfield", .summary = "Perform arbitrary bitfield integer operations on strings", .since = "3.2.0", .group = "bitmap", .complexity = "O(1) for each subcommand specified" },
    .{ .name = "bitfield_ro", .summary = "Perform arbitrary bitfield integer operations on strings, read-only variant", .since = "6.0.0", .group = "bitmap", .complexity = "O(1) for each subcommand specified" },
    // HyperLogLog
    .{ .name = "pfadd", .summary = "Adds the specified elements to the specified HyperLogLog", .since = "2.8.9", .group = "hyperloglog", .complexity = "O(1) to add every element" },
    .{ .name = "pfcount", .summary = "Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s)", .since = "2.8.9", .group = "hyperloglog", .complexity = "O(1) for single key, O(N) for multiple keys" },
    .{ .name = "pfmerge", .summary = "Merge N different HyperLogLogs into a single one", .since = "2.8.9", .group = "hyperloglog", .complexity = "O(N) to merge N HyperLogLogs" },
    // Geospatial
    .{ .name = "geoadd", .summary = "Add one or more geospatial items in the geospatial index represented using a sorted set", .since = "3.2.0", .group = "geo", .complexity = "O(log(N)) for each item added" },
    .{ .name = "geodist", .summary = "Returns the distance between two members in the geospatial index represented by the sorted set", .since = "3.2.0", .group = "geo", .complexity = "O(log(N))" },
    .{ .name = "geohash", .summary = "Returns members of a geospatial index as standard geohash strings", .since = "3.2.0", .group = "geo", .complexity = "O(log(N)) for each member requested" },
    .{ .name = "geopos", .summary = "Returns longitude and latitude of members of a geospatial index", .since = "3.2.0", .group = "geo", .complexity = "O(N) where N is the number of members requested" },
    .{ .name = "georadius", .summary = "Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a point", .since = "3.2.0", .group = "geo", .complexity = "O(N+log(M)) where M is the number of elements inside the bounding box and N is the number of elements", .doc_flags = "deprecated", .replaced_by = "geosearch" },
    .{ .name = "georadiusbymember", .summary = "Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a member", .since = "3.2.0", .group = "geo", .complexity = "O(N+log(M)) where M is the number of elements inside the bounding box and N is the number of elements", .doc_flags = "deprecated", .replaced_by = "geosearch" },
    .{ .name = "geosearch", .summary = "Query a sorted set representing a geospatial index to fetch members inside an area of a box or a circle", .since = "6.2.0", .group = "geo", .complexity = "O(N+log(M)) where M is the number of elements in the geo index and N is the number of items being returned" },
    .{ .name = "geosearchstore", .summary = "Query a sorted set representing a geospatial index to fetch members inside an area of a box or a circle, and store the result in another key", .since = "6.2.0", .group = "geo", .complexity = "O(N+log(M)) where M is the number of elements in the geo index and N is the number of items being stored" },
};

/// COMMAND DOCS [command-name [command-name ...]] — Return documentation for commands (Redis 7.0+)
/// Returns a map-like structure: [name, {summary, since, group, complexity, arguments, ...}, ...]
pub fn cmdCommandDocs(allocator: std.mem.Allocator, args: []const []const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (args.len == 0) {
        // Return docs for all commands
        try buf.append(allocator, '*');
        try std.fmt.format(buf.writer(allocator), "{d}", .{COMMAND_DOCS.len * 2});
        try buf.appendSlice(allocator, "\r\n");
        for (COMMAND_DOCS) |doc| {
            try writeBulkString(allocator, &buf, doc.name);
            try writeDocEntry(allocator, &buf, doc);
        }
    } else {
        // Return docs for specified commands
        var found_count: usize = 0;
        for (args) |arg| {
            for (COMMAND_DOCS) |doc| {
                if (std.ascii.eqlIgnoreCase(doc.name, arg)) {
                    found_count += 1;
                    break;
                }
            }
        }
        try buf.append(allocator, '*');
        try std.fmt.format(buf.writer(allocator), "{d}", .{found_count * 2});
        try buf.appendSlice(allocator, "\r\n");
        for (args) |arg| {
            for (COMMAND_DOCS) |doc| {
                if (std.ascii.eqlIgnoreCase(doc.name, arg)) {
                    try writeBulkString(allocator, &buf, doc.name);
                    try writeDocEntry(allocator, &buf, doc);
                    break;
                }
            }
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// Write a single command doc entry as a flat-map array
fn writeDocEntry(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), doc: CommandDoc) !void {
    // Count fields (summary, since, group, complexity always present; doc_flags and replaced_by if non-empty)
    var field_count: usize = 4 * 2; // summary, since, group, complexity
    if (doc.doc_flags.len > 0) field_count += 2;
    if (doc.replaced_by.len > 0) field_count += 2;
    field_count += 2; // arguments (always present, may be empty)

    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{field_count});
    try buf.appendSlice(allocator, "\r\n");

    // summary
    try writeBulkString(allocator, buf, "summary");
    try writeBulkString(allocator, buf, doc.summary);
    // since
    try writeBulkString(allocator, buf, "since");
    try writeBulkString(allocator, buf, doc.since);
    // group
    try writeBulkString(allocator, buf, "group");
    try writeBulkString(allocator, buf, doc.group);
    // complexity
    try writeBulkString(allocator, buf, "complexity");
    try writeBulkString(allocator, buf, doc.complexity);
    // doc_flags (optional)
    if (doc.doc_flags.len > 0) {
        try writeBulkString(allocator, buf, "doc_flags");
        try writeBulkString(allocator, buf, doc.doc_flags);
    }
    // replaced_by (optional)
    if (doc.replaced_by.len > 0) {
        try writeBulkString(allocator, buf, "replaced_by");
        try writeBulkString(allocator, buf, doc.replaced_by);
    }
    // arguments (empty array — full argument spec is complex)
    try writeBulkString(allocator, buf, "arguments");
    try buf.appendSlice(allocator, "*0\r\n");
}

/// COMMAND GETKEYSANDFLAGS command [arg [arg ...]] — Extract keys and per-key flags (Redis 7.0+)
/// Returns an array where each element is [key, [flags...]] for each key in the command.
/// Key flags: "read", "write", "delete", "not_key", "incomplete", "channel".
pub fn cmdCommandGetKeysAndFlags(allocator: std.mem.Allocator, args: []const []const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (args.len == 0) {
        try buf.appendSlice(allocator, "-ERR wrong number of arguments for 'command|getkeysandflags' command\r\n");
        return buf.toOwnedSlice(allocator);
    }

    const cmd_name = args[0];
    for (ALL_COMMANDS) |cmd| {
        if (std.ascii.eqlIgnoreCase(cmd.name, cmd_name)) {
            if (cmd.first_key == 0) {
                try buf.appendSlice(allocator, "*0\r\n");
                return buf.toOwnedSlice(allocator);
            }

            // Derive key-level flags from command-level flags
            var is_read = false;
            var is_write = false;
            var is_delete = false;
            for (cmd.flags) |flag| {
                if (std.mem.eql(u8, flag, "readonly")) is_read = true;
                if (std.mem.eql(u8, flag, "write")) is_write = true;
                if (std.mem.eql(u8, flag, "write") and
                    (std.ascii.eqlIgnoreCase(cmd.name, "del") or
                    std.ascii.eqlIgnoreCase(cmd.name, "unlink") or
                    std.ascii.eqlIgnoreCase(cmd.name, "getdel"))) is_delete = true;
            }

            // Build key-level flags array
            var key_flags = std.ArrayList([]const u8){};
            defer key_flags.deinit(allocator);
            if (is_read) try key_flags.append(allocator, "read");
            if (is_write and !is_delete) try key_flags.append(allocator, "write");
            if (is_delete) {
                try key_flags.append(allocator, "write");
                try key_flags.append(allocator, "delete");
            }
            if (!is_read and !is_write) {
                try key_flags.append(allocator, "read");
                try key_flags.append(allocator, "write");
            }

            // Count keys
            var key_count: usize = 0;
            var i: i32 = cmd.first_key;
            while (i <= cmd.last_key or cmd.last_key == -1) : (i += cmd.step) {
                const idx: usize = @intCast(i);
                if (idx >= args.len) break;
                key_count += 1;
                if (cmd.last_key != -1 and i >= cmd.last_key) break;
            }

            // Outer array: N keys
            try buf.append(allocator, '*');
            try std.fmt.format(buf.writer(allocator), "{d}", .{key_count});
            try buf.appendSlice(allocator, "\r\n");

            // Each key: [key, [flags...]]
            i = cmd.first_key;
            while (i <= cmd.last_key or cmd.last_key == -1) : (i += cmd.step) {
                const idx: usize = @intCast(i);
                if (idx >= args.len) break;

                // Two-element array: [key, flags]
                try buf.appendSlice(allocator, "*2\r\n");
                try writeBulkString(allocator, &buf, args[idx]);

                // Flags array
                try buf.append(allocator, '*');
                try std.fmt.format(buf.writer(allocator), "{d}", .{key_flags.items.len});
                try buf.appendSlice(allocator, "\r\n");
                for (key_flags.items) |flag| {
                    try writeBulkString(allocator, &buf, flag);
                }

                if (cmd.last_key != -1 and i >= cmd.last_key) break;
            }

            return buf.toOwnedSlice(allocator);
        }
    }

    try buf.appendSlice(allocator, "-ERR Invalid command specified\r\n");
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
        "COMMAND GETKEYSANDFLAGS <command> [<arg> ...] - Extract keys and access flags (Redis 7.0+)",
        "COMMAND LIST [FILTERBY <filter>] - List command names (Redis 7.0+)",
        "COMMAND DOCS [<command> ...] - Return documentation for commands (Redis 7.0+)",
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

/// Derive ACL categories from command flags and name for Redis 7.0+ format.
/// Returns a stack-allocated slice of category strings.
fn deriveAclCategories(allocator: std.mem.Allocator, cmd: CommandInfo) ![]const []const u8 {
    var cats = std.ArrayList([]const u8){};
    errdefer cats.deinit(allocator);

    // Access direction from flags
    var has_read = false;
    var has_write = false;
    var has_admin = false;
    var has_fast = false;
    var has_pubsub = false;
    var has_scripting = false;
    var has_sortedset = false;
    var has_string = false;
    var has_list = false;
    var has_set = false;
    var has_hash = false;
    var has_hyperloglog = false;
    var has_geo = false;
    const has_bitmap = false;
    var has_stream = false;
    var has_cluster = false;
    var has_server = false;
    var has_transactions = false;
    var has_connection = false;
    var has_generic = false;
    var has_dangerous = false;

    for (cmd.flags) |flag| {
        if (std.mem.eql(u8, flag, "readonly")) has_read = true;
        if (std.mem.eql(u8, flag, "write")) has_write = true;
        if (std.mem.eql(u8, flag, "admin")) has_admin = true;
        if (std.mem.eql(u8, flag, "fast")) has_fast = true;
        if (std.mem.eql(u8, flag, "pubsub")) has_pubsub = true;
        if (std.mem.eql(u8, flag, "scripting")) has_scripting = true;
        if (std.mem.eql(u8, flag, "dangerous")) has_dangerous = true;
    }

    // If explicit acl_categories are provided, use those instead
    if (cmd.acl_categories.len > 0) {
        for (cmd.acl_categories) |cat| {
            try cats.append(allocator, cat);
        }
        return cats.toOwnedSlice(allocator);
    }

    // Derive data type category from command name prefix
    const name_lower = try std.ascii.allocLowerString(allocator, cmd.name);
    defer allocator.free(name_lower);

    if (std.mem.startsWith(u8, name_lower, "z") or
        std.mem.eql(u8, name_lower, "zadd") or
        std.mem.eql(u8, name_lower, "zrem"))
    {
        has_sortedset = true;
    } else if (std.mem.startsWith(u8, name_lower, "h") and !std.mem.eql(u8, name_lower, "hello")) {
        has_hash = true;
    } else if (std.mem.startsWith(u8, name_lower, "s") and !std.mem.eql(u8, name_lower, "set") and
        !std.mem.eql(u8, name_lower, "setex") and !std.mem.eql(u8, name_lower, "setnx") and
        !std.mem.eql(u8, name_lower, "setrange") and !std.mem.eql(u8, name_lower, "subscribe") and
        !std.mem.eql(u8, name_lower, "ssubscribe") and !std.mem.eql(u8, name_lower, "sunsubscribe") and
        !std.mem.startsWith(u8, name_lower, "sort") and !std.mem.eql(u8, name_lower, "spublish"))
    {
        has_set = true;
    } else if (std.mem.startsWith(u8, name_lower, "l") and !std.mem.eql(u8, name_lower, "lolwut")) {
        has_list = true;
    } else if (std.mem.startsWith(u8, name_lower, "geo")) {
        has_geo = true;
    } else if (std.mem.startsWith(u8, name_lower, "pfadd") or
        std.mem.startsWith(u8, name_lower, "pfcount") or
        std.mem.startsWith(u8, name_lower, "pfmerge"))
    {
        has_hyperloglog = true;
    } else if (std.mem.startsWith(u8, name_lower, "x")) {
        has_stream = true;
    } else if (std.mem.startsWith(u8, name_lower, "cluster")) {
        has_cluster = true;
    } else if (std.mem.eql(u8, name_lower, "multi") or std.mem.eql(u8, name_lower, "exec") or
        std.mem.eql(u8, name_lower, "discard") or std.mem.eql(u8, name_lower, "watch") or
        std.mem.eql(u8, name_lower, "unwatch"))
    {
        has_transactions = true;
    } else if (std.mem.eql(u8, name_lower, "publish") or std.mem.eql(u8, name_lower, "subscribe") or
        std.mem.eql(u8, name_lower, "unsubscribe") or std.mem.eql(u8, name_lower, "psubscribe") or
        std.mem.eql(u8, name_lower, "punsubscribe") or std.mem.eql(u8, name_lower, "spublish") or
        std.mem.eql(u8, name_lower, "ssubscribe") or std.mem.eql(u8, name_lower, "sunsubscribe"))
    {
        has_pubsub = true;
    } else if (std.mem.eql(u8, name_lower, "select") or std.mem.eql(u8, name_lower, "ping") or
        std.mem.eql(u8, name_lower, "quit") or std.mem.eql(u8, name_lower, "auth") or
        std.mem.eql(u8, name_lower, "hello") or std.mem.eql(u8, name_lower, "reset"))
    {
        has_connection = true;
    } else if (std.mem.startsWith(u8, name_lower, "client") or std.mem.startsWith(u8, name_lower, "acl") or
        std.mem.startsWith(u8, name_lower, "config") or std.mem.startsWith(u8, name_lower, "info") or
        std.mem.startsWith(u8, name_lower, "debug") or std.mem.startsWith(u8, name_lower, "slowlog") or
        std.mem.startsWith(u8, name_lower, "command") or std.mem.startsWith(u8, name_lower, "latency") or
        std.mem.eql(u8, name_lower, "save") or std.mem.eql(u8, name_lower, "bgsave") or
        std.mem.eql(u8, name_lower, "bgrewriteaof") or std.mem.eql(u8, name_lower, "flushdb") or
        std.mem.eql(u8, name_lower, "flushall") or std.mem.eql(u8, name_lower, "dbsize") or
        std.mem.eql(u8, name_lower, "lastsave") or std.mem.eql(u8, name_lower, "replicaof") or
        std.mem.eql(u8, name_lower, "slaveof") or std.mem.eql(u8, name_lower, "role") or
        std.mem.eql(u8, name_lower, "shutdown") or std.mem.eql(u8, name_lower, "time") or
        std.mem.eql(u8, name_lower, "lolwut") or std.mem.eql(u8, name_lower, "failover") or
        std.mem.eql(u8, name_lower, "wait") or std.mem.eql(u8, name_lower, "waitaof") or
        std.mem.eql(u8, name_lower, "swapdb") or std.mem.eql(u8, name_lower, "monitor"))
    {
        has_server = true;
    } else if (std.mem.startsWith(u8, name_lower, "set") or std.mem.eql(u8, name_lower, "get") or
        std.mem.eql(u8, name_lower, "getset") or std.mem.eql(u8, name_lower, "getdel") or
        std.mem.eql(u8, name_lower, "getex") or std.mem.eql(u8, name_lower, "mset") or
        std.mem.eql(u8, name_lower, "mget") or std.mem.eql(u8, name_lower, "msetnx") or
        std.mem.eql(u8, name_lower, "strlen") or std.mem.startsWith(u8, name_lower, "incr") or
        std.mem.startsWith(u8, name_lower, "decr") or std.mem.eql(u8, name_lower, "append") or
        std.mem.eql(u8, name_lower, "getrange") or std.mem.eql(u8, name_lower, "substr") or
        std.mem.startsWith(u8, name_lower, "bit") or std.mem.eql(u8, name_lower, "lcs"))
    {
        has_string = true;
    } else {
        has_generic = true;
    }

    // Add access direction category
    if (has_read) try cats.append(allocator, "@read");
    if (has_write) try cats.append(allocator, "@write");
    if (has_admin) try cats.append(allocator, "@admin");
    if (has_dangerous) try cats.append(allocator, "@dangerous");

    // Add speed category
    if (has_fast) {
        try cats.append(allocator, "@fast");
    } else if (has_read or has_write) {
        try cats.append(allocator, "@slow");
    }

    // Add data type category
    if (has_string) try cats.append(allocator, "@string");
    if (has_list) try cats.append(allocator, "@list");
    if (has_set) try cats.append(allocator, "@set");
    if (has_sortedset) try cats.append(allocator, "@sortedset");
    if (has_hash) try cats.append(allocator, "@hash");
    if (has_hyperloglog) try cats.append(allocator, "@hyperloglog");
    if (has_geo) try cats.append(allocator, "@geo");
    if (has_stream) try cats.append(allocator, "@stream");
    if (has_bitmap and !has_string) try cats.append(allocator, "@bitmap");
    if (has_pubsub) try cats.append(allocator, "@pubsub");
    if (has_scripting) try cats.append(allocator, "@scripting");
    if (has_transactions) try cats.append(allocator, "@transactions");
    if (has_cluster) try cats.append(allocator, "@cluster");
    if (has_server) try cats.append(allocator, "@server");
    if (has_connection) try cats.append(allocator, "@connection");
    if (has_generic) try cats.append(allocator, "@keyspace");

    return cats.toOwnedSlice(allocator);
}

/// Write command info in Redis 7.0+ 10-element format
fn writeCommandInfo(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), cmd: CommandInfo) !void {
    // Redis 7.0+ format: 10 elements
    // 1. name, 2. arity, 3. flags, 4. first_key, 5. last_key, 6. step,
    // 7. acl_categories, 8. tips, 9. key_specifications, 10. subcommands
    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{10});
    try buf.appendSlice(allocator, "\r\n");

    // 1. Command name (bulk string)
    try writeBulkString(allocator, buf, cmd.name);

    // 2. Arity
    try buf.append(allocator, ':');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.arity});
    try buf.appendSlice(allocator, "\r\n");

    // 3. Flags (array of simple strings using + prefix as Redis does)
    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{cmd.flags.len});
    try buf.appendSlice(allocator, "\r\n");
    for (cmd.flags) |flag| {
        try buf.append(allocator, '+');
        try buf.appendSlice(allocator, flag);
        try buf.appendSlice(allocator, "\r\n");
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

    // 7. ACL categories (array of simple strings)
    const acl_cats = try deriveAclCategories(allocator, cmd);
    defer allocator.free(acl_cats);
    try buf.append(allocator, '*');
    try std.fmt.format(buf.writer(allocator), "{d}", .{acl_cats.len});
    try buf.appendSlice(allocator, "\r\n");
    for (acl_cats) |cat| {
        try buf.append(allocator, '+');
        try buf.appendSlice(allocator, cat);
        try buf.appendSlice(allocator, "\r\n");
    }

    // 8. Tips (empty array — not yet populated)
    try buf.appendSlice(allocator, "*0\r\n");

    // 9. Key specifications (empty array — not yet populated)
    try buf.appendSlice(allocator, "*0\r\n");

    // 10. Subcommands (empty array — not yet populated)
    try buf.appendSlice(allocator, "*0\r\n");
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

test "COMMAND INFO - returns 10-element format for Redis 7.0+" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"get"};
    const result = try cmdCommandInfo(allocator, &args);
    defer allocator.free(result);

    // Outer array: *1 (one command result)
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    // Inner: *10 (10 elements per command)
    try std.testing.expect(std.mem.indexOf(u8, result, "*10\r\n") != null);
    // Element 1: name as bulk string
    try std.testing.expect(std.mem.indexOf(u8, result, "$3\r\nget\r\n") != null);
    // Element 8-10: trailing empty arrays for tips/key_specs/subcommands
    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "COMMAND INFO - flags as simple strings with + prefix" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"get"};
    const result = try cmdCommandInfo(allocator, &args);
    defer allocator.free(result);

    // Flags should use simple string format (+readonly, +fast)
    try std.testing.expect(std.mem.indexOf(u8, result, "+readonly\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "+fast\r\n") != null);
}

test "COMMAND INFO - acl_categories populated" {
    const allocator = std.testing.allocator;

    // GET command should have @read and @string categories
    const get_args = [_][]const u8{"get"};
    const get_result = try cmdCommandInfo(allocator, &get_args);
    defer allocator.free(get_result);
    try std.testing.expect(std.mem.indexOf(u8, get_result, "@read") != null);
    try std.testing.expect(std.mem.indexOf(u8, get_result, "@string") != null);

    // SET command should have @write
    const set_args = [_][]const u8{"set"};
    const set_result = try cmdCommandInfo(allocator, &set_args);
    defer allocator.free(set_result);
    try std.testing.expect(std.mem.indexOf(u8, set_result, "@write") != null);
}

test "COMMAND INFO - write command has @write category" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"hset"};
    const result = try cmdCommandInfo(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "@write") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "@hash") != null);
}

test "COMMAND INFO - unknown command returns nil bulk string" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"nonexistent_xyz_command"};
    const result = try cmdCommandInfo(allocator, &args);
    defer allocator.free(result);

    // Should return *1\r\n$-1\r\n (nil bulk string for unknown command)
    try std.testing.expect(std.mem.indexOf(u8, result, "$-1\r\n") != null);
}

test "COMMAND INFO - list commands returns multiple 10-element entries" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{ "get", "set" };
    const result = try cmdCommandInfo(allocator, &args);
    defer allocator.free(result);

    // Outer array: *2 (two command results)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Both should have 10-element format
    var count: usize = 0;
    var remaining = result;
    while (std.mem.indexOf(u8, remaining, "*10\r\n")) |pos| {
        count += 1;
        remaining = remaining[pos + 5 ..];
    }
    try std.testing.expect(count == 2);
}

test "COMMAND - deriveAclCategories for server commands" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"flushdb"};
    const result = try cmdCommandInfo(allocator, &args);
    defer allocator.free(result);

    // FLUSHDB should have @write and @server categories
    try std.testing.expect(std.mem.indexOf(u8, result, "@write") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "@server") != null);
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

test "COMMAND DOCS - all docs have required fields" {
    for (COMMAND_DOCS) |doc| {
        try std.testing.expect(doc.name.len > 0);
        try std.testing.expect(doc.summary.len > 0);
        try std.testing.expect(doc.since.len > 0);
        try std.testing.expect(doc.group.len > 0);
        try std.testing.expect(doc.complexity.len > 0);
    }
}

test "COMMAND DOCS - specific command lookup" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"get"};
    const result = try cmdCommandDocs(allocator, &args);
    defer allocator.free(result);

    // Should contain "get" and "summary"
    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "summary") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "string") != null);
}

test "COMMAND DOCS - unknown command returns empty" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"nonexistent_command_xyz"};
    const result = try cmdCommandDocs(allocator, &args);
    defer allocator.free(result);

    // Should return empty array *0\r\n
    try std.testing.expect(std.mem.startsWith(u8, result, "*0\r\n"));
}

test "COMMAND DOCS - deprecated command has doc_flags" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"getset"};
    const result = try cmdCommandDocs(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "deprecated") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "replaced_by") != null);
}

test "COMMAND DOCS - no args returns all commands" {
    const allocator = std.testing.allocator;

    const result = try cmdCommandDocs(allocator, &.{});
    defer allocator.free(result);

    // Should be a large response covering many commands
    try std.testing.expect(result.len > 1000);
    try std.testing.expect(std.mem.indexOf(u8, result, "summary") != null);
}

test "COMMAND DOCS - multiple commands" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{ "set", "get", "hset" };
    const result = try cmdCommandDocs(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "set") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "hash") != null);
}

test "COMMAND GETKEYSANDFLAGS - readonly command returns read flag" {
    const allocator = std.testing.allocator;

    // GET key: readonly → key gets "read" flag
    const args = [_][]const u8{ "get", "mykey" };
    const result = try cmdCommandGetKeysAndFlags(allocator, &args);
    defer allocator.free(result);

    // Outer array has 1 entry
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    // Contains the key name
    try std.testing.expect(std.mem.indexOf(u8, result, "mykey") != null);
    // Contains "read" flag
    try std.testing.expect(std.mem.indexOf(u8, result, "read") != null);
    // Does NOT contain "write" flag for GET
    try std.testing.expect(std.mem.indexOf(u8, result, "write") == null);
}

test "COMMAND GETKEYSANDFLAGS - write command returns write flag" {
    const allocator = std.testing.allocator;

    // SET key value: write → key gets "write" flag
    const args = [_][]const u8{ "set", "mykey", "myvalue" };
    const result = try cmdCommandGetKeysAndFlags(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "mykey") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "write") != null);
    // Should not contain "read" flag for SET
    try std.testing.expect(std.mem.indexOf(u8, result, "read") == null);
}

test "COMMAND GETKEYSANDFLAGS - delete command returns write and delete flags" {
    const allocator = std.testing.allocator;

    // DEL key: write + delete flag
    const args = [_][]const u8{ "del", "mykey" };
    const result = try cmdCommandGetKeysAndFlags(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "mykey") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "write") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "delete") != null);
}

test "COMMAND GETKEYSANDFLAGS - multi-key command returns all keys with flags" {
    const allocator = std.testing.allocator;

    // MGET key1 key2 key3
    const args = [_][]const u8{ "mget", "k1", "k2", "k3" };
    const result = try cmdCommandGetKeysAndFlags(allocator, &args);
    defer allocator.free(result);

    // 3 keys
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "k1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "k2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "k3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "read") != null);
}

test "COMMAND GETKEYSANDFLAGS - no-key command returns empty array" {
    const allocator = std.testing.allocator;

    // PING has no keys
    const args = [_][]const u8{"ping"};
    const result = try cmdCommandGetKeysAndFlags(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*0\r\n"));
}

test "COMMAND GETKEYSANDFLAGS - unknown command returns error" {
    const allocator = std.testing.allocator;

    const args = [_][]const u8{"nonexistent_cmd_xyz"};
    const result = try cmdCommandGetKeysAndFlags(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "COMMAND GETKEYSANDFLAGS - no args returns error" {
    const allocator = std.testing.allocator;

    const result = try cmdCommandGetKeysAndFlags(allocator, &.{});
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "COMMAND HELP - includes GETKEYSANDFLAGS" {
    const allocator = std.testing.allocator;

    const result = try cmdCommandHelp(allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "GETKEYSANDFLAGS") != null);
}
