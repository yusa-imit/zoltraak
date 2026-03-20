const std = @import("std");

/// ACL command categories (21 core categories)
pub const CommandCategory = enum {
    // Core categories
    keyspace,
    read,
    write,
    denyoom,
    admin,
    pubsub,
    noscript,
    random,
    sort_by_pattern,
    loading,
    stale,
    skip_monitor,
    skip_slowlog,
    asking,
    fast,
    movablekeys,

    // Data type categories
    string,
    list,
    set,
    sorted_set,
    hash,
    bitmap,
    hyperloglog,
    geo,
    stream,

    // Execution context categories
    blocking,
    transaction,
    scripting,
    connection,
    server,
    cluster,
    replication,
    dangerous,
    slowlog,

    pub fn name(self: CommandCategory) []const u8 {
        return @tagName(self);
    }
};

/// Map a command name to its categories
pub const COMMAND_CATEGORIES = std.StaticStringMap([]const CommandCategory).initComptime(.{
    // String commands
    .{ "APPEND", &.{ .string, .write, .denyoom } },
    .{ "DECR", &.{ .string, .write, .denyoom, .fast } },
    .{ "DECRBY", &.{ .string, .write, .denyoom, .fast } },
    .{ "GET", &.{ .string, .read, .fast } },
    .{ "GETDEL", &.{ .string, .write, .fast } },
    .{ "GETEX", &.{ .string, .write, .fast } },
    .{ "GETRANGE", &.{ .string, .read } },
    .{ "GETSET", &.{ .string, .write, .denyoom, .fast } },
    .{ "INCR", &.{ .string, .write, .denyoom, .fast } },
    .{ "INCRBY", &.{ .string, .write, .denyoom, .fast } },
    .{ "INCRBYFLOAT", &.{ .string, .write, .denyoom, .fast } },
    .{ "MGET", &.{ .string, .read } },
    .{ "MSET", &.{ .string, .write, .denyoom } },
    .{ "MSETNX", &.{ .string, .write, .denyoom } },
    .{ "PSETEX", &.{ .string, .write, .denyoom } },
    .{ "SET", &.{ .string, .write, .denyoom } },
    .{ "SETEX", &.{ .string, .write, .denyoom } },
    .{ "SETNX", &.{ .string, .write, .denyoom, .fast } },
    .{ "SETRANGE", &.{ .string, .write, .denyoom } },
    .{ "STRLEN", &.{ .string, .read, .fast } },

    // Hash commands
    .{ "HDEL", &.{ .hash, .write, .fast } },
    .{ "HEXISTS", &.{ .hash, .read, .fast } },
    .{ "HGET", &.{ .hash, .read, .fast } },
    .{ "HGETALL", &.{ .hash, .read } },
    .{ "HINCRBY", &.{ .hash, .write, .denyoom, .fast } },
    .{ "HINCRBYFLOAT", &.{ .hash, .write, .denyoom, .fast } },
    .{ "HKEYS", &.{ .hash, .read, .sort_by_pattern } },
    .{ "HLEN", &.{ .hash, .read, .fast } },
    .{ "HMGET", &.{ .hash, .read } },
    .{ "HMSET", &.{ .hash, .write, .denyoom } },
    .{ "HSCAN", &.{ .hash, .read, .random } },
    .{ "HSET", &.{ .hash, .write, .denyoom, .fast } },
    .{ "HSETNX", &.{ .hash, .write, .denyoom, .fast } },
    .{ "HSTRLEN", &.{ .hash, .read, .fast } },
    .{ "HVALS", &.{ .hash, .read, .sort_by_pattern } },
    .{ "HRANDFIELD", &.{ .hash, .read, .random } },

    // List commands
    .{ "BLMOVE", &.{ .list, .write, .blocking } },
    .{ "BLMPOP", &.{ .list, .write, .blocking } },
    .{ "BLPOP", &.{ .list, .write, .blocking } },
    .{ "BRPOP", &.{ .list, .write, .blocking } },
    .{ "BRPOPLPUSH", &.{ .list, .write, .denyoom, .blocking } },
    .{ "LINDEX", &.{ .list, .read } },
    .{ "LINSERT", &.{ .list, .write, .denyoom } },
    .{ "LLEN", &.{ .list, .read, .fast } },
    .{ "LMOVE", &.{ .list, .write } },
    .{ "LMPOP", &.{ .list, .write } },
    .{ "LPOP", &.{ .list, .write, .fast } },
    .{ "LPOS", &.{ .list, .read } },
    .{ "LPUSH", &.{ .list, .write, .denyoom, .fast } },
    .{ "LPUSHX", &.{ .list, .write, .denyoom, .fast } },
    .{ "LRANGE", &.{ .list, .read } },
    .{ "LREM", &.{ .list, .write } },
    .{ "LSET", &.{ .list, .write, .denyoom } },
    .{ "LTRIM", &.{ .list, .write } },
    .{ "RPOP", &.{ .list, .write, .fast } },
    .{ "RPOPLPUSH", &.{ .list, .write, .denyoom } },
    .{ "RPUSH", &.{ .list, .write, .denyoom, .fast } },
    .{ "RPUSHX", &.{ .list, .write, .denyoom, .fast } },

    // Set commands
    .{ "SADD", &.{ .set, .write, .denyoom, .fast } },
    .{ "SCARD", &.{ .set, .read, .fast } },
    .{ "SDIFF", &.{ .set, .read, .sort_by_pattern } },
    .{ "SDIFFSTORE", &.{ .set, .write, .denyoom } },
    .{ "SINTER", &.{ .set, .read, .sort_by_pattern } },
    .{ "SINTERCARD", &.{ .set, .read } },
    .{ "SINTERSTORE", &.{ .set, .write, .denyoom } },
    .{ "SISMEMBER", &.{ .set, .read, .fast } },
    .{ "SMEMBERS", &.{ .set, .read, .sort_by_pattern } },
    .{ "SMISMEMBER", &.{ .set, .read, .fast } },
    .{ "SMOVE", &.{ .set, .write, .fast } },
    .{ "SPOP", &.{ .set, .write, .random, .fast } },
    .{ "SRANDMEMBER", &.{ .set, .read, .random } },
    .{ "SREM", &.{ .set, .write, .fast } },
    .{ "SSCAN", &.{ .set, .read, .random } },
    .{ "SUNION", &.{ .set, .read, .sort_by_pattern } },
    .{ "SUNIONSTORE", &.{ .set, .write, .denyoom } },

    // Sorted Set commands
    .{ "ZADD", &.{ .sorted_set, .write, .denyoom, .fast } },
    .{ "ZCARD", &.{ .sorted_set, .read, .fast } },
    .{ "ZCOUNT", &.{ .sorted_set, .read, .fast } },
    .{ "ZDIFF", &.{ .sorted_set, .read } },
    .{ "ZDIFFSTORE", &.{ .sorted_set, .write, .denyoom } },
    .{ "ZINCRBY", &.{ .sorted_set, .write, .denyoom, .fast } },
    .{ "ZINTER", &.{ .sorted_set, .read } },
    .{ "ZINTERCARD", &.{ .sorted_set, .read } },
    .{ "ZINTERSTORE", &.{ .sorted_set, .write, .denyoom } },
    .{ "ZLEXCOUNT", &.{ .sorted_set, .read, .fast } },
    .{ "ZMPOP", &.{ .sorted_set, .write } },
    .{ "ZMSCORE", &.{ .sorted_set, .read, .fast } },
    .{ "ZPOPMAX", &.{ .sorted_set, .write, .fast } },
    .{ "ZPOPMIN", &.{ .sorted_set, .write, .fast } },
    .{ "ZRANDMEMBER", &.{ .sorted_set, .read, .random } },
    .{ "ZRANGE", &.{ .sorted_set, .read } },
    .{ "ZRANGEBYLEX", &.{ .sorted_set, .read } },
    .{ "ZRANGEBYSCORE", &.{ .sorted_set, .read } },
    .{ "ZRANGESTORE", &.{ .sorted_set, .write, .denyoom } },
    .{ "ZRANK", &.{ .sorted_set, .read, .fast } },
    .{ "ZREM", &.{ .sorted_set, .write, .fast } },
    .{ "ZREMRANGEBYLEX", &.{ .sorted_set, .write } },
    .{ "ZREMRANGEBYRANK", &.{ .sorted_set, .write } },
    .{ "ZREMRANGEBYSCORE", &.{ .sorted_set, .write } },
    .{ "ZREVRANGE", &.{ .sorted_set, .read } },
    .{ "ZREVRANGEBYLEX", &.{ .sorted_set, .read } },
    .{ "ZREVRANGEBYSCORE", &.{ .sorted_set, .read } },
    .{ "ZREVRANK", &.{ .sorted_set, .read, .fast } },
    .{ "ZSCAN", &.{ .sorted_set, .read, .random } },
    .{ "ZSCORE", &.{ .sorted_set, .read, .fast } },
    .{ "ZUNION", &.{ .sorted_set, .read } },
    .{ "ZUNIONSTORE", &.{ .sorted_set, .write, .denyoom } },
    .{ "BZMPOP", &.{ .sorted_set, .write, .blocking } },
    .{ "BZPOPMAX", &.{ .sorted_set, .write, .blocking } },
    .{ "BZPOPMIN", &.{ .sorted_set, .write, .blocking } },

    // Key commands
    .{ "COPY", &.{ .keyspace, .write, .denyoom } },
    .{ "DEL", &.{ .keyspace, .write } },
    .{ "DUMP", &.{ .keyspace, .read } },
    .{ "EXISTS", &.{ .keyspace, .read, .fast } },
    .{ "EXPIRE", &.{ .keyspace, .write, .fast } },
    .{ "EXPIREAT", &.{ .keyspace, .write, .fast } },
    .{ "EXPIRETIME", &.{ .keyspace, .read, .fast } },
    .{ "KEYS", &.{ .keyspace, .read, .sort_by_pattern } },
    .{ "MIGRATE", &.{ .keyspace, .write, .movablekeys } },
    .{ "MOVE", &.{ .keyspace, .write, .fast } },
    .{ "OBJECT", &.{ .keyspace, .read } },
    .{ "PERSIST", &.{ .keyspace, .write, .fast } },
    .{ "PEXPIRE", &.{ .keyspace, .write, .fast } },
    .{ "PEXPIREAT", &.{ .keyspace, .write, .fast } },
    .{ "PEXPIRETIME", &.{ .keyspace, .read, .fast } },
    .{ "PTTL", &.{ .keyspace, .read, .fast } },
    .{ "RANDOMKEY", &.{ .keyspace, .read, .random } },
    .{ "RENAME", &.{ .keyspace, .write } },
    .{ "RENAMENX", &.{ .keyspace, .write, .fast } },
    .{ "RESTORE", &.{ .keyspace, .write, .denyoom } },
    .{ "SCAN", &.{ .keyspace, .read, .random } },
    .{ "SORT", &.{ .keyspace, .write, .denyoom, .movablekeys } },
    .{ "SORT_RO", &.{ .keyspace, .read, .movablekeys } },
    .{ "TOUCH", &.{ .keyspace, .read, .fast } },
    .{ "TTL", &.{ .keyspace, .read, .fast } },
    .{ "TYPE", &.{ .keyspace, .read, .fast } },
    .{ "UNLINK", &.{ .keyspace, .write, .fast } },
    .{ "WAIT", &.{ .keyspace, .noscript } },
    .{ "WAITAOF", &.{ .keyspace, .noscript } },

    // Bitmap commands
    .{ "BITCOUNT", &.{ .bitmap, .read } },
    .{ "BITFIELD", &.{ .bitmap, .write, .denyoom } },
    .{ "BITFIELD_RO", &.{ .bitmap, .read } },
    .{ "BITOP", &.{ .bitmap, .write, .denyoom } },
    .{ "BITPOS", &.{ .bitmap, .read } },
    .{ "GETBIT", &.{ .bitmap, .read, .fast } },
    .{ "SETBIT", &.{ .bitmap, .write, .denyoom } },

    // HyperLogLog commands
    .{ "PFADD", &.{ .hyperloglog, .write, .denyoom, .fast } },
    .{ "PFCOUNT", &.{ .hyperloglog, .read } },
    .{ "PFDEBUG", &.{ .hyperloglog, .write } },
    .{ "PFMERGE", &.{ .hyperloglog, .write, .denyoom } },
    .{ "PFSELFTEST", &.{ .hyperloglog, .read } },

    // Geo commands
    .{ "GEOADD", &.{ .geo, .write, .denyoom } },
    .{ "GEODIST", &.{ .geo, .read } },
    .{ "GEOHASH", &.{ .geo, .read } },
    .{ "GEOPOS", &.{ .geo, .read } },
    .{ "GEORADIUS", &.{ .geo, .write, .denyoom, .movablekeys } },
    .{ "GEORADIUS_RO", &.{ .geo, .read, .movablekeys } },
    .{ "GEORADIUSBYMEMBER", &.{ .geo, .write, .denyoom, .movablekeys } },
    .{ "GEORADIUSBYMEMBER_RO", &.{ .geo, .read, .movablekeys } },
    .{ "GEOSEARCH", &.{ .geo, .read } },
    .{ "GEOSEARCHSTORE", &.{ .geo, .write, .denyoom } },

    // Stream commands
    .{ "XACK", &.{ .stream, .write, .fast } },
    .{ "XADD", &.{ .stream, .write, .denyoom, .fast } },
    .{ "XAUTOCLAIM", &.{ .stream, .write, .fast } },
    .{ "XCLAIM", &.{ .stream, .write, .fast } },
    .{ "XDEL", &.{ .stream, .write, .fast } },
    .{ "XGROUP", &.{ .stream, .write } },
    .{ "XINFO", &.{ .stream, .read } },
    .{ "XLEN", &.{ .stream, .read, .fast } },
    .{ "XPENDING", &.{ .stream, .read } },
    .{ "XRANGE", &.{ .stream, .read } },
    .{ "XREAD", &.{ .stream, .read, .blocking, .movablekeys } },
    .{ "XREADGROUP", &.{ .stream, .write, .blocking, .movablekeys } },
    .{ "XREVRANGE", &.{ .stream, .read } },
    .{ "XSETID", &.{ .stream, .write, .fast } },
    .{ "XTRIM", &.{ .stream, .write } },

    // Pub/Sub commands
    .{ "PSUBSCRIBE", &.{ .pubsub, .noscript } },
    .{ "PUBLISH", &.{ .pubsub, .pubsub, .loading, .stale, .fast } },
    .{ "PUBSUB", &.{ .pubsub, .pubsub, .loading, .stale } },
    .{ "PUNSUBSCRIBE", &.{ .pubsub, .noscript } },
    .{ "SPUBLISH", &.{ .pubsub, .pubsub, .loading, .stale, .fast } },
    .{ "SSUBSCRIBE", &.{ .pubsub, .noscript } },
    .{ "SUBSCRIBE", &.{ .pubsub, .noscript } },
    .{ "SUNSUBSCRIBE", &.{ .pubsub, .noscript } },
    .{ "UNSUBSCRIBE", &.{ .pubsub, .noscript } },

    // Transaction commands
    .{ "DISCARD", &.{ .transaction, .noscript, .fast } },
    .{ "EXEC", &.{ .transaction, .noscript } },
    .{ "MULTI", &.{ .transaction, .noscript, .fast } },
    .{ "UNWATCH", &.{ .transaction, .noscript, .fast } },
    .{ "WATCH", &.{ .transaction, .noscript, .fast } },

    // Scripting commands
    .{ "EVAL", &.{ .scripting, .noscript, .movablekeys } },
    .{ "EVALSHA", &.{ .scripting, .noscript, .movablekeys } },
    .{ "EVALSHA_RO", &.{ .scripting, .noscript, .movablekeys } },
    .{ "EVAL_RO", &.{ .scripting, .noscript, .movablekeys } },
    .{ "SCRIPT", &.{ .scripting, .noscript } },

    // Connection commands
    .{ "AUTH", &.{ .connection, .noscript, .loading, .stale, .fast } },
    .{ "CLIENT", &.{ .connection, .admin, .noscript } },
    .{ "ECHO", &.{ .connection, .fast } },
    .{ "HELLO", &.{ .connection, .fast } },
    .{ "PING", &.{ .connection, .fast } },
    .{ "QUIT", &.{ .connection, .noscript, .fast } },
    .{ "RESET", &.{ .connection, .noscript, .fast } },
    .{ "SELECT", &.{ .connection, .loading, .stale, .fast } },

    // Server commands
    .{ "ACL", &.{ .admin, .noscript } },
    .{ "BGREWRITEAOF", &.{ .admin, .noscript } },
    .{ "BGSAVE", &.{ .admin, .noscript } },
    .{ "COMMAND", &.{ .connection, .loading, .stale } },
    .{ "CONFIG", &.{ .admin, .noscript } },
    .{ "DBSIZE", &.{ .keyspace, .read, .fast } },
    .{ "DEBUG", &.{ .admin, .noscript } },
    .{ "FAILOVER", &.{ .admin, .noscript } },
    .{ "FLUSHALL", &.{ .keyspace, .write } },
    .{ "FLUSHDB", &.{ .keyspace, .write } },
    .{ "INFO", &.{ .admin, .loading, .stale } },
    .{ "LASTSAVE", &.{ .admin, .random, .fast } },
    .{ "LATENCY", &.{ .admin, .noscript } },
    .{ "LOLWUT", &.{ .admin, .read, .fast } },
    .{ "MEMORY", &.{ .admin, .read } },
    .{ "MODULE", &.{ .admin, .noscript } },
    .{ "MONITOR", &.{ .admin, .noscript } },
    .{ "PSYNC", &.{ .replication, .dangerous, .noscript } },
    .{ "REPLCONF", &.{ .replication, .dangerous, .noscript } },
    .{ "REPLICAOF", &.{ .admin, .noscript, .dangerous } },
    .{ "ROLE", &.{ .admin, .noscript, .loading, .stale, .fast } },
    .{ "SAVE", &.{ .admin, .noscript } },
    .{ "SHUTDOWN", &.{ .admin, .noscript, .dangerous } },
    .{ "SLAVEOF", &.{ .admin, .noscript, .dangerous } },
    .{ "SLOWLOG", &.{ .admin, .slowlog, .loading, .stale } },
    .{ "SYNC", &.{ .replication, .dangerous, .noscript } },
    .{ "TIME", &.{ .admin, .loading, .stale, .fast } },

    // Cluster commands (stub)
    .{ "ASKING", &.{ .cluster, .fast } },
    .{ "CLUSTER", &.{ .admin, .stale } },
    .{ "READONLY", &.{ .cluster, .fast } },
    .{ "READWRITE", &.{ .cluster, .fast } },
});

/// Get all categories for a command (case-insensitive)
pub fn getCategoriesForCommand(command_name: []const u8) ![]const CommandCategory {
    var buf: [64]u8 = undefined;
    const cmd_upper = std.ascii.upperString(&buf, command_name);

    if (COMMAND_CATEGORIES.get(cmd_upper)) |categories| {
        return categories;
    }

    // Unknown command has no categories (safe-by-default)
    return &.{};
}

/// Get all commands in a specific category
pub fn getCommandsInCategory(allocator: std.mem.Allocator, category: CommandCategory) ![]const []const u8 {
    var commands = std.ArrayList([]const u8){};
    errdefer commands.deinit(allocator);

    var iter = COMMAND_CATEGORIES.iterator();
    while (iter.next()) |entry| {
        for (entry.value_ptr.*) |cat| {
            if (cat == category) {
                try commands.append(allocator, entry.key_ptr.*);
                break;
            }
        }
    }

    return commands.toOwnedSlice(allocator);
}

// Unit tests
test "getCategoriesForCommand returns categories for known command" {
    const categories = try getCategoriesForCommand("GET");
    try std.testing.expect(categories.len > 0);

    // GET should be in 'read' and 'string' categories
    var found_read = false;
    var found_string = false;
    for (categories) |cat| {
        if (cat == .read) found_read = true;
        if (cat == .string) found_string = true;
    }
    try std.testing.expect(found_read);
    try std.testing.expect(found_string);
}

test "getCategoriesForCommand is case-insensitive" {
    const cats_upper = try getCategoriesForCommand("SET");
    const cats_lower = try getCategoriesForCommand("set");
    const cats_mixed = try getCategoriesForCommand("Set");

    try std.testing.expectEqual(cats_upper.len, cats_lower.len);
    try std.testing.expectEqual(cats_upper.len, cats_mixed.len);
}

test "getCategoriesForCommand returns empty for unknown command" {
    const categories = try getCategoriesForCommand("UNKNOWNCOMMAND");
    try std.testing.expectEqual(@as(usize, 0), categories.len);
}

test "getCommandsInCategory returns commands in category" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const commands = try getCommandsInCategory(allocator, .fast);
    defer allocator.free(commands);

    try std.testing.expect(commands.len > 0);
    // PING should be in fast category
    var found_ping = false;
    for (commands) |cmd| {
        if (std.mem.eql(u8, cmd, "PING")) {
            found_ping = true;
            break;
        }
    }
    try std.testing.expect(found_ping);
}
