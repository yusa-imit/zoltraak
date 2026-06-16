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

    // Connection commands
    .{ .name = "hello", .arity = -1, .flags = &.{ "noscript", "loading", "stale", "fast", "no_auth", "allow_busy" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "auth", .arity = -2, .flags = &.{ "noscript", "loading", "stale", "fast", "no_auth", "allow_busy" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "echo", .arity = 2, .flags = &.{ "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "quit", .arity = -1, .flags = &.{ "loading", "stale", "fast", "allow_busy" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "select", .arity = 2, .flags = &.{ "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "time", .arity = 1, .flags = &.{ "random", "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "lolwut", .arity = -1, .flags = &.{"fast"}, .first_key = 0, .last_key = 0, .step = 0 },

    // Generic key commands
    .{ .name = "type", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ttl", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "pttl", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "persist", .arity = 2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "expire", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "expireat", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "pexpire", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "pexpireat", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "expiretime", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "pexpiretime", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "keys", .arity = 2, .flags = &.{ "readonly", "sort_for_script" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "rename", .arity = 3, .flags = &.{"write"}, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "renamenx", .arity = 3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "randomkey", .arity = 1, .flags = &.{ "readonly", "random" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "touch", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "dump", .arity = 2, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "restore", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "copy", .arity = -3, .flags = &.{"write"}, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "move", .arity = 3, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "swapdb", .arity = 3, .flags = &.{ "write", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "sort_ro", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lastsave", .arity = 1, .flags = &.{ "random", "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "substr", .arity = 4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "lcs", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 2, .step = 1 },

    // List commands (additional)
    .{ .name = "lmpop", .arity = -4, .flags = &.{"write"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "blmpop", .arity = -5, .flags = &.{ "write", "noscript" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "brpoplpush", .arity = 4, .flags = &.{ "write", "denyoom", "noscript" }, .first_key = 1, .last_key = 2, .step = 1 },

    // Hash commands (additional)
    .{ .name = "hmset", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hrandfield", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hgetdel", .arity = -4, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hgetex", .arity = -4, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hsetex", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hstrlen", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hexpire", .arity = -6, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hpexpire", .arity = -6, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hexpireat", .arity = -6, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hpexpireat", .arity = -6, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hpersist", .arity = -5, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "httl", .arity = -5, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hpttl", .arity = -5, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hexpiretime", .arity = -5, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hpexpiretime", .arity = -5, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Sorted set commands (additional)
    .{ .name = "zmpop", .arity = -4, .flags = &.{"write"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "bzmpop", .arity = -5, .flags = &.{ "write", "noscript" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "bzpopmin", .arity = -3, .flags = &.{ "write", "noscript" }, .first_key = 1, .last_key = -2, .step = 1 },
    .{ .name = "bzpopmax", .arity = -3, .flags = &.{ "write", "noscript" }, .first_key = 1, .last_key = -2, .step = 1 },
    .{ .name = "zdiff", .arity = -3, .flags = &.{"readonly"}, .first_key = 2, .last_key = 0, .step = 0 },
    .{ .name = "zdiffstore", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 0, .step = 0 },
    .{ .name = "zinter", .arity = -3, .flags = &.{"readonly"}, .first_key = 2, .last_key = 0, .step = 0 },
    .{ .name = "zintercard", .arity = -3, .flags = &.{"readonly"}, .first_key = 2, .last_key = 0, .step = 0 },
    .{ .name = "zinterstore", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 0, .step = 0 },
    .{ .name = "zunion", .arity = -3, .flags = &.{"readonly"}, .first_key = 2, .last_key = 0, .step = 0 },
    .{ .name = "zunionstore", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 0, .step = 0 },
    .{ .name = "zlexcount", .arity = 4, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrangebylex", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrevrangebylex", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zrangestore", .arity = -5, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "zremrangebyrank", .arity = 4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zremrangebyscore", .arity = 4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "zremrangebylex", .arity = 4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },

    // HyperLogLog commands
    .{ .name = "pfadd", .arity = -2, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "pfcount", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "pfmerge", .arity = -2, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -1, .step = 1 },

    // Geo commands
    .{ .name = "geoadd", .arity = -5, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "geodist", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "geohash", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "geopos", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "georadius", .arity = -6, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "georadiusbymember", .arity = -5, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "geosearch", .arity = -7, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "geosearchstore", .arity = -8, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "georadius_ro", .arity = -6, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "georadiusbymember_ro", .arity = -5, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },

    // Pub/Sub commands (additional)
    .{ .name = "psubscribe", .arity = -2, .flags = &.{ "pubsub", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "punsubscribe", .arity = -1, .flags = &.{ "pubsub", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ssubscribe", .arity = -2, .flags = &.{ "pubsub", "noscript", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "sunsubscribe", .arity = -1, .flags = &.{ "pubsub", "noscript", "fast" }, .first_key = 1, .last_key = -1, .step = 1 },
    .{ .name = "spublish", .arity = 3, .flags = &.{ "pubsub", "fast", "loading", "stale" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Stream commands
    .{ .name = "xadd", .arity = -5, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xdel", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xlen", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xrange", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xrevrange", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xtrim", .arity = -4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xread", .arity = -4, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "xreadgroup", .arity = -7, .flags = &.{"write"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "xack", .arity = -4, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xclaim", .arity = -6, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xautoclaim", .arity = -7, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xinfo", .arity = -2, .flags = &.{"readonly"}, .first_key = 2, .last_key = 2, .step = 1 },
    .{ .name = "xgroup", .arity = -2, .flags = &.{"write"}, .first_key = 2, .last_key = 2, .step = 1 },
    .{ .name = "xpending", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xsetid", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },

    // ACL/admin commands
    .{ .name = "acl", .arity = -2, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "debug", .arity = -2, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "slowlog", .arity = -2, .flags = &.{ "admin", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "latency", .arity = -2, .flags = &.{ "admin", "fast", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "memory", .arity = -2, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "monitor", .arity = 1, .flags = &.{ "admin", "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "role", .arity = 1, .flags = &.{ "noscript", "loading", "stale", "fast" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "failover", .arity = -1, .flags = &.{ "admin", "noscript" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "shutdown", .arity = -1, .flags = &.{ "admin", "noscript", "loading", "stale", "allow_busy" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "slaveof", .arity = 3, .flags = &.{ "admin", "noscript", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "cluster", .arity = -2, .flags = &.{"admin"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "sentinel", .arity = -2, .flags = &.{ "admin", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "module", .arity = -2, .flags = &.{"admin"}, .first_key = 0, .last_key = 0, .step = 0 },

    // Scripting commands
    .{ .name = "eval", .arity = -3, .flags = &.{"noscript"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "evalsha", .arity = -3, .flags = &.{"noscript"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "eval_ro", .arity = -3, .flags = &.{ "noscript", "readonly" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "evalsha_ro", .arity = -3, .flags = &.{ "noscript", "readonly" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "script", .arity = -2, .flags = &.{ "noscript", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "fcall", .arity = -3, .flags = &.{"noscript"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "fcall_ro", .arity = -3, .flags = &.{ "noscript", "readonly" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "function", .arity = -2, .flags = &.{"noscript"}, .first_key = 0, .last_key = 0, .step = 0 },

    // Cluster commands (additional)
    .{ .name = "asking", .arity = 1, .flags = &.{ "fast", "allow_busy" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "migrate", .arity = -6, .flags = &.{ "write", "admin" }, .first_key = 3, .last_key = 3, .step = 1 },
    .{ .name = "readonly", .arity = 1, .flags = &.{"fast"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "readwrite", .arity = 1, .flags = &.{"fast"}, .first_key = 0, .last_key = 0, .step = 0 },

    // Generic key commands (Redis 8.x extensions)
    .{ .name = "delex", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "digest", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "hotkeys", .arity = -2, .flags = &.{ "admin", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "msetex", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 2, .last_key = -1, .step = 2 },

    // Vector Set commands (Redis 8.0+)
    // VADD key [REDUCE dim] (VALUES num | FP32) values... element [opts] — min 6 tokens
    .{ .name = "vadd", .arity = -6, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vcard", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vdim", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vemb", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    // VGETATTR key member — Redis 8.0: 3 tokens total
    .{ .name = "vgetattr", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vinfo", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vismember", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vlinks", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vrandmember", .arity = -2, .flags = &.{ "readonly", "random" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vrange", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "vrem", .arity = -3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    // VSETATTR key member blob — 4 tokens total
    .{ .name = "vsetattr", .arity = 4, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    // VSIM key (ELE element | VALUES num vals...) [opts] — min 4 tokens
    .{ .name = "vsim", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },

    // Stream commands (Redis 8.x)
    .{ .name = "xackdel", .arity = -5, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xcfgset", .arity = -3, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "xdelex", .arity = -4, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Bloom Filter commands (RedisBloom module)
    .{ .name = "bf.reserve", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.add", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.madd", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.exists", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.mexists", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.insert", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.info", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.card", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.scandump", .arity = 3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "bf.loadchunk", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Cuckoo Filter commands (RedisBloom module)
    .{ .name = "cf.reserve", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.add", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.addnx", .arity = 3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.exists", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.mexists", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.del", .arity = 3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.count", .arity = 3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.insert", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.insertnx", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.info", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.scandump", .arity = 3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cf.loadchunk", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },

    // Count-Min Sketch commands (RedisBloom module)
    .{ .name = "cms.initbydim", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cms.initbyprob", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cms.incrby", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cms.query", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cms.merge", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "cms.info", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },

    // TopK commands (RedisBloom module)
    .{ .name = "topk.reserve", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "topk.add", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "topk.incrby", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "topk.query", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "topk.count", .arity = -3, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "topk.list", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "topk.info", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },

    // T-Digest commands (RedisBloom module)
    .{ .name = "tdigest.create", .arity = -2, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.add", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.reset", .arity = 2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.merge", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.quantile", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.cdf", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.min", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.max", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.rank", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.revrank", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.byrank", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.byrevrank", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.info", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "tdigest.trimmed_mean", .arity = 4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },

    // JSON commands (RedisJSON module)
    .{ .name = "json.set", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.get", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.del", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.forget", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.type", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.mget", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = -2, .step = 1 },
    .{ .name = "json.mset", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = -3, .step = 3 },
    .{ .name = "json.numincrby", .arity = 4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.nummultby", .arity = 4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.strappend", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.strlen", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.toggle", .arity = -2, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.clear", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.arrappend", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.arrindex", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.arrinsert", .arity = -5, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.arrlen", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.arrpop", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.arrtrim", .arity = 5, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.objkeys", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.objlen", .arity = -2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.resp", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.merge", .arity = 4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "json.debug", .arity = -2, .flags = &.{"readonly"}, .first_key = 2, .last_key = 2, .step = 1 },

    // Search commands (RediSearch module)
    .{ .name = "ft.create", .arity = -2, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft._list", .arity = 1, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.dropindex", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.info", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.alter", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.search", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.aggregate", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.explain", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.explaincli", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.profile", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.spellcheck", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.cursor", .arity = -3, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.aliasadd", .arity = 3, .flags = &.{ "write", "denyoom" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.aliasdel", .arity = 2, .flags = &.{"write"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.aliasupdate", .arity = 3, .flags = &.{ "write", "denyoom" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.dictadd", .arity = -3, .flags = &.{ "write", "denyoom" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.dictdel", .arity = -3, .flags = &.{"write"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.dictdump", .arity = 2, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.syndump", .arity = 2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.synupdate", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.sugadd", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.sugget", .arity = -3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.suglen", .arity = 2, .flags = &.{ "readonly", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.sugdel", .arity = 3, .flags = &.{ "write", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.tagvals", .arity = 3, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ft.config", .arity = -2, .flags = &.{ "admin", "loading", "stale" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ft.hybrid", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },

    // Time Series commands (RedisTimeSeries module)
    .{ .name = "ts.create", .arity = -2, .flags = &.{ "write", "denyoom" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.alter", .arity = -2, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.add", .arity = -4, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.madd", .arity = -4, .flags = &.{ "write", "denyoom" }, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ts.incrby", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.decrby", .arity = -3, .flags = &.{ "write", "denyoom", "fast" }, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.del", .arity = 4, .flags = &.{"write"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.get", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.info", .arity = -2, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.mget", .arity = -2, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ts.range", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.revrange", .arity = -4, .flags = &.{"readonly"}, .first_key = 1, .last_key = 1, .step = 1 },
    .{ .name = "ts.mrange", .arity = -4, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ts.mrevrange", .arity = -4, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ts.queryindex", .arity = -2, .flags = &.{"readonly"}, .first_key = 0, .last_key = 0, .step = 0 },
    .{ .name = "ts.createrule", .arity = -4, .flags = &.{"write"}, .first_key = 1, .last_key = 2, .step = 1 },
    .{ .name = "ts.deleterule", .arity = 3, .flags = &.{"write"}, .first_key = 1, .last_key = 2, .step = 1 },
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

/// Returns true if the given command group matches the requested ACL category.
/// Handles Redis-canonical ACL category names (with or without leading @).
fn groupMatchesAclCat(group: []const u8, cat: []const u8) bool {
    // Strip leading @ from category if present
    const cat_name = if (cat.len > 0 and cat[0] == '@') cat[1..] else cat;

    // @all matches every command
    if (std.ascii.eqlIgnoreCase(cat_name, "all")) return true;

    // Direct group → category mapping
    if (std.ascii.eqlIgnoreCase(cat_name, "string")) return std.mem.eql(u8, group, "string");
    if (std.ascii.eqlIgnoreCase(cat_name, "list")) return std.mem.eql(u8, group, "list");
    if (std.ascii.eqlIgnoreCase(cat_name, "set")) return std.mem.eql(u8, group, "set");
    if (std.ascii.eqlIgnoreCase(cat_name, "sortedset") or std.ascii.eqlIgnoreCase(cat_name, "sorted_set")) return std.mem.eql(u8, group, "sorted_set");
    if (std.ascii.eqlIgnoreCase(cat_name, "hash")) return std.mem.eql(u8, group, "hash");
    if (std.ascii.eqlIgnoreCase(cat_name, "bitmap")) return std.mem.eql(u8, group, "bitmap");
    if (std.ascii.eqlIgnoreCase(cat_name, "hyperloglog")) return std.mem.eql(u8, group, "hyperloglog");
    if (std.ascii.eqlIgnoreCase(cat_name, "geo")) return std.mem.eql(u8, group, "geo");
    if (std.ascii.eqlIgnoreCase(cat_name, "stream")) return std.mem.eql(u8, group, "stream");
    if (std.ascii.eqlIgnoreCase(cat_name, "pubsub")) return std.mem.eql(u8, group, "pubsub");
    if (std.ascii.eqlIgnoreCase(cat_name, "transaction") or std.ascii.eqlIgnoreCase(cat_name, "transactions")) return std.mem.eql(u8, group, "transactions");
    if (std.ascii.eqlIgnoreCase(cat_name, "scripting")) return std.mem.eql(u8, group, "scripting");
    if (std.ascii.eqlIgnoreCase(cat_name, "connection")) return std.mem.eql(u8, group, "connection");
    if (std.ascii.eqlIgnoreCase(cat_name, "keyspace") or std.ascii.eqlIgnoreCase(cat_name, "generic")) return std.mem.eql(u8, group, "generic");
    // @server and @admin cover server-management and cluster commands
    if (std.ascii.eqlIgnoreCase(cat_name, "server") or std.ascii.eqlIgnoreCase(cat_name, "admin") or std.ascii.eqlIgnoreCase(cat_name, "dangerous")) {
        return std.mem.eql(u8, group, "server") or std.mem.eql(u8, group, "cluster");
    }
    if (std.ascii.eqlIgnoreCase(cat_name, "cluster")) return std.mem.eql(u8, group, "cluster");

    return false;
}

/// Return the group string for a command by looking it up in COMMAND_DOCS.
/// Returns null if not found.
fn getCommandGroup(name: []const u8) ?[]const u8 {
    for (COMMAND_DOCS) |doc| {
        if (std.ascii.eqlIgnoreCase(doc.name, name)) return doc.group;
    }
    return null;
}

/// COMMAND LIST [FILTERBY (ACLCAT category | PATTERN pattern | MODULE module)] (Redis 7.0+)
/// filter_type: "ACLCAT", "PATTERN", or "MODULE" (case-insensitive, null = no filter)
/// filter_value: the category name, glob pattern, or module name
pub fn cmdCommandList(allocator: std.mem.Allocator, filter_type: ?[]const u8, filter_value: ?[]const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const ft = if (filter_type) |t| t else null;

    // MODULE filter: built-in commands don't belong to any module → return empty list
    if (ft != null and std.ascii.eqlIgnoreCase(ft.?, "MODULE")) {
        try buf.appendSlice(allocator, "*0\r\n");
        return buf.toOwnedSlice(allocator);
    }

    // Collect matching command names
    var matching = std.ArrayList([]const u8){};
    defer matching.deinit(allocator);

    for (ALL_COMMANDS) |cmd| {
        if (ft != null) {
            if (std.ascii.eqlIgnoreCase(ft.?, "PATTERN")) {
                const pat = filter_value orelse "*";
                if (!matchesPattern(cmd.name, pat)) continue;
            } else if (std.ascii.eqlIgnoreCase(ft.?, "ACLCAT")) {
                const cat = filter_value orelse "";
                const group = getCommandGroup(cmd.name) orelse "";
                // Also check @read/@write/@fast/@slow/@all derived from flags
                const is_readonly = blk: {
                    for (cmd.flags) |f| {
                        if (std.mem.eql(u8, f, "readonly")) break :blk true;
                    }
                    break :blk false;
                };
                const cat_name = if (cat.len > 0 and cat[0] == '@') cat[1..] else cat;
                const matches_group = groupMatchesAclCat(group, cat);
                const matches_readwrite = (std.ascii.eqlIgnoreCase(cat_name, "read") and is_readonly) or
                    (std.ascii.eqlIgnoreCase(cat_name, "write") and !is_readonly and group.len > 0) or
                    std.ascii.eqlIgnoreCase(cat_name, "all") or
                    std.ascii.eqlIgnoreCase(cat_name, "fast") or
                    std.ascii.eqlIgnoreCase(cat_name, "slow");
                if (!matches_group and !matches_readwrite) continue;
            }
        }
        try matching.append(allocator, cmd.name);
    }

    // Write RESP array of command names
    try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{matching.items.len});
    for (matching.items) |name| {
        try writeBulkString(allocator, &buf, name);
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
    .{ .name = "hstrlen", .summary = "Get the length of the value of a hash field", .since = "3.2.0", .group = "hash", .complexity = "O(1)" },
    .{ .name = "hexpire", .summary = "Set expiry for hash field using relative time to expire (seconds)", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hpexpire", .summary = "Set expiry for hash field using relative time to expire (milliseconds)", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hexpireat", .summary = "Set expiry for hash field using an absolute Unix timestamp (seconds)", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hpexpireat", .summary = "Set expiry for hash field using an absolute Unix timestamp (milliseconds)", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hpersist", .summary = "Remove the expiration of a hash field", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "httl", .summary = "Get the TTL for hash fields in seconds", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hpttl", .summary = "Get the TTL for hash fields in milliseconds", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hexpiretime", .summary = "Get the expiration Unix timestamp for hash fields in seconds", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
    .{ .name = "hpexpiretime", .summary = "Get the expiration Unix timestamp for hash fields in milliseconds", .since = "7.4.0", .group = "hash", .complexity = "O(N) where N is the number of specified fields" },
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
    .{ .name = "zremrangebyrank", .summary = "Remove all members in a sorted set within the given indexes", .since = "2.0.0", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number removed" },
    .{ .name = "zremrangebyscore", .summary = "Remove all members in a sorted set within the given scores", .since = "1.2.0", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number removed" },
    .{ .name = "zremrangebylex", .summary = "Remove all members in a sorted set between the given lexicographical range", .since = "2.8.9", .group = "sorted_set", .complexity = "O(log(N)+M) where N is the number of elements and M is the number removed" },
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
    .{ .name = "georadius_ro", .summary = "A read-only variant for GEORADIUS", .since = "3.2.10", .group = "geo", .complexity = "O(N+log(M)) where M is the number of elements inside the bounding box and N is the number of elements", .doc_flags = "deprecated", .replaced_by = "geosearch" },
    .{ .name = "georadiusbymember_ro", .summary = "A read-only variant for GEORADIUSBYMEMBER", .since = "3.2.10", .group = "geo", .complexity = "O(N+log(M)) where M is the number of elements inside the bounding box and N is the number of elements", .doc_flags = "deprecated", .replaced_by = "geosearch" },
    // Cluster commands (additional)
    .{ .name = "asking", .summary = "Sent by cluster clients after an ASK redirect", .since = "3.0.0", .group = "cluster", .complexity = "O(1)" },
    .{ .name = "migrate", .summary = "Atomically transfer a key from a Redis instance to another one", .since = "2.6.0", .group = "generic", .complexity = "O(N) serialization complexity per key transferred" },
    .{ .name = "readonly", .summary = "Enables read queries for a connection to a cluster replica node", .since = "3.0.0", .group = "cluster", .complexity = "O(1)" },
    .{ .name = "readwrite", .summary = "Resets the connection cluster node to default read/write mode", .since = "3.0.0", .group = "cluster", .complexity = "O(1)" },
    // Generic key commands (Redis 8.x)
    .{ .name = "delex", .summary = "Conditionally delete a key based on value or digest comparison", .since = "8.4.0", .group = "generic", .complexity = "O(1) for IFEQ/IFNE, O(N) for IFDEQ/IFDNE" },
    .{ .name = "digest", .summary = "Get the hash digest for the value stored in a key", .since = "8.4.0", .group = "generic", .complexity = "O(N) where N is the size of the value" },
    .{ .name = "hotkeys", .summary = "Track and retrieve the hottest keys accessed by the server", .since = "8.0.0", .group = "server", .complexity = "O(N) for GET where N is the number of hot keys tracked" },
    .{ .name = "msetex", .summary = "Set multiple keys with a shared expiration time", .since = "8.0.0", .group = "string", .complexity = "O(N) where N is the number of keys to set" },
    // Vector Set commands (Redis 8.0+)
    .{ .name = "vadd", .summary = "Add a vector element to a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(log(N))" },
    .{ .name = "vcard", .summary = "Return the number of elements in a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vdim", .summary = "Return the dimension of vectors in a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vemb", .summary = "Return the vector embedding for a given element", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vgetattr", .summary = "Return an attribute of a vector set element", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vinfo", .summary = "Return information about a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vismember", .summary = "Determine if an element is a member of a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vlinks", .summary = "Return the HNSW graph neighbors for a given element", .since = "8.0.0", .group = "vector_set", .complexity = "O(M) where M is the number of neighbors" },
    .{ .name = "vrandmember", .summary = "Return one or more random elements from a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(N) where N is the number of elements returned" },
    .{ .name = "vrange", .summary = "Return elements from a vector set sorted by similarity", .since = "8.0.0", .group = "vector_set", .complexity = "O(N*log(N))" },
    .{ .name = "vrem", .summary = "Remove one or more elements from a vector set", .since = "8.0.0", .group = "vector_set", .complexity = "O(M*log(N)) where M is the number of elements removed" },
    .{ .name = "vsetattr", .summary = "Set the attribute blob for a vector set element", .since = "8.0.0", .group = "vector_set", .complexity = "O(1)" },
    .{ .name = "vsim", .summary = "Return elements similar to a given vector or element", .since = "8.0.0", .group = "vector_set", .complexity = "O(log(N))" },
    // Stream commands (Redis 8.x)
    .{ .name = "xackdel", .summary = "Acknowledge and delete entries from a stream consumer group", .since = "8.2.0", .group = "stream", .complexity = "O(M) where M is the number of IDs being acknowledged and deleted" },
    .{ .name = "xcfgset", .summary = "Set configuration parameters for a stream", .since = "8.6.0", .group = "stream", .complexity = "O(1)" },
    .{ .name = "xdelex", .summary = "Delete stream entries with consumer group reference handling", .since = "8.2.0", .group = "stream", .complexity = "O(M) where M is the number of IDs deleted" },
    // Bloom Filter commands (RedisBloom module)
    .{ .name = "bf.reserve", .summary = "Creates a Bloom Filter with the given parameters", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(1)" },
    .{ .name = "bf.add", .summary = "Adds an item to a Bloom Filter", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "bf.madd", .summary = "Adds one or more items to a Bloom Filter and creates it if it does not exist", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(k*n) where k is the number of hash functions and n is the number of items" },
    .{ .name = "bf.exists", .summary = "Determines whether a given item was added to a Bloom Filter", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "bf.mexists", .summary = "Determines whether one or more items were added to a Bloom Filter", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(k*n) where k is the number of hash functions and n is the number of items" },
    .{ .name = "bf.insert", .summary = "Adds one or more items to a Bloom Filter", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(k*n) where k is the number of hash functions and n is the number of items" },
    .{ .name = "bf.info", .summary = "Returns information about a Bloom Filter", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(1)" },
    .{ .name = "bf.card", .summary = "Returns the cardinality of a Bloom filter", .since = "2.7.7", .group = "bloom_filter", .complexity = "O(1)" },
    .{ .name = "bf.scandump", .summary = "Begins an incremental save of the Bloom filter", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(n) where n is the capacity" },
    .{ .name = "bf.loadchunk", .summary = "Restores a filter previously saved using SCANDUMP", .since = "1.0.0", .group = "bloom_filter", .complexity = "O(n) where n is the capacity" },
    // Cuckoo Filter commands (RedisBloom module)
    .{ .name = "cf.reserve", .summary = "Creates an empty Cuckoo Filter with the specified capacity", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(1)" },
    .{ .name = "cf.add", .summary = "Adds an item to a Cuckoo Filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "cf.addnx", .summary = "Adds an item to a Cuckoo Filter only if it does not exist", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "cf.exists", .summary = "Checks if an item was added to a Cuckoo Filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "cf.mexists", .summary = "Checks if one or more items were added to a Cuckoo Filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(k*n) where k is the number of hash functions and n is the number of items" },
    .{ .name = "cf.del", .summary = "Deletes an item from a Cuckoo Filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "cf.count", .summary = "Returns the number of times an item may be in the filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(k) where k is the number of hash functions" },
    .{ .name = "cf.insert", .summary = "Adds one or more items to a Cuckoo Filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(n*k) where n is the number of items and k is the number of hash functions" },
    .{ .name = "cf.insertnx", .summary = "Adds one or more items to a Cuckoo Filter only if they do not exist", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(n*k) where n is the number of items and k is the number of hash functions" },
    .{ .name = "cf.info", .summary = "Returns information about a Cuckoo Filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(1)" },
    .{ .name = "cf.scandump", .summary = "Begins an incremental save of the Cuckoo filter", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(n) where n is the filter size" },
    .{ .name = "cf.loadchunk", .summary = "Restores a filter previously saved using SCANDUMP", .since = "1.0.0", .group = "cuckoo_filter", .complexity = "O(n) where n is the filter size" },
    // Count-Min Sketch commands (RedisBloom module)
    .{ .name = "cms.initbydim", .summary = "Initializes a Count-Min Sketch to dimensions specified by user", .since = "2.0.0", .group = "cms", .complexity = "O(1)" },
    .{ .name = "cms.initbyprob", .summary = "Initializes a Count-Min Sketch to accommodate requested tolerances", .since = "2.0.0", .group = "cms", .complexity = "O(1)" },
    .{ .name = "cms.incrby", .summary = "Increases the count of one or more items by increment", .since = "2.0.0", .group = "cms", .complexity = "O(n*k) where n is the number of items and k is the number of hash functions" },
    .{ .name = "cms.query", .summary = "Returns the count for one or more items in a sketch", .since = "2.0.0", .group = "cms", .complexity = "O(n*k) where n is the number of items and k is the number of hash functions" },
    .{ .name = "cms.merge", .summary = "Merges several sketches into one sketch", .since = "2.0.0", .group = "cms", .complexity = "O(n*k) where n is the number of sketches and k is the number of hash functions" },
    .{ .name = "cms.info", .summary = "Returns width, depth and total count of the sketch", .since = "2.0.0", .group = "cms", .complexity = "O(1)" },
    // TopK commands (RedisBloom module)
    .{ .name = "topk.reserve", .summary = "Initializes a TopK with the specified number of top occurring items to keep", .since = "2.0.0", .group = "topk", .complexity = "O(k) where k is the number of top occurring items to keep" },
    .{ .name = "topk.add", .summary = "Adds one or more items to a TopK Filter", .since = "2.0.0", .group = "topk", .complexity = "O(n*k) where n is the number of items and k is the number of hash functions" },
    .{ .name = "topk.incrby", .summary = "Increases the count of one or more items by increment", .since = "2.0.0", .group = "topk", .complexity = "O(n*k) where n is the number of items and k is the number of hash functions" },
    .{ .name = "topk.query", .summary = "Checks whether an item is one of TopK items", .since = "2.0.0", .group = "topk", .complexity = "O(k) where k is the number of top occurring items" },
    .{ .name = "topk.count", .summary = "Returns count for one or more items in a TopK sketch", .since = "2.0.0", .group = "topk", .complexity = "O(n) where n is the number of items" },
    .{ .name = "topk.list", .summary = "Return full list of items in Top K list", .since = "2.0.0", .group = "topk", .complexity = "O(k) where k is the number of top occurring items" },
    .{ .name = "topk.info", .summary = "Returns number of required items (k), width, depth and decay values", .since = "2.0.0", .group = "topk", .complexity = "O(1)" },
    // T-Digest commands (RedisBloom module)
    .{ .name = "tdigest.create", .summary = "Allocate the memory and initialize the t-digest", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.add", .summary = "Adds one or more samples to a t-digest sketch", .since = "2.4.0", .group = "tdigest", .complexity = "O(N) where N is the number of samples to add" },
    .{ .name = "tdigest.reset", .summary = "Reset the sketch to zero - as if it was just created", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.merge", .summary = "Merges multiple t-digest sketches into one", .since = "2.4.0", .group = "tdigest", .complexity = "O(N) where N is the number of samples across all sketches" },
    .{ .name = "tdigest.quantile", .summary = "Returns, for each input fraction, an estimation of the value with that fraction of all observations equal or less than it", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.cdf", .summary = "Returns the fraction of all observations which are less than or equal to the passed value", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.min", .summary = "Returns the minimum observation value from the sketch", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.max", .summary = "Returns the maximum observation value from the sketch", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.rank", .summary = "Retrieve the estimated rank of value (the number of observations in the sketch that are smaller than value + half the number of observations that are equal to value)", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.revrank", .summary = "Retrieve the estimated rank of value from the right side (the number of observations in the sketch that are larger than value + half the number of observations that are equal to value)", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.byrank", .summary = "Retrieve an estimation of the value with the given rank", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.byrevrank", .summary = "Retrieve an estimation of the value with the given reverse rank", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.info", .summary = "Returns information and statistics about a t-digest sketch", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    .{ .name = "tdigest.trimmed_mean", .summary = "Returns an estimation of the mean value from the sketch, excluding observation values outside the low and high cutoff quantiles", .since = "2.4.0", .group = "tdigest", .complexity = "O(1)" },
    // JSON commands (RedisJSON module)
    .{ .name = "json.set", .summary = "Sets the JSON value at path in each key", .since = "1.0.0", .group = "json", .complexity = "O(M+N) where M is the original size and N is the new size" },
    .{ .name = "json.get", .summary = "Return the value at path in JSON serialized form", .since = "1.0.0", .group = "json", .complexity = "O(N) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.del", .summary = "Delete a value", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the deleted value" },
    .{ .name = "json.forget", .summary = "Delete a value (alias for JSON.DEL)", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the deleted value", .doc_flags = "deprecated", .replaced_by = "json.del" },
    .{ .name = "json.type", .summary = "Report the type of JSON value at path", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.mget", .summary = "Returns the values at path from multiple key arguments", .since = "1.0.0", .group = "json", .complexity = "O(M*N) where M is the number of keys and N is the size of the value" },
    .{ .name = "json.mset", .summary = "Sets or updates the JSON value of one or more keys", .since = "2.6.0", .group = "json", .complexity = "O(M+N) where M is the original size and N is the new size" },
    .{ .name = "json.numincrby", .summary = "Increment the number value stored at path by a value", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.nummultby", .summary = "Multiply the number value stored at path by a value", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values", .doc_flags = "deprecated", .replaced_by = "json.numincrby" },
    .{ .name = "json.strappend", .summary = "Append a string to the string values at path", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.strlen", .summary = "Report the length of the JSON String at path in each path", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.toggle", .summary = "Toggle a Boolean value stored at path", .since = "2.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.clear", .summary = "Clear container values (arrays/objects) and set numeric values to 0", .since = "2.0.0", .group = "json", .complexity = "O(N) where N is the number of values in the key" },
    .{ .name = "json.arrappend", .summary = "Append the json values into the array at path after the last element in it", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.arrindex", .summary = "Search for the first occurrence of a JSON scalar value in an array", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the array" },
    .{ .name = "json.arrinsert", .summary = "Insert the json-values into the array at path before the index", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the array" },
    .{ .name = "json.arrlen", .summary = "Report the length of the JSON Array at path in each path", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.arrpop", .summary = "Remove and return element from the index in the array", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the array" },
    .{ .name = "json.arrtrim", .summary = "Trim an array so that it contains only the specified inclusive range of elements", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the array" },
    .{ .name = "json.objkeys", .summary = "Return the keys in the object that's referenced by path", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.objlen", .summary = "Report the number of keys in the JSON Object at path in each path", .since = "1.0.0", .group = "json", .complexity = "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values" },
    .{ .name = "json.resp", .summary = "Return the JSON in path in Redis Serialization Protocol (RESP)", .since = "1.0.0", .group = "json", .complexity = "O(N) where N is the size of the JSON value" },
    .{ .name = "json.merge", .summary = "Merge a given JSON value into matching paths", .since = "2.6.0", .group = "json", .complexity = "O(M+N) where M is the size of the original value and N is the size of the value to merge" },
    .{ .name = "json.debug", .summary = "A container for JSON DEBUG commands", .since = "1.0.0", .group = "json", .complexity = "O(1)" },
    // Search commands (RediSearch module)
    .{ .name = "ft.create", .summary = "Create an index with the given spec", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft._list", .summary = "Return a list of all existing indexes", .since = "2.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.dropindex", .summary = "Delete an index", .since = "2.0.0", .group = "search", .complexity = "O(1) or O(N) if DD option is used" },
    .{ .name = "ft.info", .summary = "Return information and statistics on the index", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.alter", .summary = "Add a new field to the index", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.search", .summary = "Search the index with a textual query, returning either documents or just ids", .since = "1.0.0", .group = "search", .complexity = "O(N)" },
    .{ .name = "ft.aggregate", .summary = "Run a search query on an index, and perform aggregate transformations on the results", .since = "1.1.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.explain", .summary = "Return the execution plan for a complex query", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.explaincli", .summary = "Return the execution plan for a complex query but formatted for display", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.profile", .summary = "Apply FT.SEARCH or FT.AGGREGATE command to collect performance details", .since = "2.2.0", .group = "search", .complexity = "O(N)" },
    .{ .name = "ft.spellcheck", .summary = "Perform spelling correction on a query", .since = "1.4.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.cursor", .summary = "A container for cursor management commands", .since = "1.1.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.aliasadd", .summary = "Add an alias to an index", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.aliasdel", .summary = "Remove an alias from an index", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.aliasupdate", .summary = "Add or update an alias to an index", .since = "2.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.dictadd", .summary = "Add terms to a dictionary", .since = "1.4.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.dictdel", .summary = "Delete terms from a dictionary", .since = "1.4.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.dictdump", .summary = "Dump all terms in the given dictionary", .since = "1.4.0", .group = "search", .complexity = "O(N) where N is the size of the dictionary" },
    .{ .name = "ft.syndump", .summary = "Dump the contents of a synonym group", .since = "1.2.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.synupdate", .summary = "Update a synonym group", .since = "1.2.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.sugadd", .summary = "Add a suggestion string to an auto-complete suggestion dictionary", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.sugget", .summary = "Get completion suggestions for a prefix", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.suglen", .summary = "Get the size of an auto-complete suggestion dictionary", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.sugdel", .summary = "Delete a string from a suggestion index", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.tagvals", .summary = "Return a list of all distinct values indexed in a Tag field", .since = "1.0.0", .group = "search", .complexity = "O(N)" },
    .{ .name = "ft.config", .summary = "A container for FT configuration commands", .since = "1.0.0", .group = "search", .complexity = "O(1)" },
    .{ .name = "ft.hybrid", .summary = "Search an index with both vector and text/tag/numeric criteria", .since = "2.4.0", .group = "search", .complexity = "O(N)" },
    // Time Series commands (RedisTimeSeries module)
    .{ .name = "ts.create", .summary = "Create a new time series", .since = "1.0.0", .group = "timeseries", .complexity = "O(1)" },
    .{ .name = "ts.alter", .summary = "Update the retention, chunk size, duplicate policy, and labels of an existing time series", .since = "1.0.0", .group = "timeseries", .complexity = "O(N) where N is the number of labels" },
    .{ .name = "ts.add", .summary = "Append a sample to a time series", .since = "1.0.0", .group = "timeseries", .complexity = "O(M) where M is the number of compaction rules or O(R) where R is the number of downsampled time series" },
    .{ .name = "ts.madd", .summary = "Append new samples to one or more time series", .since = "1.0.0", .group = "timeseries", .complexity = "O(N*M) where N is the number of samples and M is the number of compaction rules" },
    .{ .name = "ts.incrby", .summary = "Increase the value of the sample with the maximum existing timestamp, or create a new sample with a value equal to the value of the sample with the maximum existing timestamp with a given increment", .since = "1.0.0", .group = "timeseries", .complexity = "O(M) where M is the number of compaction rules" },
    .{ .name = "ts.decrby", .summary = "Decrease the value of the sample with the maximum existing timestamp, or create a new sample with a value equal to the value of the sample with the maximum existing timestamp with a given decrement", .since = "1.0.0", .group = "timeseries", .complexity = "O(M) where M is the number of compaction rules" },
    .{ .name = "ts.del", .summary = "Delete all samples between two timestamps for a given time series", .since = "1.6.0", .group = "timeseries", .complexity = "O(N) where N is the number of data points that lie within the range" },
    .{ .name = "ts.get", .summary = "Get the sample with the highest timestamp from a given time series", .since = "1.0.0", .group = "timeseries", .complexity = "O(1)" },
    .{ .name = "ts.info", .summary = "Return information and statistics on the time series", .since = "1.0.0", .group = "timeseries", .complexity = "O(1)" },
    .{ .name = "ts.mget", .summary = "Return the last samples matching the specific filter", .since = "1.0.0", .group = "timeseries", .complexity = "O(N) where N is the number of time-series that match the filters" },
    .{ .name = "ts.range", .summary = "Query a range in forward direction", .since = "1.0.0", .group = "timeseries", .complexity = "O(T/C) where T is the number of data points and C is the chunk size" },
    .{ .name = "ts.revrange", .summary = "Query a range in reverse direction", .since = "1.1.0", .group = "timeseries", .complexity = "O(T/C) where T is the number of data points and C is the chunk size" },
    .{ .name = "ts.mrange", .summary = "Query a range across multiple time series by filters in forward direction", .since = "1.0.0", .group = "timeseries", .complexity = "O(N) where N is the number of data points" },
    .{ .name = "ts.mrevrange", .summary = "Query a range across multiple time series by filters in reverse direction", .since = "1.1.0", .group = "timeseries", .complexity = "O(N) where N is the number of data points" },
    .{ .name = "ts.queryindex", .summary = "Query all time series keys matching a filter list", .since = "1.0.0", .group = "timeseries", .complexity = "O(N) where N is the number of time-series that match the filters" },
    .{ .name = "ts.createrule", .summary = "Create a compaction rule", .since = "1.0.0", .group = "timeseries", .complexity = "O(1)" },
    .{ .name = "ts.deleterule", .summary = "Delete a compaction rule", .since = "1.0.0", .group = "timeseries", .complexity = "O(1)" },
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
        "COMMAND LIST [FILTERBY (ACLCAT <cat> | PATTERN <pat> | MODULE <mod>)] - List command names (Redis 7.0+)",
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

test "COMMAND LIST - no filter returns all commands" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, null, null);
    defer allocator.free(result);
    // Should start with a large array
    try std.testing.expect(std.mem.startsWith(u8, result, "*"));
    // Should include "get" and "set"
    try std.testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "set") != null);
}

test "COMMAND LIST FILTERBY ACLCAT string - returns string commands" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "ACLCAT", "string");
    defer allocator.free(result);
    // Should contain string commands like "get", "set"
    try std.testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "set") != null);
    // Should NOT contain list commands like "lpush"
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nlpush") == null);
}

test "COMMAND LIST FILTERBY ACLCAT list - returns list commands" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "ACLCAT", "list");
    defer allocator.free(result);
    // Should contain list commands
    try std.testing.expect(std.mem.indexOf(u8, result, "lpush") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "lrange") != null);
    // Should NOT contain string commands like "append"
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nappend") == null);
}

test "COMMAND LIST FILTERBY ACLCAT hash - returns hash commands" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "ACLCAT", "hash");
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "hset") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "hget") != null);
}

test "COMMAND LIST FILTERBY ACLCAT @sortedset - returns zset commands (with @ prefix)" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "ACLCAT", "@sortedset");
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "zadd") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "zrange") != null);
}

test "COMMAND LIST FILTERBY PATTERN z* - returns commands starting with z" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "PATTERN", "z*");
    defer allocator.free(result);
    // Should contain zadd, zrange, etc.
    try std.testing.expect(std.mem.indexOf(u8, result, "zadd") != null);
    // Should NOT contain "set" (no z prefix)
    try std.testing.expect(std.mem.indexOf(u8, result, "$3\r\nset") == null);
}

test "COMMAND LIST FILTERBY PATTERN *get* - returns commands with 'get' substring" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "PATTERN", "*get*");
    defer allocator.free(result);
    // "get", "getset", "getdel", "getex" should all match
    try std.testing.expect(std.mem.indexOf(u8, result, "getset") != null or std.mem.indexOf(u8, result, "get") != null);
}

test "COMMAND LIST FILTERBY MODULE - returns empty (built-in commands have no module)" {
    const allocator = std.testing.allocator;
    const result = try cmdCommandList(allocator, "MODULE", "any_module");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "groupMatchesAclCat - mapping correctness" {
    // Direct group matches
    try std.testing.expect(groupMatchesAclCat("string", "string"));
    try std.testing.expect(groupMatchesAclCat("list", "list"));
    try std.testing.expect(groupMatchesAclCat("sorted_set", "sortedset"));
    try std.testing.expect(groupMatchesAclCat("sorted_set", "sorted_set"));
    try std.testing.expect(groupMatchesAclCat("generic", "keyspace"));
    try std.testing.expect(groupMatchesAclCat("server", "admin"));
    try std.testing.expect(groupMatchesAclCat("server", "dangerous"));
    try std.testing.expect(groupMatchesAclCat("cluster", "server"));
    // @all matches everything
    try std.testing.expect(groupMatchesAclCat("string", "all"));
    try std.testing.expect(groupMatchesAclCat("string", "@all"));
    // Non-matches
    try std.testing.expect(!groupMatchesAclCat("string", "list"));
    try std.testing.expect(!groupMatchesAclCat("list", "hash"));
}
