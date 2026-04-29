const std = @import("std");
const Config = @import("config.zig").Config;

/// Eviction policy types
pub const EvictionPolicy = enum {
    noeviction,
    allkeys_lru,
    volatile_lru,
    allkeys_lfu,
    volatile_lfu,
    allkeys_random,
    volatile_random,
    volatile_ttl,

    /// Parse policy string from config (case-insensitive)
    pub fn parse(policy_str: []const u8) ?EvictionPolicy {
        if (std.ascii.eqlIgnoreCase(policy_str, "noeviction")) return .noeviction;
        if (std.ascii.eqlIgnoreCase(policy_str, "allkeys-lru")) return .allkeys_lru;
        if (std.ascii.eqlIgnoreCase(policy_str, "volatile-lru")) return .volatile_lru;
        if (std.ascii.eqlIgnoreCase(policy_str, "allkeys-lfu")) return .allkeys_lfu;
        if (std.ascii.eqlIgnoreCase(policy_str, "volatile-lfu")) return .volatile_lfu;
        if (std.ascii.eqlIgnoreCase(policy_str, "allkeys-random")) return .allkeys_random;
        if (std.ascii.eqlIgnoreCase(policy_str, "volatile-random")) return .volatile_random;
        if (std.ascii.eqlIgnoreCase(policy_str, "volatile-ttl")) return .volatile_ttl;
        return null;
    }
};

/// LRU Clock for tracking key access times
/// Uses 24-bit global counter updated on every access
/// This approximates Redis's LRU implementation
pub const LRUClock = struct {
    /// Global LRU clock (24-bit counter, wraps at 16777216)
    clock: std.atomic.Value(u32),

    /// Last access times for each key (24-bit snapshot of clock)
    last_access: std.StringHashMap(u32),

    allocator: std.mem.Allocator,

    const MAX_CLOCK: u32 = 1 << 24; // 16777216

    pub fn init(allocator: std.mem.Allocator) LRUClock {
        return LRUClock{
            .clock = std.atomic.Value(u32).init(0),
            .last_access = std.StringHashMap(u32).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *LRUClock) void {
        self.last_access.deinit();
    }

    /// Get current LRU clock value
    pub fn getClock(self: *const LRUClock) u32 {
        return self.clock.load(.monotonic) & (MAX_CLOCK - 1); // Mask to 24 bits
    }

    /// Tick the LRU clock (called periodically or on every command)
    pub fn tick(self: *LRUClock) void {
        const current = self.clock.load(.monotonic);
        _ = self.clock.cmpxchgWeak(current, (current + 1) & (MAX_CLOCK - 1), .monotonic, .monotonic);
    }

    /// Record access time for a key
    pub fn touch(self: *LRUClock, key: []const u8) !void {
        const current_clock = self.getClock();
        try self.last_access.put(key, current_clock);
    }

    /// Get idle time for a key (time since last access)
    /// Returns null if key not tracked
    pub fn getIdleTime(self: *const LRUClock, key: []const u8) ?u32 {
        const last_access_time = self.last_access.get(key) orelse return null;
        const current_clock = self.getClock();

        // Handle wrap-around
        if (current_clock >= last_access_time) {
            return current_clock - last_access_time;
        } else {
            return (MAX_CLOCK + current_clock) - last_access_time;
        }
    }

    /// Remove key from access tracking
    pub fn remove(self: *LRUClock, key: []const u8) void {
        _ = self.last_access.remove(key);
    }
};

/// Commands that grow memory and should trigger eviction
const WRITE_COMMANDS = std.StaticStringMap(void).initComptime(.{
    .{"SET"}, .{"SETNX"}, .{"SETEX"}, .{"PSETEX"}, .{"MSET"}, .{"MSETNX"}, .{"MSETEX"}, .{"APPEND"},
    .{"INCR"}, .{"DECR"}, .{"INCRBY"}, .{"DECRBY"}, .{"INCRBYFLOAT"},
    .{"LPUSH"}, .{"RPUSH"}, .{"LPUSHX"}, .{"RPUSHX"}, .{"LINSERT"}, .{"LSET"}, .{"LMPOP"},
    .{"SADD"}, .{"SINTERSTORE"}, .{"SUNIONSTORE"}, .{"SDIFFSTORE"},
    .{"ZADD"}, .{"ZINCRBY"}, .{"ZINTERSTORE"}, .{"ZUNIONSTORE"}, .{"ZDIFFSTORE"}, .{"ZRANGESTORE"}, .{"ZMPOP"},
    .{"HSET"}, .{"HSETNX"}, .{"HMSET"}, .{"HINCRBY"}, .{"HINCRBYFLOAT"}, .{"HSETEX"},
    .{"XADD"}, .{"XSETID"},
    .{"PFADD"}, .{"PFMERGE"},
    .{"GEOADD"},
    .{"JSON.SET"}, .{"JSON.MSET"}, .{"JSON.NUMINCRBY"}, .{"JSON.NUMMULTBY"}, .{"JSON.STRAPPEND"}, .{"JSON.ARRAPPEND"}, .{"JSON.ARRINSERT"},
    .{"TS.ADD"}, .{"TS.MADD"}, .{"TS.INCRBY"}, .{"TS.DECRBY"}, .{"TS.CREATE"},
    .{"BF.RESERVE"}, .{"BF.ADD"}, .{"BF.MADD"}, .{"BF.INSERT"}, .{"BF.LOADCHUNK"},
    .{"CF.RESERVE"}, .{"CF.ADD"}, .{"CF.ADDNX"}, .{"CF.INSERT"}, .{"CF.INSERTNX"}, .{"CF.LOADCHUNK"},
    .{"CMS.INITBYDIM"}, .{"CMS.INITBYPROB"}, .{"CMS.INCRBY"}, .{"CMS.MERGE"},
    .{"TOPK.RESERVE"}, .{"TOPK.ADD"}, .{"TOPK.INCRBY"},
    .{"TDIGEST.CREATE"}, .{"TDIGEST.ADD"}, .{"TDIGEST.MERGE"},
    .{"VADD"},
});

/// Check if a command grows memory and should trigger eviction check
pub fn isMemoryGrowingCommand(command: []const u8) bool {
    // Normalize to uppercase for comparison
    var upper_buf: [64]u8 = undefined;
    if (command.len > upper_buf.len) return false;

    const upper = std.ascii.upperString(&upper_buf, command);
    return WRITE_COMMANDS.has(upper);
}

test "eviction - policy parsing" {
    try std.testing.expectEqual(EvictionPolicy.noeviction, EvictionPolicy.parse("noeviction"));
    try std.testing.expectEqual(EvictionPolicy.allkeys_lru, EvictionPolicy.parse("allkeys-lru"));
    try std.testing.expectEqual(EvictionPolicy.allkeys_lru, EvictionPolicy.parse("ALLKEYS-LRU")); // case insensitive
    try std.testing.expectEqual(EvictionPolicy.volatile_ttl, EvictionPolicy.parse("volatile-ttl"));
    try std.testing.expectEqual(null, EvictionPolicy.parse("invalid-policy"));
}

test "eviction - LRU clock basic operations" {
    var clock = LRUClock.init(std.testing.allocator);
    defer clock.deinit();

    // Initial clock should be 0
    try std.testing.expectEqual(@as(u32, 0), clock.getClock());

    // Tick should increment
    clock.tick();
    try std.testing.expectEqual(@as(u32, 1), clock.getClock());

    // Touch should record access time
    try clock.touch("key1");
    try std.testing.expect(clock.last_access.contains("key1"));
    try std.testing.expectEqual(@as(u32, 1), clock.last_access.get("key1").?);

    // Idle time should be 0 immediately after touch
    try std.testing.expectEqual(@as(u32, 0), clock.getIdleTime("key1").?);

    // After ticking, idle time should increase
    clock.tick();
    clock.tick();
    try std.testing.expectEqual(@as(u32, 2), clock.getIdleTime("key1").?);
}

test "eviction - LRU clock wrapping" {
    var clock = LRUClock.init(std.testing.allocator);
    defer clock.deinit();

    // Set clock near max
    const near_max = LRUClock.MAX_CLOCK - 5;
    _ = clock.clock.store(near_max, .monotonic);

    try clock.touch("key1");
    try std.testing.expectEqual(near_max, clock.last_access.get("key1").?);

    // Tick past wrap point
    for (0..10) |_| {
        clock.tick();
    }

    // Clock should have wrapped
    const current = clock.getClock();
    try std.testing.expect(current < near_max);

    // Idle time should still calculate correctly across wrap
    const idle = clock.getIdleTime("key1").?;
    try std.testing.expectEqual(@as(u32, 10), idle);
}

test "eviction - memory growing commands" {
    try std.testing.expect(isMemoryGrowingCommand("SET"));
    try std.testing.expect(isMemoryGrowingCommand("set")); // case insensitive
    try std.testing.expect(isMemoryGrowingCommand("LPUSH"));
    try std.testing.expect(isMemoryGrowingCommand("SADD"));
    try std.testing.expect(isMemoryGrowingCommand("ZADD"));
    try std.testing.expect(isMemoryGrowingCommand("HSET"));

    // Commands that DON'T grow memory
    try std.testing.expect(!isMemoryGrowingCommand("GET"));
    try std.testing.expect(!isMemoryGrowingCommand("DEL"));
    try std.testing.expect(!isMemoryGrowingCommand("EXPIRE"));
    try std.testing.expect(!isMemoryGrowingCommand("TTL"));
    try std.testing.expect(!isMemoryGrowingCommand("PING"));
}

test "eviction - remove key from tracking" {
    var clock = LRUClock.init(std.testing.allocator);
    defer clock.deinit();

    try clock.touch("key1");
    try std.testing.expect(clock.last_access.contains("key1"));

    clock.remove("key1");
    try std.testing.expect(!clock.last_access.contains("key1"));
    try std.testing.expectEqual(null, clock.getIdleTime("key1"));
}
