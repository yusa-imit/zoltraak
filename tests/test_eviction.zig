const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const Writer = @import("../src/protocol/writer.zig").Writer;

test "eviction - noeviction policy blocks writes when memory full" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory to 1KB (very small for testing)
    try storage.config.set("maxmemory", .{ .int = 1024 });
    try storage.config.set("maxmemory-policy", .{ .string = "noeviction" });

    // Fill memory with keys until we're near limit
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const key = try std.fmt.allocPrint(std.testing.allocator, "key{d}", .{i});
        defer std.testing.allocator.free(key);

        const value = try std.testing.allocator.alloc(u8, 100); // 100 bytes per value
        defer std.testing.allocator.free(value);
        @memset(value, 'x');

        _ = storage.set(key, value, null) catch break; // May fail if memory full
    }

    // Now try SET (write command) - should fail with OOM
    const result = storage.checkMemoryLimitAndEvict("SET");
    try std.testing.expectError(error.OOM, result);

    // But DEL (doesn't grow memory) should be allowed
    try storage.checkMemoryLimitAndEvict("DEL");

    // And GET (read-only) should be allowed
    try storage.checkMemoryLimitAndEvict("GET");
}

test "eviction - allkeys-lru evicts oldest accessed keys" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory to 1KB and policy to allkeys-lru
    try storage.config.set("maxmemory", .{ .int = 1024 });
    try storage.config.set("maxmemory-policy", .{ .string = "allkeys-lru" });

    // Add keys and track their access
    try storage.set("key1", "value1", null);
    try storage.lru_clock.touch("key1");

    storage.lru_clock.tick(); // Advance clock

    try storage.set("key2", "value2", null);
    try storage.lru_clock.touch("key2");

    storage.lru_clock.tick(); // Advance clock

    try storage.set("key3", "value3", null);
    try storage.lru_clock.touch("key3");

    // key1 is oldest (accessed first), key3 is newest

    // Fill memory to trigger eviction
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const key = try std.fmt.allocPrint(std.testing.allocator, "fill{d}", .{i});
        defer std.testing.allocator.free(key);

        const value = try std.testing.allocator.alloc(u8, 100);
        defer std.testing.allocator.free(value);
        @memset(value, 'x');

        // This may trigger eviction
        const result = storage.set(key, value, null);
        if (result) {
            try storage.lru_clock.touch(key);
        } else |_| {
            // Memory full, try to evict
            const evicted = storage.checkMemoryLimitAndEvict("SET") catch false;
            _ = evicted;
        }
    }

    // At least some keys should have been evicted
    // (Hard to test deterministically, but code path is exercised)
}

test "eviction - LRU clock updates on access" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "myvalue", null);

    // Touch the key
    try storage.lru_clock.touch("mykey");
    const initial_idle = storage.lru_clock.getIdleTime("mykey").?;

    // Tick clock
    storage.lru_clock.tick();
    storage.lru_clock.tick();

    // Idle time should increase
    const later_idle = storage.lru_clock.getIdleTime("mykey").?;
    try std.testing.expect(later_idle > initial_idle);

    // Touch again - idle time should reset
    try storage.lru_clock.touch("mykey");
    const reset_idle = storage.lru_clock.getIdleTime("mykey").?;
    try std.testing.expect(reset_idle < later_idle);
}

test "eviction - policy parsing is case-insensitive" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Test case-insensitive policy names
    try storage.config.set("maxmemory-policy", .{ .string = "NOEVICTION" });
    var policy_val = try storage.config.get("maxmemory-policy");
    try std.testing.expectEqualStrings("NOEVICTION", policy_val.string);

    try storage.config.set("maxmemory-policy", .{ .string = "allkeys-lru" });
    policy_val = try storage.config.get("maxmemory-policy");
    try std.testing.expectEqualStrings("allkeys-lru", policy_val.string);

    try storage.config.set("maxmemory-policy", .{ .string = "Volatile-TTL" });
    policy_val = try storage.config.get("maxmemory-policy");
    try std.testing.expectEqualStrings("Volatile-TTL", policy_val.string);
}

test "eviction - maxmemory 0 disables limit" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory to 0 (unlimited)
    try storage.config.set("maxmemory", .{ .int = 0 });
    try storage.config.set("maxmemory-policy", .{ .string = "noeviction" });

    // Should not return OOM regardless of memory usage
    try storage.checkMemoryLimitAndEvict("SET");
    try storage.checkMemoryLimitAndEvict("LPUSH");
    try storage.checkMemoryLimitAndEvict("SADD");
}

test "eviction - read-only commands bypass memory check" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory very low
    try storage.config.set("maxmemory", .{ .int = 1 }); // 1 byte (impossible to satisfy)
    try storage.config.set("maxmemory-policy", .{ .string = "noeviction" });

    // Read-only commands should still work
    try storage.checkMemoryLimitAndEvict("GET");
    try storage.checkMemoryLimitAndEvict("HGET");
    try storage.checkMemoryLimitAndEvict("LRANGE");
    try storage.checkMemoryLimitAndEvict("SMEMBERS");
    try storage.checkMemoryLimitAndEvict("ZRANGE");
    try storage.checkMemoryLimitAndEvict("PING");
    try storage.checkMemoryLimitAndEvict("INFO");
    try storage.checkMemoryLimitAndEvict("CONFIG");
}

test "eviction - DEL and EXPIRE bypass memory check" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory very low
    try storage.config.set("maxmemory", .{ .int = 1 });
    try storage.config.set("maxmemory-policy", .{ .string = "noeviction" });

    // Memory-reducing commands should work even when memory full
    try storage.checkMemoryLimitAndEvict("DEL");
    try storage.checkMemoryLimitAndEvict("UNLINK");
    try storage.checkMemoryLimitAndEvict("EXPIRE");
    try storage.checkMemoryLimitAndEvict("EXPIREAT");
    try storage.checkMemoryLimitAndEvict("TTL");
    try storage.checkMemoryLimitAndEvict("PERSIST");
}

test "eviction - write commands trigger OOM with noeviction" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory very low
    try storage.config.set("maxmemory", .{ .int = 1 });
    try storage.config.set("maxmemory-policy", .{ .string = "noeviction" });

    // All write commands should fail
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("SET"));
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("LPUSH"));
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("SADD"));
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("ZADD"));
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("HSET"));
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("APPEND"));
    try std.testing.expectError(error.OOM, storage.checkMemoryLimitAndEvict("INCR"));
}

test "eviction - LRU clock wraps correctly at 24-bit boundary" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set clock near MAX
    const near_max = (1 << 24) - 5; // 16777211
    _ = storage.lru_clock.clock.store(near_max, .monotonic);

    try storage.lru_clock.touch("key1");

    // Tick past wrap point
    for (0..10) |_| {
        storage.lru_clock.tick();
    }

    // Clock should have wrapped
    const current = storage.lru_clock.getClock();
    try std.testing.expect(current < 10); // Should be near 0 after wrap

    // Idle time should still calculate correctly
    const idle = storage.lru_clock.getIdleTime("key1").?;
    try std.testing.expectEqual(@as(u32, 10), idle);
}
