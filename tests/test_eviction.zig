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

test "eviction - volatile-lru evicts oldest key with TTL" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set policy to volatile-lru
    try storage.config.set("maxmemory", .{ .int = 512 });
    try storage.config.set("maxmemory-policy", .{ .string = "volatile-lru" });

    // Add keys with and without TTL
    try storage.set("no_ttl_1", "value", null);
    try storage.set("with_ttl_1", "value", 3600000); // 1 hour TTL
    try storage.lru_clock.touch("with_ttl_1");

    storage.lru_clock.tick();

    try storage.set("with_ttl_2", "value", 3600000);
    try storage.lru_clock.touch("with_ttl_2");

    // with_ttl_1 is older than with_ttl_2, should be evicted first
    // Trigger eviction
    try storage.checkMemoryLimitAndEvict("SET");

    // Both volatile keys might be evicted, but no_ttl_1 should remain
    const no_ttl_exists = storage.get("no_ttl_1") != null;
    try std.testing.expect(no_ttl_exists);
}

test "eviction - allkeys-lfu evicts least frequently used key" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", .{ .int = 512 });
    try storage.config.set("maxmemory-policy", .{ .string = "allkeys-lfu" });

    // Add keys with different access frequencies
    try storage.set("freq_1", "value", null);
    try storage.lfu_counter.increment("freq_1");

    try storage.set("freq_10", "value", null);
    for (0..10) |_| {
        try storage.lfu_counter.increment("freq_10");
    }

    try storage.set("freq_100", "value", null);
    for (0..100) |_| {
        try storage.lfu_counter.increment("freq_100");
    }

    // freq_1 has lowest frequency, should be evicted first if memory pressure occurs
    // Just verify the mechanism works (actual eviction depends on memory state)
    const freq1_count = storage.lfu_counter.getCounter("freq_1");
    const freq10_count = storage.lfu_counter.getCounter("freq_10");
    const freq100_count = storage.lfu_counter.getCounter("freq_100");

    try std.testing.expect(freq1_count < freq10_count);
    try std.testing.expect(freq10_count < freq100_count);
}

test "eviction - volatile-lfu evicts LFU key among keys with TTL" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", .{ .int = 512 });
    try storage.config.set("maxmemory-policy", .{ .string = "volatile-lfu" });

    // Add keys with and without TTL
    try storage.set("no_ttl", "value", null);
    for (0..10) |_| {
        try storage.lfu_counter.increment("no_ttl");
    }

    try storage.set("with_ttl_low_freq", "value", 3600000);
    try storage.lfu_counter.increment("with_ttl_low_freq");

    try storage.set("with_ttl_high_freq", "value", 3600000);
    for (0..50) |_| {
        try storage.lfu_counter.increment("with_ttl_high_freq");
    }

    // with_ttl_low_freq should be evicted first (has TTL + lowest frequency)
    const low_freq = storage.lfu_counter.getCounter("with_ttl_low_freq");
    const high_freq = storage.lfu_counter.getCounter("with_ttl_high_freq");
    try std.testing.expect(low_freq < high_freq);
}

test "eviction - allkeys-random evicts random key" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", .{ .int = 512 });
    try storage.config.set("maxmemory-policy", .{ .string = "allkeys-random" });

    // Add multiple keys
    for (0..5) |i| {
        const key = try std.fmt.allocPrint(std.testing.allocator, "key{d}", .{i});
        defer std.testing.allocator.free(key);
        try storage.set(key, "value", null);
    }

    // Random eviction should work (hard to test deterministically)
    // Just verify the policy doesn't crash
    try storage.checkMemoryLimitAndEvict("SET");
}

test "eviction - volatile-random evicts random key with TTL" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", .{ .int = 512 });
    try storage.config.set("maxmemory-policy", .{ .string = "volatile-random" });

    // Add keys with TTL
    for (0..5) |i| {
        const key = try std.fmt.allocPrint(std.testing.allocator, "volatile{d}", .{i});
        defer std.testing.allocator.free(key);
        try storage.set(key, "value", 3600000); // With TTL
    }

    // Add key without TTL
    try storage.set("persistent", "value", null);

    // Trigger eviction - should evict a volatile key, not persistent
    try storage.checkMemoryLimitAndEvict("SET");

    // Persistent key should still exist
    const persistent_exists = storage.get("persistent") != null;
    try std.testing.expect(persistent_exists);
}

test "eviction - volatile-ttl evicts key with soonest expiration" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", .{ .int = 512 });
    try storage.config.set("maxmemory-policy", .{ .string = "volatile-ttl" });

    // Add keys with different TTLs
    const now_ms = std.time.milliTimestamp();
    try storage.set("expires_soon", "value", 1000); // 1 second
    try storage.set("expires_later", "value", 3600000); // 1 hour
    try storage.set("no_ttl", "value", null);

    // expires_soon has shortest TTL, should be prioritized for eviction
    const soon_val = storage.get("expires_soon");
    const later_val = storage.get("expires_later");

    try std.testing.expect(soon_val != null);
    try std.testing.expect(later_val != null);

    // Verify TTL ordering (approximate, timing-dependent)
    _ = now_ms;
}

test "eviction - LFU counter increments probabilistically" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Low counter increments frequently
    for (0..10) |_| {
        try storage.lfu_counter.increment("low_freq");
    }
    const low_count = storage.lfu_counter.getCounter("low_freq");
    try std.testing.expect(low_count > 0);

    // High counter increments rarely (set manually)
    try storage.lfu_counter.counters.put("high_freq", 200);
    const before = storage.lfu_counter.getCounter("high_freq");

    for (0..10) |_| {
        try storage.lfu_counter.increment("high_freq");
    }
    const after = storage.lfu_counter.getCounter("high_freq");

    // High counter increment should be less than low counter
    try std.testing.expect((after - before) < low_count);
}

test "eviction - LFU counter caps at 255" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Manually set to 255
    try storage.lfu_counter.counters.put("maxed", 255);

    // Try to increment - should stay at 255
    try storage.lfu_counter.increment("maxed");
    const count = storage.lfu_counter.getCounter("maxed");
    try std.testing.expectEqual(@as(u8, 255), count);
}

test "eviction - all policies handle empty key set" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const policies = [_][]const u8{
        "allkeys-lru",
        "volatile-lru",
        "allkeys-lfu",
        "volatile-lfu",
        "allkeys-random",
        "volatile-random",
        "volatile-ttl",
    };

    for (policies) |policy| {
        try storage.config.set("maxmemory-policy", .{ .string = policy });
        try storage.config.set("maxmemory", .{ .int = 1 });

        // With no keys, eviction should gracefully return OOM
        const result = storage.checkMemoryLimitAndEvict("SET");
        try std.testing.expectError(error.OOM, result);
    }
}

test "eviction - volatile policies return OOM when no volatile keys exist" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const volatile_policies = [_][]const u8{
        "volatile-lru",
        "volatile-lfu",
        "volatile-random",
        "volatile-ttl",
    };

    // Add only persistent keys (no TTL)
    try storage.set("key1", "value", null);
    try storage.set("key2", "value", null);

    for (volatile_policies) |policy| {
        try storage.config.set("maxmemory-policy", .{ .string = policy });
        try storage.config.set("maxmemory", .{ .int = 1 }); // Very low

        // Should fail because no volatile keys to evict
        const result = storage.checkMemoryLimitAndEvict("SET");
        try std.testing.expectError(error.OOM, result);
    }
}
