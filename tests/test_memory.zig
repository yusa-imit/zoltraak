const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const cmdMemory = @import("../src/commands/introspection.zig").cmdMemory;
const cmdMemoryStats = @import("../src/commands/introspection.zig").cmdMemoryStats;
const cmdMemoryUsage = @import("../src/commands/introspection.zig").cmdMemoryUsage;
const cmdMemoryDoctor = @import("../src/commands/introspection.zig").cmdMemoryDoctor;
const cmdMemoryPurge = @import("../src/commands/introspection.zig").cmdMemoryPurge;
const cmdMemoryMallocStats = @import("../src/commands/introspection.zig").cmdMemoryMallocStats;
const cmdMemoryHelp = @import("../src/commands/introspection.zig").cmdMemoryHelp;

test "MEMORY STATS integration" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    // Populate with keys
    _ = try storage.set("key1", "value1", null);
    _ = try storage.set("key2", "value2", null);
    _ = try storage.rpush("list1", &[_][]const u8{ "a", "b", "c" }, null);

    const args = [_][]const u8{"stats"};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    // Verify bulk string format
    try std.testing.expect(std.mem.startsWith(u8, result, "$"));
    try std.testing.expect(std.mem.indexOf(u8, result, "peak.allocated:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "total.allocated:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "startup.allocated:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "dataset.bytes:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "keys.count:3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "fragmentation:") != null);
}

test "MEMORY USAGE with existing string key" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const test_value = "Hello, Redis!";
    _ = try storage.set("mykey", test_value, null);

    const args = [_][]const u8{ "usage", "mykey" };
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    // Should return integer (bytes)
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
    try std.testing.expect(result[result.len - 2] == '\r');
    try std.testing.expect(result[result.len - 1] == '\n');

    // Parse the size - should be > 0
    const size_str = result[1 .. result.len - 2];
    const size = try std.fmt.parseInt(i64, size_str, 10);
    try std.testing.expect(size > 0);
}

test "MEMORY USAGE with SAMPLES parameter" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    _ = try storage.set("key1", "value1", null);

    const args = [_][]const u8{ "usage", "key1", "samples", "10" };
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
}

test "MEMORY USAGE with list" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    _ = try storage.rpush("mylist", &[_][]const u8{ "a", "b", "c", "d", "e" }, null);

    const args = [_][]const u8{ "usage", "mylist" };
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    const size_str = result[1 .. result.len - 2];
    const size = try std.fmt.parseInt(i64, size_str, 10);
    try std.testing.expect(size > 100); // List should have overhead
}

test "MEMORY USAGE with hash" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const fields = [_][]const u8{ "field1", "field2" };
    const values = [_][]const u8{ "value1", "value2" };
    _ = try storage.hset("myhash", &fields, &values, null);

    const args = [_][]const u8{ "usage", "myhash" };
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    const size_str = result[1 .. result.len - 2];
    const size = try std.fmt.parseInt(i64, size_str, 10);
    try std.testing.expect(size > 200); // Hash should have significant overhead
}

test "MEMORY USAGE with missing key" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{ "usage", "nonexistent" };
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "MEMORY DOCTOR with healthy memory" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{"doctor"};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    // Should return bulk string
    try std.testing.expect(std.mem.startsWith(u8, result, "$"));
    try std.testing.expect(std.mem.indexOf(u8, result, "Hi Sam") != null);
}

test "MEMORY PURGE returns OK" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{"purge"};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "MEMORY MALLOC-STATS returns stats" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{"malloc-stats"};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "$"));
    try std.testing.expect(std.mem.indexOf(u8, result, "Allocator:") != null);
}

test "MEMORY HELP returns help text" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{"help"};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    // Should return array
    try std.testing.expect(std.mem.startsWith(u8, result, "*"));
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY STATS") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY USAGE") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY DOCTOR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY PURGE") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY MALLOC-STATS") != null);
}

test "MEMORY wrong number of args" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR wrong number of arguments") != null);
}

test "MEMORY unknown subcommand" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const args = [_][]const u8{"invalid"};
    const result = try cmdMemory(std.testing.allocator, &storage, &args);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR unknown subcommand") != null);
}

test "MEMORY tracking with key operations" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    // Initial state
    const initial_dataset = storage.memory_tracker.dataset_bytes;

    // Set a key
    _ = try storage.set("testkey", "testvalue", null);

    // Dataset should increase
    try std.testing.expect(storage.memory_tracker.dataset_bytes >= initial_dataset);

    // Get stats
    const stats_args = [_][]const u8{"stats"};
    const result = try cmdMemory(std.testing.allocator, &storage, &stats_args);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "keys.count:1") != null);
}
