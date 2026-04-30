const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;

/// Test CONFIG GET for activedefrag parameters
test "CONFIG GET activedefrag" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Test CONFIG GET activedefrag
    const result = try storage.config.get("activedefrag");
    try testing.expectEqual(false, result.bool);
}

/// Test CONFIG GET for activedefrag-cycle-min
test "CONFIG GET activedefrag-cycle-min" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.config.get("activedefrag-cycle-min");
    try testing.expectEqual(@as(i64, 1), result.int);
}

/// Test CONFIG GET for activedefrag-cycle-max
test "CONFIG GET activedefrag-cycle-max" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.config.get("activedefrag-cycle-max");
    try testing.expectEqual(@as(i64, 25), result.int);
}

/// Test CONFIG GET for activedefrag-threshold-lower
test "CONFIG GET activedefrag-threshold-lower" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.config.get("activedefrag-threshold-lower");
    try testing.expectEqual(@as(i64, 10), result.int);
}

/// Test CONFIG GET for activedefrag-threshold-upper
test "CONFIG GET activedefrag-threshold-upper" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.config.get("activedefrag-threshold-upper");
    try testing.expectEqual(@as(i64, 100), result.int);
}

/// Test CONFIG SET activedefrag to yes
test "CONFIG SET activedefrag yes" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Initially false
    const before = try storage.config.get("activedefrag");
    try testing.expectEqual(false, before.bool);

    // Set to yes
    try storage.config.set("activedefrag", .{ .bool = true });

    // Verify changed
    const after = try storage.config.get("activedefrag");
    try testing.expectEqual(true, after.bool);
}

/// Test CONFIG SET activedefrag-cycle-min
test "CONFIG SET activedefrag-cycle-min" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("activedefrag-cycle-min", .{ .int = 5 });
    const result = try storage.config.get("activedefrag-cycle-min");
    try testing.expectEqual(@as(i64, 5), result.int);
}

/// Test CONFIG SET activedefrag-cycle-max
test "CONFIG SET activedefrag-cycle-max" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("activedefrag-cycle-max", .{ .int = 50 });
    const result = try storage.config.get("activedefrag-cycle-max");
    try testing.expectEqual(@as(i64, 50), result.int);
}

/// Test CONFIG SET activedefrag-threshold-lower
test "CONFIG SET activedefrag-threshold-lower" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("activedefrag-threshold-lower", .{ .int = 20 });
    const result = try storage.config.get("activedefrag-threshold-lower");
    try testing.expectEqual(@as(i64, 20), result.int);
}

/// Test CONFIG SET activedefrag-threshold-upper
test "CONFIG SET activedefrag-threshold-upper" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("activedefrag-threshold-upper", .{ .int = 200 });
    const result = try storage.config.get("activedefrag-threshold-upper");
    try testing.expectEqual(@as(i64, 200), result.int);
}

/// Test defrag task is not running when activedefrag is disabled
test "defrag task not running when disabled" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // activedefrag defaults to false
    const stats = storage.defrag_task.getStats();
    try testing.expect(!stats.is_running);
}

/// Test defrag task stats
test "defrag task stats" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const stats = storage.defrag_task.getStats();
    try testing.expectEqual(@as(usize, 0), stats.keys_scanned);
    try testing.expectEqual(@as(usize, 0), stats.keys_defragmented);
    try testing.expectEqual(@as(usize, 0), stats.bytes_freed);
}

/// Test defrag task reset stats
test "defrag task reset stats" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Manually set some stats
    storage.defrag_task.mutex.lock();
    storage.defrag_task.keys_scanned = 100;
    storage.defrag_task.keys_defragmented = 10;
    storage.defrag_task.bytes_freed = 1024;
    storage.defrag_task.mutex.unlock();

    storage.defrag_task.resetStats();

    const stats = storage.defrag_task.getStats();
    try testing.expectEqual(@as(usize, 0), stats.keys_scanned);
    try testing.expectEqual(@as(usize, 0), stats.keys_defragmented);
    try testing.expectEqual(@as(usize, 0), stats.bytes_freed);
}
