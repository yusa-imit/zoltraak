const std = @import("std");
const testing = std.testing;
const storage_mod = @import("../src/storage/memory.zig");
const sentinel_cmd = @import("../src/commands/sentinel.zig");

test "SENTINEL SIMULATE-FAILURE: valid crash-after-election mode" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-123",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE", "crash-after-election" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
    try testing.expectEqual(@as(u8, 1), storage.sentinel.simulate_failure_flags);
}

test "SENTINEL SIMULATE-FAILURE: valid crash-after-promotion mode" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-456",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE", "crash-after-promotion" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
    try testing.expectEqual(@as(u8, 2), storage.sentinel.simulate_failure_flags);
}

test "SENTINEL SIMULATE-FAILURE: case-insensitive mode matching" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-789",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE", "CRASH-AFTER-ELECTION" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
    try testing.expectEqual(@as(u8, 1), storage.sentinel.simulate_failure_flags);
}

test "SENTINEL SIMULATE-FAILURE: help mode returns array" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-help",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE", "help" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    // Should return array with available modes
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "crash-after-election") != null);
    try testing.expect(std.mem.indexOf(u8, result, "crash-after-promotion") != null);
}

test "SENTINEL SIMULATE-FAILURE: error on unknown mode" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-error",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE", "invalid-mode" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "Unknown mode") != null);
}

test "SENTINEL SIMULATE-FAILURE: error on wrong arity" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-arity",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "SENTINEL SIMULATE-FAILURE: error when sentinel disabled" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = false,
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "SIMULATE-FAILURE", "crash-after-election" };
    const result = try sentinel_cmd.cmdSentinelSimulateFailure(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "sentinel mode disabled") != null);
}

test "SENTINEL PENDING-SCRIPTS: returns empty array (stub)" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-scripts",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "PENDING-SCRIPTS" };
    const result = try sentinel_cmd.cmdSentinelPendingScripts(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expectEqualStrings("*0\r\n", result);
}

test "SENTINEL PENDING-SCRIPTS: error on wrong arity" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-scripts-arity",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "PENDING-SCRIPTS", "extra-arg" };
    const result = try sentinel_cmd.cmdSentinelPendingScripts(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "SENTINEL PENDING-SCRIPTS: error when sentinel disabled" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = false,
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "PENDING-SCRIPTS" };
    const result = try sentinel_cmd.cmdSentinelPendingScripts(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "sentinel mode disabled") != null);
}

test "SENTINEL INFO-CACHE: returns empty array for valid master (stub)" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-info",
    });
    defer storage.deinit();

    // Monitor a master first
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    const args = &[_][]const u8{ "SENTINEL", "INFO-CACHE", "mymaster" };
    const result = try sentinel_cmd.cmdSentinelInfoCache(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expectEqualStrings("*0\r\n", result);
}

test "SENTINEL INFO-CACHE: error on missing master-name" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-info-arity",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "INFO-CACHE" };
    const result = try sentinel_cmd.cmdSentinelInfoCache(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "SENTINEL INFO-CACHE: error on unknown master" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = true,
        .sentinel_id = "test-sentinel-info-unknown",
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "INFO-CACHE", "unknown-master" };
    const result = try sentinel_cmd.cmdSentinelInfoCache(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "No such master") != null);
}

test "SENTINEL INFO-CACHE: error when sentinel disabled" {
    var storage = try storage_mod.Storage.init(testing.allocator, .{
        .sentinel_enabled = false,
    });
    defer storage.deinit();

    const args = &[_][]const u8{ "SENTINEL", "INFO-CACHE", "mymaster" };
    const result = try sentinel_cmd.cmdSentinelInfoCache(testing.allocator, args, &storage, null, 0);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "sentinel mode disabled") != null);
}
