const std = @import("std");
const storage_mod = @import("../src/storage/memory.zig");
const parser_mod = @import("../src/protocol/parser.zig");
const sentinel_cmds = @import("../src/commands/sentinel.zig");

const Storage = storage_mod.Storage;
const RespValue = parser_mod.RespValue;

// Integration tests for SENTINEL SENTINELS and IS-MASTER-DOWN-BY-ADDR commands

test "SENTINEL SENTINELS: returns empty array when no sentinels registered" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Monitor a master first
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    const args = [_][]const u8{ "SENTINEL", "SENTINELS", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelSentinels(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should be array with 0 elements: *0\r\n
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "SENTINEL SENTINELS: returns sentinel info after registration" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Monitor a master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Register a sentinel
    try storage.sentinel.registerSentinel("mymaster", "abc123def456", "127.0.0.2", 26379);

    const args = [_][]const u8{ "SENTINEL", "SENTINELS", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelSentinels(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should contain array with 1 sentinel info array
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    // Should contain the sentinel ID
    try std.testing.expect(std.mem.indexOf(u8, result, "abc123def456") != null);
    // Should contain the IP
    try std.testing.expect(std.mem.indexOf(u8, result, "127.0.0.2") != null);
    // Should contain the port
    try std.testing.expect(std.mem.indexOf(u8, result, "26379") != null);
}

test "SENTINEL SENTINELS: returns multiple sentinels" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Monitor a master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Register two sentinels
    try storage.sentinel.registerSentinel("mymaster", "sentinel1", "127.0.0.2", 26379);
    try storage.sentinel.registerSentinel("mymaster", "sentinel2", "127.0.0.3", 26380);

    const args = [_][]const u8{ "SENTINEL", "SENTINELS", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelSentinels(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should contain array with 2 sentinel info arrays
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "SENTINEL SENTINELS: returns error for unknown master" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "SENTINELS", "nonexistent" };
    const result = try sentinel_cmds.cmdSentinelSentinels(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "No such master") != null);
}

test "SENTINEL SENTINELS: returns error when sentinel disabled" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    // sentinel.enabled is false by default

    const args = [_][]const u8{ "SENTINEL", "SENTINELS", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelSentinels(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "sentinel mode disabled") != null);
}

test "SENTINEL SENTINELS: validates arity" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Too few arguments
    const args1 = [_][]const u8{ "SENTINEL", "SENTINELS" };
    const result1 = try sentinel_cmds.cmdSentinelSentinels(allocator, &args1, &storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.startsWith(u8, result1, "-ERR"));

    // Too many arguments
    const args2 = [_][]const u8{ "SENTINEL", "SENTINELS", "master1", "extra" };
    const result2 = try sentinel_cmds.cmdSentinelSentinels(allocator, &args2, &storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.startsWith(u8, result2, "-ERR"));
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: returns [0, null] for unknown master" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*" };
    const result = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should be array: *2\r\n:0\r\n$-1\r\n (0 and null)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n:0\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$-1") != null);
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: returns [0, null] for healthy master" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Monitor a master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    const args = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*" };
    const result = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should be array: *2\r\n:0\r\n$-1\r\n (0 and null) - master is healthy
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n:0\r\n"));
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: returns [1, null] for down master" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Monitor a master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Mark master as down
    const master = storage.sentinel.getMaster("mymaster").?;
    master.is_down = true;

    const args = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*" };
    const result = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should be array: *2\r\n:1\r\n$-1\r\n (1 and null) - master is down
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n:1\r\n"));
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: returns error when sentinel disabled" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    // sentinel.enabled is false by default

    const args = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*" };
    const result = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "sentinel mode disabled") != null);
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: validates arity" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Too few arguments
    const args1 = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0" };
    const result1 = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args1, &storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.startsWith(u8, result1, "-ERR"));

    // Too many arguments
    const args2 = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*", "extra" };
    const result2 = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args2, &storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.startsWith(u8, result2, "-ERR"));
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: validates port" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Invalid port
    const args = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "invalid", "0", "*" };
    const result = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "Invalid port") != null);
}

test "SENTINEL IS-MASTER-DOWN-BY-ADDR: matches by IP and port exactly" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();
    storage.sentinel.enabled = true;

    // Monitor two masters
    try storage.sentinel.monitorMaster("master1", "127.0.0.1", 6379, 2);
    try storage.sentinel.monitorMaster("master2", "127.0.0.2", 6380, 2);

    // Mark master1 as down
    const master1 = storage.sentinel.getMaster("master1").?;
    master1.is_down = true;

    // Query master1 (should be down)
    const args1 = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*" };
    const result1 = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args1, &storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.startsWith(u8, result1, "*2\r\n:1\r\n"));

    // Query master2 (should be healthy)
    const args2 = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.2", "6380", "0", "*" };
    const result2 = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args2, &storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.startsWith(u8, result2, "*2\r\n:0\r\n"));

    // Query wrong IP (should be unknown)
    const args3 = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.99", "6379", "0", "*" };
    const result3 = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args3, &storage, null, 0);
    defer allocator.free(result3);
    try std.testing.expect(std.mem.startsWith(u8, result3, "*2\r\n:0\r\n"));

    // Query wrong port (should be unknown)
    const args4 = [_][]const u8{ "SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "9999", "0", "*" };
    const result4 = try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, &args4, &storage, null, 0);
    defer allocator.free(result4);
    try std.testing.expect(std.mem.startsWith(u8, result4, "*2\r\n:0\r\n"));
}
