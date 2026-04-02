const std = @import("std");
const storage_mod = @import("../src/storage/memory.zig");
const sentinel_cmds = @import("../src/commands/sentinel.zig");
const parser_mod = @import("../src/protocol/parser.zig");

const Storage = storage_mod.Storage;
const RespValue = parser_mod.RespValue;

/// Helper to parse RESP response
fn parseResp(allocator: std.mem.Allocator, data: []const u8) !RespValue {
    var parser = parser_mod.Parser.init(allocator);
    defer parser.deinit();
    return try parser.parse(data);
}

// ============================================================================
// SENTINEL CKQUORUM Integration Tests
// ============================================================================

test "SENTINEL CKQUORUM: returns OK when quorum can be reached" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable Sentinel mode
    storage.sentinel.enabled = true;

    // Add master with quorum of 2
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try storage.sentinel.registerSentinel("mymaster", "s1", "127.0.0.2", 26379);

    // Run command
    const args = [_][]const u8{ "SENTINEL", "CKQUORUM", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelCkquorum(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return simple string with "OK"
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.simple_string, resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.simple_string, "OK") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.simple_string, "2 usable Sentinels") != null);
}

test "SENTINEL CKQUORUM: returns NOQUORUM when quorum cannot be reached" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Add master with quorum of 5 (too high)
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 5);
    try storage.sentinel.registerSentinel("mymaster", "s1", "127.0.0.2", 26379);

    // Run command
    const args = [_][]const u8{ "SENTINEL", "CKQUORUM", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelCkquorum(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return simple string with "NOQUORUM"
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.simple_string, resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.simple_string, "NOQUORUM") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.simple_string, "2 usable Sentinels") != null);
}

test "SENTINEL CKQUORUM: returns error for unknown master" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Run command with nonexistent master
    const args = [_][]const u8{ "SENTINEL", "CKQUORUM", "nonexistent" };
    const result = try sentinel_cmds.cmdSentinelCkquorum(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "No such master") != null);
}

test "SENTINEL CKQUORUM: returns error when sentinel disabled" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Sentinel disabled by default

    // Run command
    const args = [_][]const u8{ "SENTINEL", "CKQUORUM", "mymaster" };
    const result = try sentinel_cmds.cmdSentinelCkquorum(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "sentinel mode disabled") != null);
}

test "SENTINEL CKQUORUM: returns error with wrong arg count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Run command with wrong args
    const args = [_][]const u8{ "SENTINEL", "CKQUORUM" };
    const result = try sentinel_cmds.cmdSentinelCkquorum(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "wrong number of arguments") != null);
}

// ============================================================================
// SENTINEL FLUSHCONFIG Integration Tests
// ============================================================================

test "SENTINEL FLUSHCONFIG: writes config to file" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Add a master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Override config path for test
    storage.sentinel_config_path = "test_flush.conf";

    // Run command
    const args = [_][]const u8{ "SENTINEL", "FLUSHCONFIG" };
    const result = try sentinel_cmds.cmdSentinelFlushconfig(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Cleanup file
    defer std.fs.cwd().deleteFile("test_flush.conf") catch {};

    // Should return OK
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.simple_string, resp);
    try std.testing.expectEqualStrings("OK", resp.simple_string);

    // Verify file was created
    const file = try std.fs.cwd().openFile("test_flush.conf", .{});
    defer file.close();
    const content = try file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(content);

    try std.testing.expect(std.mem.indexOf(u8, content, "sentinel monitor mymaster") != null);
}

test "SENTINEL FLUSHCONFIG: returns error when sentinel disabled" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Run command
    const args = [_][]const u8{ "SENTINEL", "FLUSHCONFIG" };
    const result = try sentinel_cmds.cmdSentinelFlushconfig(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "sentinel mode disabled") != null);
}

test "SENTINEL FLUSHCONFIG: returns error with wrong arg count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Run command with extra args
    const args = [_][]const u8{ "SENTINEL", "FLUSHCONFIG", "extra" };
    const result = try sentinel_cmds.cmdSentinelFlushconfig(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "wrong number of arguments") != null);
}

// ============================================================================
// SENTINEL SET Integration Tests
// ============================================================================

test "SENTINEL SET: sets quorum option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Add master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Run command to set quorum
    const args = [_][]const u8{ "SENTINEL", "SET", "mymaster", "quorum", "5" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return OK
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.simple_string, resp);
    try std.testing.expectEqualStrings("OK", resp.simple_string);

    // Verify quorum was updated
    const master = storage.sentinel.getMaster("mymaster").?;
    try std.testing.expectEqual(@as(u8, 5), master.quorum);
}

test "SENTINEL SET: sets down-after-milliseconds option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Add master
    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Run command to set down-after-milliseconds
    const args = [_][]const u8{ "SENTINEL", "SET", "mymaster", "down-after-milliseconds", "60000" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return OK
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.simple_string, resp);
    try std.testing.expectEqualStrings("OK", resp.simple_string);

    // Verify was updated
    const master = storage.sentinel.getMaster("mymaster").?;
    try std.testing.expectEqual(@as(u64, 60000), master.down_after_milliseconds);
}

test "SENTINEL SET: returns error for unknown master" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Run command with nonexistent master
    const args = [_][]const u8{ "SENTINEL", "SET", "nonexistent", "quorum", "3" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "No such master") != null);
}

test "SENTINEL SET: returns error for invalid value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Run command with invalid value
    const args = [_][]const u8{ "SENTINEL", "SET", "mymaster", "quorum", "abc" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "Invalid argument") != null);
}

test "SENTINEL SET: returns error for unsupported option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    try storage.sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Run command with unsupported option
    const args = [_][]const u8{ "SENTINEL", "SET", "mymaster", "unknown-option", "123" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "Unsupported option") != null);
}

test "SENTINEL SET: returns error when sentinel disabled" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Run command
    const args = [_][]const u8{ "SENTINEL", "SET", "mymaster", "quorum", "3" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "sentinel mode disabled") != null);
}

test "SENTINEL SET: returns error with wrong arg count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Run command with wrong args
    const args = [_][]const u8{ "SENTINEL", "SET", "mymaster", "quorum" };
    const result = try sentinel_cmds.cmdSentinelSet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    const resp = try parseResp(allocator, result);
    defer resp.deinit(allocator);

    try std.testing.expectEqual(RespValue.@"error", resp);
    try std.testing.expect(std.mem.indexOf(u8, resp.@"error", "wrong number of arguments") != null);
}
