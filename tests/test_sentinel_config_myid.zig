const std = @import("std");
const storage_mod = @import("../src/storage/memory.zig");
const sentinel_cmds = @import("../src/commands/sentinel.zig");
const Writer = @import("../src/protocol/writer.zig").Writer;

const Storage = storage_mod.Storage;

test "SENTINEL MYID: returns 40-char bulk string" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Enable Sentinel mode
    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "MYID" };
    const result = try sentinel_cmds.cmdSentinelMyid(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return bulk string format: $40\r\n<id>\r\n
    try std.testing.expect(std.mem.startsWith(u8, result, "$40\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, result, "\r\n"));

    // Extract ID from result (skip "$40\r\n" and trailing "\r\n")
    const id_start = "$40\r\n".len;
    const id_end = result.len - 2; // Remove trailing \r\n
    const id = result[id_start..id_end];

    // ID should be 40 characters
    try std.testing.expectEqual(@as(usize, 40), id.len);

    // All characters should be hex
    for (id) |char| {
        const is_hex = (char >= '0' and char <= '9') or (char >= 'a' and char <= 'f');
        try std.testing.expect(is_hex);
    }
}

test "SENTINEL MYID: rejects extra arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "MYID", "extra" };
    const result = try sentinel_cmds.cmdSentinelMyid(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
}

test "SENTINEL MYID: returns error when Sentinel disabled" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Sentinel mode disabled (default)
    try std.testing.expectEqual(false, storage.sentinel.enabled);

    const args = [_][]const u8{ "SENTINEL", "MYID" };
    const result = try sentinel_cmds.cmdSentinelMyid(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR This instance has sentinel mode disabled"));
}

test "SENTINEL MYID: returns same ID on multiple calls" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "MYID" };

    const result1 = try sentinel_cmds.cmdSentinelMyid(allocator, &args, &storage, null, 0);
    defer allocator.free(result1);

    const result2 = try sentinel_cmds.cmdSentinelMyid(allocator, &args, &storage, null, 0);
    defer allocator.free(result2);

    // Should return identical results
    try std.testing.expectEqualStrings(result1, result2);
}

test "SENTINEL CONFIG GET sentinel-config-file: returns array with path" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "CONFIG", "GET", "sentinel-config-file" };
    const result = try sentinel_cmds.cmdSentinelConfigGet(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return array format: *2\r\n$20\r\nsentinel-config-file\r\n$<len>\r\n<path>\r\n
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "sentinel-config-file") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, storage.sentinel_config_path) != null);
}

test "SENTINEL CONFIG GET unknown-param: returns empty array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "CONFIG", "GET", "unknown-param" };
    const result = try sentinel_cmds.cmdSentinelConfigGet(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return empty array: *0\r\n
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "SENTINEL CONFIG GET: rejects wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Too few arguments
    const args1 = [_][]const u8{ "SENTINEL", "CONFIG", "GET" };
    const result1 = try sentinel_cmds.cmdSentinelConfigGet(allocator, &args1, &storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.startsWith(u8, result1, "-ERR wrong number of arguments"));

    // Too many arguments
    const args2 = [_][]const u8{ "SENTINEL", "CONFIG", "GET", "param", "extra" };
    const result2 = try sentinel_cmds.cmdSentinelConfigGet(allocator, &args2, &storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.startsWith(u8, result2, "-ERR wrong number of arguments"));
}

test "SENTINEL CONFIG GET: returns error when Sentinel disabled" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Sentinel mode disabled (default)

    const args = [_][]const u8{ "SENTINEL", "CONFIG", "GET", "sentinel-config-file" };
    const result = try sentinel_cmds.cmdSentinelConfigGet(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR This instance has sentinel mode disabled"));
}

test "SENTINEL CONFIG SET sentinel-config-file: rejects runtime change" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "CONFIG", "SET", "sentinel-config-file", "/new/path.conf" };
    const result = try sentinel_cmds.cmdSentinelConfigSet(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    // Should return error (cannot change at runtime)
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR Cannot change sentinel-config-file at runtime"));
}

test "SENTINEL CONFIG SET unknown-param: returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    const args = [_][]const u8{ "SENTINEL", "CONFIG", "SET", "unknown-param", "value" };
    const result = try sentinel_cmds.cmdSentinelConfigSet(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR Unsupported CONFIG parameter"));
}

test "SENTINEL CONFIG SET: rejects wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    storage.sentinel.enabled = true;

    // Too few arguments
    const args1 = [_][]const u8{ "SENTINEL", "CONFIG", "SET", "param" };
    const result1 = try sentinel_cmds.cmdSentinelConfigSet(allocator, &args1, &storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.startsWith(u8, result1, "-ERR wrong number of arguments"));

    // Too many arguments
    const args2 = [_][]const u8{ "SENTINEL", "CONFIG", "SET", "param", "value", "extra" };
    const result2 = try sentinel_cmds.cmdSentinelConfigSet(allocator, &args2, &storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.startsWith(u8, result2, "-ERR wrong number of arguments"));
}

test "SENTINEL CONFIG SET: returns error when Sentinel disabled" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Sentinel mode disabled (default)

    const args = [_][]const u8{ "SENTINEL", "CONFIG", "SET", "some-param", "value" };
    const result = try sentinel_cmds.cmdSentinelConfigSet(allocator, &args, &storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR This instance has sentinel mode disabled"));
}
