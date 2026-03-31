const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const cluster_cmds = @import("../src/commands/cluster.zig");

test "CLUSTER RESET - soft reset with no keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Add some slots
    if (storage.cluster.myself) |myself| {
        const slots = [_]u16{ 0, 1, 2, 100, 200 };
        try storage.cluster.addSlotsToNode(myself, &slots);
    }

    const args = [_][]const u8{ "CLUSTER", "RESET", "SOFT" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return +OK
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // Verify all slots are cleared
    for (storage.cluster.slots) |slot| {
        try std.testing.expectEqual(@as(?*@import("../src/storage/cluster.zig").ClusterNode, null), slot);
    }
}

test "CLUSTER RESET - soft reset default mode" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "RESET" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return +OK (default is SOFT)
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);
}

test "CLUSTER RESET - hard reset with no keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Save original node ID
    var original_id: [40]u8 = undefined;
    if (storage.cluster.myself) |myself| {
        @memcpy(&original_id, &myself.id);
    }

    const args = [_][]const u8{ "CLUSTER", "RESET", "HARD" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return +OK
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // Verify node ID changed
    if (storage.cluster.myself) |myself| {
        try std.testing.expect(!std.mem.eql(u8, &myself.id, &original_id));
    }

    // Verify epochs reset to 0
    try std.testing.expectEqual(@as(u64, 0), storage.cluster.current_epoch);
    if (storage.cluster.myself) |myself| {
        try std.testing.expectEqual(@as(u64, 0), myself.config_epoch);
    }
}

test "CLUSTER RESET - fails when master has keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Add a key to storage
    try storage.set("testkey", "testvalue");

    const args = [_][]const u8{ "CLUSTER", "RESET", "SOFT" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "master nodes containing keys") != null);
}

test "CLUSTER RESET - cluster disabled" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = false;

    const args = [_][]const u8{ "CLUSTER", "RESET" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "cluster support disabled") != null);
}

test "CLUSTER RESET - invalid mode" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "RESET", "INVALID" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "HARD or SOFT") != null);
}

test "CLUSTER RESET - arity error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "RESET", "SOFT", "EXTRA" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "CLUSTER RESET - case insensitive mode" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "RESET", "HaRd" };
    const result = try cluster_cmds.cmdClusterReset(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return +OK (case insensitive)
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);
}
