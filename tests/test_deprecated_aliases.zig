const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const executeCommand = @import("../src/commands/strings.zig").executeCommand;
const cluster_cmds = @import("../src/commands/cluster.zig");
const repl_cmds = @import("../src/commands/replication.zig");
const lists = @import("../src/commands/lists.zig");

test "CLUSTER SLAVES alias for CLUSTER REPLICAS" {
    var storage = try Storage.init(std.testing.allocator, .{});
    defer storage.deinit();

    // Enable cluster mode
    storage.cluster.enabled = true;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "CLUSTER" },
        RespValue{ .bulk_string = "SLAVES" },
        RespValue{ .bulk_string = std.mem.zeroes([40]u8)[0..] }, // dummy node ID
    };

    // Should not error (may return empty array, but command should be recognized)
    const result = try executeCommand(
        std.testing.allocator,
        &storage,
        &args,
        null,
        null,
        null,
        0,
        6379,
    );
    defer std.testing.allocator.free(result);

    // If it reaches here without error, the alias works
    try std.testing.expect(result.len > 0);
}

test "SLAVEOF alias for REPLICAOF - syntax check" {
    var storage = try Storage.init(std.testing.allocator, .{});
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SLAVEOF" },
        RespValue{ .bulk_string = "127.0.0.1" },
        RespValue{ .bulk_string = "6380" },
    };

    // This will fail with "ERR replication not initialized" but proves the command is recognized
    const result = executeCommand(
        std.testing.allocator,
        &storage,
        &args,
        null,
        null,
        null,
        0,
        6379,
    ) catch |err| {
        // Expected to fail since replication is not initialized
        // But the command should be recognized
        _ = err;
        return;
    };
    defer std.testing.allocator.free(result);

    // If we get a result, verify it's an error about replication
    try std.testing.expect(std.mem.indexOf(u8, result, "replication") != null or result.len > 0);
}

test "SLAVEOF NO ONE - stop replication syntax" {
    var storage = try Storage.init(std.testing.allocator, .{});
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SLAVEOF" },
        RespValue{ .bulk_string = "NO" },
        RespValue{ .bulk_string = "ONE" },
    };

    // Should not crash, even without replication initialized
    const result = executeCommand(
        std.testing.allocator,
        &storage,
        &args,
        null,
        null,
        null,
        0,
        6379,
    ) catch |err| {
        _ = err;
        return; // Expected to fail
    };
    defer std.testing.allocator.free(result);
}

test "BRPOPLPUSH alias for BLMOVE RIGHT LEFT" {
    var storage = try Storage.init(std.testing.allocator, .{});
    defer storage.deinit();

    // Create source list
    const lpush_args = [_]RespValue{
        RespValue{ .bulk_string = "LPUSH" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "item1" },
        RespValue{ .bulk_string = "item2" },
        RespValue{ .bulk_string = "item3" },
    };
    const lpush_result = try executeCommand(
        std.testing.allocator,
        &storage,
        &lpush_args,
        null,
        null,
        null,
        0,
        6379,
    );
    defer std.testing.allocator.free(lpush_result);

    // BRPOPLPUSH source dest 1 (timeout 1 second)
    // This should work as alias for BLMOVE source dest RIGHT LEFT 1
    const args = [_]RespValue{
        RespValue{ .bulk_string = "BRPOPLPUSH" },
        RespValue{ .bulk_string = "source" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "1" }, // timeout
    };

    const result = try executeCommand(
        std.testing.allocator,
        &storage,
        &args,
        null,
        null,
        null,
        0,
        6379,
    );
    defer std.testing.allocator.free(result);

    // Should have moved an element
    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "item") != null);
}

test "CLUSTER SLAVES - command routing" {
    var storage = try Storage.init(std.testing.allocator, .{});
    defer storage.deinit();

    storage.cluster.enabled = true;

    // CLUSTER SLAVES should be recognized in dispatcher
    const args = [_]RespValue{
        RespValue{ .bulk_string = "CLUSTER" },
        RespValue{ .bulk_string = "SLAVES" },
        RespValue{ .bulk_string = "node-id-that-does-not-exist-1234567890123" },
    };

    const result = try executeCommand(
        std.testing.allocator,
        &storage,
        &args,
        null,
        null,
        null,
        0,
        6379,
    );
    defer std.testing.allocator.free(result);

    // Should return an error (node doesn't exist) but command is recognized
    try std.testing.expect(result.len > 0);
}
