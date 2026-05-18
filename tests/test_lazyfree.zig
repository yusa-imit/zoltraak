const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const protocol = @import("../src/protocol/parser.zig");
const RespValue = protocol.RespValue;
const strings_cmds = @import("../src/commands/strings.zig");
const keys_cmds = @import("../src/commands/keys.zig");

test "FLUSHALL ASYNC submits work to background thread" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add some test data
    _ = try storage.set("key1", "value1", null);
    _ = try storage.set("key2", "value2", null);
    _ = try storage.set("key3", "value3", null);

    try std.testing.expectEqual(@as(usize, 3), storage.data.count());

    // Create FLUSHALL ASYNC command
    const args = [_]RespValue{
        RespValue{ .bulk_string = "FLUSHALL" },
        RespValue{ .bulk_string = "ASYNC" },
    };

    const result = try strings_cmds.executeCommand(
        allocator,
        storage,
        null, // pubsub
        null, // repl
        null, // client_registry
        0, // client_id
        0, // my_port
        null, // databases
        0, // num_databases
        &args,
    );
    defer allocator.free(result);

    // Verify response is OK
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // HashMap should be cleared immediately
    try std.testing.expectEqual(@as(usize, 0), storage.data.count());
}

test "FLUSHALL SYNC is synchronous" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add some test data
    _ = try storage.set("key1", "value1", null);
    _ = try storage.set("key2", "value2", null);

    try std.testing.expectEqual(@as(usize, 2), storage.data.count());

    // Create FLUSHALL SYNC command
    const args = [_]RespValue{
        RespValue{ .bulk_string = "FLUSHALL" },
        RespValue{ .bulk_string = "SYNC" },
    };

    const result = try strings_cmds.executeCommand(
        allocator,
        storage,
        null,
        null,
        null,
        0,
        0,
        null,
        0,
        &args,
    );
    defer allocator.free(result);

    // Verify response is OK
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // HashMap should be cleared
    try std.testing.expectEqual(@as(usize, 0), storage.data.count());
}

test "FLUSHALL without argument defaults to SYNC" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add some test data
    _ = try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "FLUSHALL" },
    };

    const result = try strings_cmds.executeCommand(
        allocator,
        storage,
        null,
        null,
        null,
        0,
        0,
        null,
        0,
        &args,
    );
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);
    try std.testing.expectEqual(@as(usize, 0), storage.data.count());
}

test "FLUSHALL rejects invalid mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "FLUSHALL" },
        RespValue{ .bulk_string = "INVALID" },
    };

    const result = try strings_cmds.executeCommand(
        allocator,
        storage,
        null,
        null,
        null,
        0,
        0,
        null,
        0,
        &args,
    );
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR syntax error") != null);
}

test "FLUSHALL rejects too many arguments" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "FLUSHALL" },
        RespValue{ .bulk_string = "ASYNC" },
        RespValue{ .bulk_string = "EXTRA" },
    };

    const result = try strings_cmds.executeCommand(
        allocator,
        storage,
        null,
        null,
        null,
        0,
        0,
        null,
        0,
        &args,
    );
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR syntax error") != null);
}

test "UNLINK schedules async deletion" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add test data
    _ = try storage.set("key1", "value1", null);
    _ = try storage.set("key2", "value2", null);
    _ = try storage.set("key3", "value3", null);

    try std.testing.expectEqual(@as(usize, 3), storage.data.count());

    // UNLINK keys
    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNLINK" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
    };

    const result = try keys_cmds.cmdUnlink(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 2 (number of deleted keys)
    try std.testing.expect(std.mem.indexOf(u8, result, ":2") != null);

    // Keys should be immediately removed from HashMap
    try std.testing.expectEqual(@as(usize, 1), storage.data.count());
    try std.testing.expect(storage.get("key3") != null);
}

test "UNLINK returns 0 for nonexistent keys" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNLINK" },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try keys_cmds.cmdUnlink(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, ":0") != null);
}

test "UNLINK multiple keys with some nonexistent" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.set("exists1", "value1", null);
    _ = try storage.set("exists2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNLINK" },
        RespValue{ .bulk_string = "exists1" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "exists2" },
    };

    const result = try keys_cmds.cmdUnlink(allocator, storage, &args);
    defer allocator.free(result);

    // Should return 2 (only existing keys)
    try std.testing.expect(std.mem.indexOf(u8, result, ":2") != null);
    try std.testing.expectEqual(@as(usize, 0), storage.data.count());
}

test "CONFIG SET lazyfree-lazy-eviction" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set lazyfree-lazy-eviction to yes
    try storage.config.set("lazyfree-lazy-eviction", .{ .bool = true });

    // Get the value back
    const val = try storage.config.get("lazyfree-lazy-eviction");
    try std.testing.expectEqual(true, val.bool);
}

test "CONFIG SET all lazyfree options" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set all lazy free options
    try storage.config.set("lazyfree-lazy-eviction", .{ .bool = true });
    try storage.config.set("lazyfree-lazy-expire", .{ .bool = true });
    try storage.config.set("lazyfree-lazy-server-del", .{ .bool = true });
    try storage.config.set("lazyfree-lazy-user-del", .{ .bool = false });
    try storage.config.set("replica-lazy-flush", .{ .bool = true });

    // Verify all values
    try std.testing.expectEqual(true, (try storage.config.get("lazyfree-lazy-eviction")).bool);
    try std.testing.expectEqual(true, (try storage.config.get("lazyfree-lazy-expire")).bool);
    try std.testing.expectEqual(true, (try storage.config.get("lazyfree-lazy-server-del")).bool);
    try std.testing.expectEqual(false, (try storage.config.get("lazyfree-lazy-user-del")).bool);
    try std.testing.expectEqual(true, (try storage.config.get("replica-lazy-flush")).bool);
}

test "INFO stats includes lazyfree_pending_objects" {
    const info_cmds = @import("../src/commands/info.zig");
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "INFO" },
        RespValue{ .bulk_string = "stats" },
    };

    const result = try info_cmds.cmdInfo(
        allocator,
        storage,
        null, // config
        null, // repl
        null, // client_registry
        0, // client_count
        0, // total_commands_processed
        0, // total_connections_received
        0, // start_time_seconds
        &args,
    );
    defer allocator.free(result);

    // Verify field exists
    try std.testing.expect(std.mem.indexOf(u8, result, "lazyfree_pending_objects:") != null);
}

test "lazyfree_pending_objects updates after UNLINK" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create a key with a larger value to ensure it's queued
    const large_value = try allocator.alloc(u8, 1000);
    defer allocator.free(large_value);
    @memset(large_value, 'x');

    _ = try storage.set("key1", large_value, null);

    // Check initial count
    try std.testing.expectEqual(@as(usize, 0), storage.lazyfree_task.getPendingCount());

    // UNLINK key
    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNLINK" },
        RespValue{ .bulk_string = "key1" },
    };
    const result = try keys_cmds.cmdUnlink(allocator, storage, &args);
    defer allocator.free(result);

    // Verify count increased (key should be in background queue)
    try std.testing.expect(storage.lazyfree_task.getPendingCount() > 0);
}

test "lazyfree_pending_objects shows 0 when queue empty" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // No keys, no async operations
    try std.testing.expectEqual(@as(usize, 0), storage.lazyfree_task.getPendingCount());
}
