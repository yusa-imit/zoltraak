const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const RespValue = protocol.RespValue;
const strings_mod = @import("../src/commands/strings.zig");
const Storage = @import("../src/storage/memory.zig").Storage;
const PubSub = @import("../src/storage/pubsub.zig").PubSub;
const Aof = @import("../src/storage/aof.zig").Aof;
const TxState = @import("../src/commands/transactions.zig").TxState;
const ReplicationState = @import("../src/storage/replication.zig").ReplicationState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;
const ScriptStore = @import("../src/storage/scripting.zig").ScriptStore;

fn executeCommand(
    allocator: std.mem.Allocator,
    storage: *Storage,
    cmd: RespValue,
    client_registry: *ClientRegistry,
    client_id: u64,
) ![]const u8 {
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    return try strings_mod.executeCommand(
        allocator,
        storage,
        cmd,
        null, // aof
        &pubsub,
        0, // subscriber_id
        &tx,
        null, // repl
        6379, // my_port
        null, // replica_stream
        null, // replica_idx
        client_registry,
        client_id,
        &script_store,
    );
}

test "MONITOR - enables monitor mode" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    // Monitor mode should be off initially
    try std.testing.expect(!client_registry.isMonitoring(client_id));

    // Execute MONITOR command
    const cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "MONITOR" },
    } };

    const result = try executeCommand(allocator, &storage, cmd, &client_registry, client_id);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Monitor mode should now be enabled
    try std.testing.expect(client_registry.isMonitoring(client_id));
}

test "MONITOR - broadcasts commands to monitoring clients" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Register two clients
    const client1 = try client_registry.registerClient("127.0.0.1:1", 10);
    const client2 = try client_registry.registerClient("127.0.0.1:2", 11);

    // Enable monitoring for client2
    client_registry.setMonitorMode(client2, true);

    // Execute a SET command from client1
    const cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "testkey" },
        RespValue{ .bulk_string = "testvalue" },
    } };

    const result = try executeCommand(allocator, &storage, cmd, &client_registry, client1);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify the broadcast (this tests the broadcast mechanism itself)
    const cmd_args = [_][]const u8{ "SET", "testkey", "testvalue" };
    const messages = try client_registry.broadcastToMonitors(
        allocator,
        1234567890,
        123456,
        0,
        "127.0.0.1:54321",
        &cmd_args,
    );
    defer {
        for (messages.items) |msg| {
            allocator.free(msg.message);
        }
        messages.deinit(allocator);
    }

    // Should have 1 message for client2
    try std.testing.expectEqual(@as(usize, 1), messages.items.len);
    try std.testing.expectEqual(client2, messages.items[0].client_id);

    // Message should contain the command
    const msg = messages.items[0].message;
    try std.testing.expect(std.mem.indexOf(u8, msg, "\"SET\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "\"testkey\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "\"testvalue\"") != null);
}

test "MONITOR - does not broadcast MONITOR command itself" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const client1 = try client_registry.registerClient("127.0.0.1:1", 10);
    const client2 = try client_registry.registerClient("127.0.0.1:2", 11);

    // Enable monitoring for both clients
    client_registry.setMonitorMode(client1, true);
    client_registry.setMonitorMode(client2, true);

    // Get initial monitoring client count
    const monitors_before = try client_registry.getMonitoringClients(allocator);
    defer allocator.free(monitors_before);
    try std.testing.expectEqual(@as(usize, 2), monitors_before.len);

    // Execute MONITOR command (should not be broadcast)
    const cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "MONITOR" },
    } };

    const result = try executeCommand(allocator, &storage, cmd, &client_registry, client1);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Both clients should still be monitoring
    const monitors_after = try client_registry.getMonitoringClients(allocator);
    defer allocator.free(monitors_after);
    try std.testing.expectEqual(@as(usize, 2), monitors_after.len);
}

test "MONITOR - multiple monitoring clients receive broadcasts" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Register three clients
    const client1 = try client_registry.registerClient("127.0.0.1:1", 10);
    const client2 = try client_registry.registerClient("127.0.0.1:2", 11);
    const client3 = try client_registry.registerClient("127.0.0.1:3", 12);

    // Enable monitoring for client2 and client3
    client_registry.setMonitorMode(client2, true);
    client_registry.setMonitorMode(client3, true);

    // Broadcast a command
    const cmd_args = [_][]const u8{ "DEL", "key1", "key2" };
    const messages = try client_registry.broadcastToMonitors(
        allocator,
        1234567890,
        123456,
        0,
        "127.0.0.1:54321",
        &cmd_args,
    );
    defer {
        for (messages.items) |msg| {
            allocator.free(msg.message);
        }
        messages.deinit(allocator);
    }

    // Should have 2 messages (client2 and client3)
    try std.testing.expectEqual(@as(usize, 2), messages.items.len);

    // Both messages should be identical
    try std.testing.expectEqualStrings(messages.items[0].message, messages.items[1].message);

    // Message should contain the command
    const msg = messages.items[0].message;
    try std.testing.expect(std.mem.indexOf(u8, msg, "\"DEL\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "\"key1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "\"key2\"") != null);
}

test "MONITOR - timestamp format" {
    const allocator = std.testing.allocator;
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);
    client_registry.setMonitorMode(client_id, true);

    const cmd_args = [_][]const u8{"PING"};
    const messages = try client_registry.broadcastToMonitors(
        allocator,
        1234567890,
        123456,
        0,
        "127.0.0.1:54321",
        &cmd_args,
    );
    defer {
        for (messages.items) |msg| {
            allocator.free(msg.message);
        }
        messages.deinit(allocator);
    }

    // Check timestamp format: +seconds.microseconds
    const msg = messages.items[0].message;
    try std.testing.expect(std.mem.startsWith(u8, msg, "+1234567890.123456"));
}
