const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const PubSub = zoltraak.pubsub.PubSub;
const ClientRegistry = zoltraak.ClientRegistry;
const TxState = zoltraak.transactions_commands.TxState;
const utility_cmds = zoltraak.utility_commands;

// Iteration 409: RESET command was missing three documented Redis behaviors —
// it left the connection authenticated, left CLIENT REPLY SKIP/OFF active,
// and left MONITOR mode on. Per https://redis.io/commands/reset/, RESET must
// force re-AUTH, restore CLIENT REPLY to ON, and turn off MONITOR.

test "iter409 - RESET deauthenticates a previously authenticated client" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");
    try client_registry.setAuthenticatedUser(client_id, "admin");
    try testing.expect(client_registry.isAuthenticated(client_id));

    const args = [_][]const u8{"RESET"};
    const result = try utility_cmds.cmdReset(allocator, &args, storage, &pubsub, &tx_state, &client_registry, client_id, storage.config, 2);
    defer allocator.free(result);

    try testing.expectEqualStrings("+RESET\r\n", result);
    try testing.expect(!client_registry.isAuthenticated(client_id));
}

test "iter409 - RESET restores CLIENT REPLY to ON from OFF" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");
    client_registry.setReplyMode(client_id, .OFF);
    try testing.expect(client_registry.getReplyMode(client_id) == .OFF);

    const args = [_][]const u8{"RESET"};
    const result = try utility_cmds.cmdReset(allocator, &args, storage, &pubsub, &tx_state, &client_registry, client_id, storage.config, 2);
    defer allocator.free(result);

    try testing.expect(client_registry.getReplyMode(client_id) == .ON);
}

test "iter409 - RESET restores CLIENT REPLY to ON from SKIP" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");
    client_registry.setReplyMode(client_id, .SKIP);

    const args = [_][]const u8{"RESET"};
    const result = try utility_cmds.cmdReset(allocator, &args, storage, &pubsub, &tx_state, &client_registry, client_id, storage.config, 2);
    defer allocator.free(result);

    try testing.expect(client_registry.getReplyMode(client_id) == .ON);
}

test "iter409 - RESET turns off MONITOR mode" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");
    client_registry.setMonitorMode(client_id, true);
    try testing.expect(client_registry.isMonitoring(client_id));

    const args = [_][]const u8{"RESET"};
    const result = try utility_cmds.cmdReset(allocator, &args, storage, &pubsub, &tx_state, &client_registry, client_id, storage.config, 2);
    defer allocator.free(result);

    try testing.expect(!client_registry.isMonitoring(client_id));
}

test "iter409 - RESET on a never-authenticated, default-state client is a no-op for these fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    const client_id = try client_registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");

    const args = [_][]const u8{"RESET"};
    const result = try utility_cmds.cmdReset(allocator, &args, storage, &pubsub, &tx_state, &client_registry, client_id, storage.config, 2);
    defer allocator.free(result);

    try testing.expectEqualStrings("+RESET\r\n", result);
    try testing.expect(!client_registry.isAuthenticated(client_id));
    try testing.expect(client_registry.getReplyMode(client_id) == .ON);
    try testing.expect(!client_registry.isMonitoring(client_id));
}
