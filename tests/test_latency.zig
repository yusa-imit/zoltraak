const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const executeCommand = @import("../src/commands/strings.zig").executeCommand;
const PubSub = @import("../src/storage/pubsub.zig").PubSub;
const TxState = @import("../src/commands/transactions.zig").TxState;
const ClientRegistry = @import("../src/storage/client.zig").ClientRegistry;
const ScriptStore = @import("../src/storage/scripting.zig").ScriptStore;

/// Helper to execute LATENCY commands
fn execLatency(
    allocator: std.mem.Allocator,
    storage: *Storage,
    ps: *PubSub,
    tx: *TxState,
    client_registry: *ClientRegistry,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var array_items = try allocator.alloc(RespValue, args.len + 1);
    defer allocator.free(array_items);

    array_items[0] = RespValue{ .bulk_string = "LATENCY" };
    for (args, 0..) |arg, i| {
        array_items[i + 1] = RespValue{ .bulk_string = arg };
    }

    const cmd = RespValue{ .array = array_items };
    return try executeCommand(
        allocator,
        storage,
        cmd,
        null, // aof
        ps,
        0, // subscriber_id
        tx,
        null, // repl
        6379,
        null, // replica_stream
        null, // replica_idx
        client_registry,
        1, // client_id
        script_store,
    );
}

test "LATENCY LATEST - no events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    const args = [_][]const u8{"LATEST"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "LATENCY LATEST - with events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record some events
    try storage.latency_monitor.recordEvent(.command, 1000);
    try storage.latency_monitor.recordEvent(.fork, 5000);

    const args = [_][]const u8{"LATEST"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "command") != null or std.mem.indexOf(u8, result, "fork") != null);
}

test "LATENCY HISTORY - specific event" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record events
    try storage.latency_monitor.recordEvent(.aof_write, 500);
    try storage.latency_monitor.recordEvent(.aof_write, 1500);

    const args = [_][]const u8{ "HISTORY", "aof-write" };
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":500\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":1500\r\n") != null);
}

test "LATENCY HISTORY - invalid event" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    const args = [_][]const u8{ "HISTORY", "invalid-event" };
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "LATENCY RESET - specific events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record events
    try storage.latency_monitor.recordEvent(.command, 1000);
    try storage.latency_monitor.recordEvent(.fork, 5000);

    const args = [_][]const u8{ "RESET", "command" };
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "LATENCY RESET - all events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record events
    try storage.latency_monitor.recordEvent(.command, 1000);
    try storage.latency_monitor.recordEvent(.fork, 5000);

    const args = [_][]const u8{"RESET"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings(":11\r\n", result);
}

test "LATENCY GRAPH - event with data" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record events
    try storage.latency_monitor.recordEvent(.fork, 5000);
    try storage.latency_monitor.recordEvent(.fork, 3000);

    const args = [_][]const u8{ "GRAPH", "fork" };
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "fork") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "5000us") != null or std.mem.indexOf(u8, result, "3000us") != null);
}

test "LATENCY HISTOGRAM - command histograms" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record command latencies
    try storage.latency_monitor.recordCommandLatency("GET", 100);
    try storage.latency_monitor.recordCommandLatency("GET", 200);
    try storage.latency_monitor.recordCommandLatency("SET", 150);

    const args = [_][]const u8{"HISTOGRAM"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "GET") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "SET") != null);
}

test "LATENCY DOCTOR - no events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    const args = [_][]const u8{"DOCTOR"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Latency Analysis") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "No latency events") != null);
}

test "LATENCY DOCTOR - high latency detected" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // Record high latency event (>10ms = 10000us)
    try storage.latency_monitor.recordEvent(.aof_fsync_always, 50000); // 50ms

    const args = [_][]const u8{"DOCTOR"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WARNING") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "High latency") != null);
}

test "LATENCY HELP" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    const args = [_][]const u8{"HELP"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "LATENCY LATEST") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "LATENCY HELP") != null);
}

test "LATENCY - wrong number of arguments" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    // LATENCY with no arguments
    const args = [_][]const u8{};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "LATENCY - unknown subcommand" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var ps = PubSub.init(std.testing.allocator);
    defer ps.deinit();

    var tx = TxState.init(std.testing.allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(std.testing.allocator);
    defer client_registry.deinit();

    var script_store = ScriptStore.init(std.testing.allocator);
    defer script_store.deinit();

    const args = [_][]const u8{"INVALID"};
    const result = try execLatency(
        std.testing.allocator,
        &storage,
        &ps,
        &tx,
        &client_registry,
        &script_store,
        &args,
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "unknown") != null);
}
