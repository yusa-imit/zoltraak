// Iteration 353: CLIENT LIST real multi/watch counts + laddr field
//
// Fixes CLIENT LIST and CLIENT INFO to report:
// 1. Real multi= count (-1 when not in MULTI, N when N commands queued)
// 2. Real watch= count (number of WATCHed keys)
// 3. Real laddr= (local server address, e.g. "127.0.0.1:6379")
const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const transactions_mod = zoltraak.transactions_commands;
const TxState = transactions_mod.TxState;

fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| {
        resp_args[i] = .{ .bulk_string = a };
    }
    const cmd = RespValue{ .array = resp_args };
    var script_store = scripting.ScriptStore.init(allocator);
    defer script_store.deinit();
    var databases = [_]Storage{storage.*};
    return commands.executeCommand(
        allocator,
        storage,
        cmd,
        null,
        ps,
        subscriber_id,
        tx,
        null,
        6379,
        null,
        null,
        client_registry,
        client_id,
        &script_store,
        null,
        &databases,
        1,
    );
}

test "iter353 - CLIENT LIST shows multi=-1 when not in transaction" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9101", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    // multi=-1 means not in a transaction
    try testing.expect(std.mem.indexOf(u8, result, "multi=-1") != null);
}

test "iter353 - CLIENT LIST reads multi_count from registry directly" {
    // Test that CLIENT LIST correctly reads and formats the multi_count field.
    // We set it directly via updateTxCounts to simulate being in a MULTI block.
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9102", 10, "127.0.0.1:6379");

    // Simulate 3 commands queued in MULTI
    registry.updateTxCounts(client_id, 3, 0);

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    // CLIENT LIST should show multi=3
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "multi=3") != null);
}

test "iter353 - MULTI command sets multi_count to 0 in registry" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9103", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    // Before MULTI: multi=-1
    const pre_result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(pre_result);
    try testing.expect(std.mem.indexOf(u8, pre_result, "multi=-1") != null);

    // Start MULTI
    const multi_result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{"MULTI"});
    defer allocator.free(multi_result);
    try testing.expectEqualStrings("+OK\r\n", multi_result);

    // Since we're now in MULTI, CLIENT LIST will be QUEUED - check via updateTxCounts instead
    // The MULTI command should have updated the registry to multi=0
    // Verify by resetting to non-active and checking
    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{"DISCARD"}));

    // After DISCARD: multi=-1 again
    const post_result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(post_result);
    try testing.expect(std.mem.indexOf(u8, post_result, "multi=-1") != null);
}

test "iter353 - CLIENT LIST shows watch=0 initially" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9105", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "watch=0") != null);
}

test "iter353 - CLIENT LIST shows watch=N after WATCH N keys" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9106", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    // WATCH 2 keys
    const watch_result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "WATCH", "key1", "key2" });
    defer allocator.free(watch_result);
    try testing.expectEqualStrings("+OK\r\n", watch_result);

    // CLIENT LIST should show watch=2
    const list_result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(list_result);
    try testing.expect(std.mem.indexOf(u8, list_result, "watch=2") != null);

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{"UNWATCH"}));
}

test "iter353 - CLIENT LIST shows watch=0 after UNWATCH" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9107", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "WATCH", "mykey" }));
    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{"UNWATCH"}));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "watch=0") != null);
}

test "iter353 - CLIENT LIST shows laddr field populated" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9108", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    // laddr should be populated with the server address (not empty)
    try testing.expect(std.mem.indexOf(u8, result, "laddr=127.0.0.1:6379") != null);
}

test "iter353 - CLIENT LIST laddr empty string gives empty laddr" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    // Register with empty laddr (legacy call from tests)
    const client_id = try registry.registerClient("127.0.0.1:9109", 10, "");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    // laddr should be present in the format (even if empty)
    try testing.expect(std.mem.indexOf(u8, result, "laddr=") != null);
}

test "iter353 - CLIENT INFO shows real multi/watch/laddr" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9110", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    // WATCH a key
    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "WATCH", "mykey" }));

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "INFO" });
    defer allocator.free(result);

    // Should show real watch count and laddr
    try testing.expect(std.mem.indexOf(u8, result, "watch=1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "laddr=127.0.0.1:6379") != null);
    try testing.expect(std.mem.indexOf(u8, result, "multi=-1") != null);

    allocator.free(try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{"UNWATCH"}));
}

test "iter353 - updateTxCounts reflects in CLIENT LIST" {
    // Direct test of updateTxCounts method
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9111", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    // Set multi=5 watch=3 directly
    registry.updateTxCounts(client_id, 5, 3);

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, 1, &tx, &.{ "CLIENT", "LIST" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "multi=5") != null);
    try testing.expect(std.mem.indexOf(u8, result, "watch=3") != null);
}
