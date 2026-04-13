const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const parseCommand = @import("../src/protocol/parser.zig").parseCommand;
const processCommand = @import("../src/commands/strings.zig").processCommand;
const TxState = @import("../src/commands/strings.zig").TxState;
const ReplicationState = @import("../src/commands/strings.zig").ReplicationState;
const ScriptStore = @import("../src/commands/strings.zig").ScriptStore;
const PubSubState = @import("../src/storage/pubsub.zig").PubSubState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;

test "FT.CREATE: basic index creation" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const cmd = "FT.CREATE myindex ON HASH SCHEMA title TEXT price NUMERIC\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);
}

test "FT.CREATE: duplicate index error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Create first index
    const cmd1 = "FT.CREATE myindex ON HASH SCHEMA title TEXT\r\n";
    const parsed1 = try parseCommand(allocator, cmd1);
    defer parsed1.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    // Try to create duplicate
    const cmd2 = "FT.CREATE myindex ON HASH SCHEMA title TEXT\r\n";
    const parsed2 = try parseCommand(allocator, cmd2);
    defer parsed2.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "Index already exists") != null);
}

test "FT.CREATE: with PREFIX" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const cmd = "FT.CREATE myindex ON HASH PREFIX 1 product: SCHEMA title TEXT\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // Verify prefix was set
    storage.mutex.lock();
    defer storage.mutex.unlock();

    const index = storage.search.getIndex("myindex").?;
    try std.testing.expect(index.prefix != null);
    try std.testing.expectEqualStrings("product:", index.prefix.?);
}

test "FT.CREATE: multiple fields with options" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const cmd = "FT.CREATE myindex ON HASH SCHEMA title TEXT SORTABLE price NUMERIC SORTABLE\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // Verify fields
    storage.mutex.lock();
    defer storage.mutex.unlock();

    const index = storage.search.getIndex("myindex").?;
    try std.testing.expectEqual(@as(usize, 2), index.fields.items.len);
    try std.testing.expectEqualStrings("title", index.fields.items[0].name);
    try std.testing.expect(index.fields.items[0].sortable);
    try std.testing.expectEqualStrings("price", index.fields.items[1].name);
    try std.testing.expect(index.fields.items[1].sortable);
}

test "FT.CREATE: JSON index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const cmd = "FT.CREATE myindex ON JSON SCHEMA $.title TEXT\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);
}

test "FT._LIST: empty list" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const cmd = "FT._LIST\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*0") != null);
}

test "FT._LIST: list indices" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Create two indices
    const cmd1 = "FT.CREATE idx1 ON HASH SCHEMA title TEXT\r\n";
    const parsed1 = try parseCommand(allocator, cmd1);
    defer parsed1.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const cmd2 = "FT.CREATE idx2 ON JSON SCHEMA $.title TEXT\r\n";
    const parsed2 = try parseCommand(allocator, cmd2);
    defer parsed2.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    // List indices
    const cmd3 = "FT._LIST\r\n";
    const parsed3 = try parseCommand(allocator, cmd3);
    defer parsed3.deinit(allocator);

    var arena3 = std.heap.ArenaAllocator.init(allocator);
    defer arena3.deinit();

    const result3 = try processCommand(arena3.allocator(), parsed3.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result3);

    try std.testing.expect(std.mem.indexOf(u8, result3, "*2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result3, "idx1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result3, "idx2") != null);
}

test "FT.DROPINDEX: basic drop" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Create index
    const cmd1 = "FT.CREATE myindex ON HASH SCHEMA title TEXT\r\n";
    const parsed1 = try parseCommand(allocator, cmd1);
    defer parsed1.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    // Drop index
    const cmd2 = "FT.DROPINDEX myindex\r\n";
    const parsed2 = try parseCommand(allocator, cmd2);
    defer parsed2.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "+OK") != null);
}

test "FT.DROPINDEX: nonexistent index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const cmd = "FT.DROPINDEX nonexistent\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown Index name") != null);
}
