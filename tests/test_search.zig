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

test "FT.INFO: basic index info" {
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

    // Get info
    const cmd2 = "FT.INFO myindex\r\n";
    const parsed2 = try parseCommand(allocator, cmd2);
    defer parsed2.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    // Verify response contains expected fields
    try std.testing.expect(std.mem.indexOf(u8, result2, "index_name") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "myindex") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "index_definition") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "HASH") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "attributes") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "num_docs") != null);
}

test "FT.INFO: nonexistent index" {
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

    const cmd = "FT.INFO nonexistent\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown index name") != null);
}

test "FT.INFO: JSON index with prefix" {
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

    // Create JSON index with prefix
    const cmd1 = "FT.CREATE products ON JSON PREFIX 1 product: SCHEMA $.title TEXT $.price NUMERIC\r\n";
    const parsed1 = try parseCommand(allocator, cmd1);
    defer parsed1.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    // Get info
    const cmd2 = "FT.INFO products\r\n";
    const parsed2 = try parseCommand(allocator, cmd2);
    defer parsed2.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    // Verify JSON type and prefix
    try std.testing.expect(std.mem.indexOf(u8, result2, "JSON") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "product:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "prefixes") != null);
}

test "FT.INFO: multiple fields with options" {
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

    // Create index with multiple fields and options
    const cmd1 = "FT.CREATE myindex ON HASH SCHEMA title TEXT SORTABLE NOSTEM description TEXT price NUMERIC SORTABLE tags TAG\r\n";
    const parsed1 = try parseCommand(allocator, cmd1);
    defer parsed1.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    // Get info
    const cmd2 = "FT.INFO myindex\r\n";
    const parsed2 = try parseCommand(allocator, cmd2);
    defer parsed2.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    // Verify field types appear
    try std.testing.expect(std.mem.indexOf(u8, result2, "TEXT") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "NUMERIC") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "TAG") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "SORTABLE") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "NOSTEM") != null);
}

test "FT.INFO: arity validation" {
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

    // Missing index name
    const cmd = "FT.INFO\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "FT.ALTER: add field to existing index" {
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

    // Create index first
    const create_cmd = "FT.CREATE myindex ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    // Alter index: add new field
    const alter_cmd = "FT.ALTER myindex SCHEMA ADD price NUMERIC\r\n";
    const parsed_alter = try parseCommand(allocator, alter_cmd);
    defer parsed_alter.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed_alter.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "+OK") != null);
}

test "FT.ALTER: add field with SORTABLE" {
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

    const create_cmd = "FT.CREATE idx ON JSON SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const alter_cmd = "FT.ALTER idx SCHEMA ADD age NUMERIC SORTABLE\r\n";
    const parsed_alter = try parseCommand(allocator, alter_cmd);
    defer parsed_alter.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed_alter.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "+OK") != null);
}

test "FT.ALTER: add field with AS alias" {
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

    const create_cmd = "FT.CREATE idx ON HASH SCHEMA f1 TAG\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const alter_cmd = "FT.ALTER idx SCHEMA ADD full_name TEXT AS name\r\n";
    const parsed_alter = try parseCommand(allocator, alter_cmd);
    defer parsed_alter.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed_alter.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "+OK") != null);
}

test "FT.ALTER: error on nonexistent index" {
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

    const cmd = "FT.ALTER nonexistent SCHEMA ADD field TEXT\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown index name") != null);
}

test "FT.ALTER: error on wrong arity" {
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

    const cmd = "FT.ALTER idx SCHEMA\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "FT.ALTER: error on missing SCHEMA keyword" {
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

    const create_cmd = "FT.CREATE idx ON HASH SCHEMA f1 TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const cmd = "FT.ALTER idx ADD field TEXT\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "expected SCHEMA") != null);
}

test "FT.ALTER: error on missing ADD keyword" {
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

    const create_cmd = "FT.CREATE idx ON HASH SCHEMA f1 TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const cmd = "FT.ALTER idx SCHEMA field TEXT\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "expected ADD") != null);
}

test "FT.ALTER: error on invalid field type" {
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

    const create_cmd = "FT.CREATE idx ON HASH SCHEMA f1 TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const cmd = "FT.ALTER idx SCHEMA ADD field INVALID\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "invalid field type") != null);
}

test "FT.ALTER: add multiple options" {
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

    const create_cmd = "FT.CREATE idx ON JSON SCHEMA f1 TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    const alter_cmd = "FT.ALTER idx SCHEMA ADD f2 TEXT SORTABLE NOINDEX NOSTEM\r\n";
    const parsed_alter = try parseCommand(allocator, alter_cmd);
    defer parsed_alter.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed_alter.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.indexOf(u8, result2, "+OK") != null);
}

test "FT.SEARCH: basic search empty results" {
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
    const create_cmd = "FT.CREATE myindex ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const result1 = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result1);

    // Search (should return 0 results since stub implementation)
    const search_cmd = "FT.SEARCH myindex hello\r\n";
    const parsed_search = try parseCommand(allocator, search_cmd);
    defer parsed_search.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result2 = try processCommand(arena2.allocator(), parsed_search.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result2);

    // Should return array with count 0
    try std.testing.expect(std.mem.indexOf(u8, result2, "*1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, ":0") != null);
}

test "FT.SEARCH: error on nonexistent index" {
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

    // Search on non-existent index
    const search_cmd = "FT.SEARCH nonexistent hello\r\n";
    const parsed_search = try parseCommand(allocator, search_cmd);
    defer parsed_search.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_search.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown index name") != null);
}

test "FT.SEARCH: error on wrong arity" {
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

    // Search with missing query
    const search_cmd = "FT.SEARCH myindex\r\n";
    const parsed_search = try parseCommand(allocator, search_cmd);
    defer parsed_search.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_search.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

// FT.EXPLAIN tests
test "FT.EXPLAIN: basic query explanation" {
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
    const create_cmd = "FT.CREATE myindex ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain query
    const explain_cmd = "FT.EXPLAIN myindex hello\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "hello") != null);
}

test "FT.EXPLAIN: multi-word query" {
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
    const create_cmd = "FT.CREATE idx ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain multi-word query
    const explain_cmd = "FT.EXPLAIN idx hello world\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "hello world") != null);
}

test "FT.EXPLAIN: with DIALECT argument" {
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
    const create_cmd = "FT.CREATE idx2 ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with DIALECT 2
    const explain_cmd = "FT.EXPLAIN idx2 test DIALECT 2\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "test") != null);
}

test "FT.EXPLAIN: error on nonexistent index" {
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

    // Explain on nonexistent index
    const explain_cmd = "FT.EXPLAIN nonexistent hello\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown index name") != null);
}

test "FT.EXPLAIN: error on missing query" {
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

    // Explain with missing query argument
    const explain_cmd = "FT.EXPLAIN myindex\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "FT.EXPLAIN: error on invalid DIALECT" {
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
    const create_cmd = "FT.CREATE idx3 ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with invalid DIALECT value
    const explain_cmd = "FT.EXPLAIN idx3 hello DIALECT abc\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "must be an integer") != null);
}

test "FT.EXPLAIN: error on too many arguments" {
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
    const create_cmd = "FT.CREATE idx4 ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with too many arguments
    const explain_cmd = "FT.EXPLAIN idx4 hello DIALECT 1 extra\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "FT.EXPLAIN: error on missing DIALECT value" {
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
    const create_cmd = "FT.CREATE idx5 ON HASH SCHEMA title TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with DIALECT but no value
    const explain_cmd = "FT.EXPLAIN idx5 hello DIALECT\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "requires an integer argument") != null);
}
