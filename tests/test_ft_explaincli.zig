const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const parseCommand = @import("../src/protocol/parser.zig").parseCommand;
const processCommand = @import("../src/commands/strings.zig").processCommand;
const TxState = @import("../src/commands/strings.zig").TxState;
const ReplicationState = @import("../src/commands/strings.zig").ReplicationState;
const ScriptStore = @import("../src/commands/strings.zig").ScriptStore;
const PubSubState = @import("../src/storage/pubsub.zig").PubSubState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;

test "FT.EXPLAINCLI: basic single term query returns array" {
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
    const create_cmd = "FT.CREATE products ON HASH SCHEMA name TEXT price NUMERIC\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain single term with CLI format
    const explain_cmd = "FT.EXPLAINCLI products laptop\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Response should be array marker (*) with 3 elements
    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null); // Array of 3 elements
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM {") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "laptop") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "}") != null);
}

test "FT.EXPLAINCLI: special characters in query" {
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

    // Explain query with special characters
    const explain_cmd = "FT.EXPLAINCLI idx c++ c#\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should be array format
    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "c++") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "c#") != null);
}

test "FT.EXPLAINCLI: numeric query string representation" {
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
    const create_cmd = "FT.CREATE idx2 ON HASH SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with numbers in query
    const explain_cmd = "FT.EXPLAINCLI idx2 item123 product456\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "item123") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "product456") != null);
}

test "FT.EXPLAINCLI: dialect 1 (default)" {
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
    const create_cmd = "FT.CREATE idx3 ON HASH SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with explicit dialect 1
    const explain_cmd = "FT.EXPLAINCLI idx3 test DIALECT 1\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "test") != null);
}

test "FT.EXPLAINCLI: dialect 2" {
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
    const create_cmd = "FT.CREATE idx4 ON HASH SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with dialect 2
    const explain_cmd = "FT.EXPLAINCLI idx4 example DIALECT 2\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should still work even if dialect is not fully used in stub
    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
}

test "FT.EXPLAINCLI: empty query string" {
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
    const create_cmd = "FT.CREATE idx5 ON HASH SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain with empty query
    const explain_cmd = "FT.EXPLAINCLI idx5 \"\"\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should still return TERM even if query is empty
    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
}

test "FT.EXPLAINCLI: JSON index support" {
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

    // Create JSON index
    const create_cmd = "FT.CREATE json_idx ON JSON SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain on JSON index
    const explain_cmd = "FT.EXPLAINCLI json_idx document\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "document") != null);
}

test "FT.EXPLAINCLI: error on nonexistent index" {
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

    // Explain on non-existent index
    const explain_cmd = "FT.EXPLAINCLI nonexistent query\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR Unknown index name") != null);
}

test "FT.EXPLAINCLI: error on wrong arity (missing query)" {
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

    // Missing query argument
    const explain_cmd = "FT.EXPLAINCLI idx\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return arity error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR wrong number of arguments") != null);
}

test "FT.EXPLAINCLI: error on invalid DIALECT syntax" {
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
    const create_cmd = "FT.CREATE idx6 ON HASH SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // DIALECT without value
    const explain_cmd = "FT.EXPLAINCLI idx6 query DIALECT\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR DIALECT requires an integer argument") != null);
}

test "FT.EXPLAINCLI: error on non-integer DIALECT value" {
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
    const create_cmd = "FT.CREATE idx7 ON HASH SCHEMA name TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // DIALECT with non-integer value
    const explain_cmd = "FT.EXPLAINCLI idx7 query DIALECT abc\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR DIALECT must be an integer") != null);
}
