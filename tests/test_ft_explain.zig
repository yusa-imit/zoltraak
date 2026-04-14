const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const parseCommand = @import("../src/protocol/parser.zig").parseCommand;
const processCommand = @import("../src/commands/strings.zig").processCommand;
const TxState = @import("../src/commands/strings.zig").TxState;
const ReplicationState = @import("../src/commands/strings.zig").ReplicationState;
const ScriptStore = @import("../src/commands/strings.zig").ScriptStore;
const PubSubState = @import("../src/storage/pubsub.zig").PubSubState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;

test "FT.EXPLAIN: basic single term query" {
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

    // Explain single term
    const explain_cmd = "FT.EXPLAIN products laptop\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Response should be bulk string with TERM wrapper
    try std.testing.expect(std.mem.indexOf(u8, result, "$") != null); // Bulk string marker
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "laptop") != null);
}

test "FT.EXPLAIN: special characters in query" {
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
    const explain_cmd = "FT.EXPLAIN idx c++ c#\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "c++") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "c#") != null);
}

test "FT.EXPLAIN: numeric query string representation" {
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
    const explain_cmd = "FT.EXPLAIN idx2 item123 product456\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "item123") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "product456") != null);
}

test "FT.EXPLAIN: dialect 1 (default)" {
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
    const explain_cmd = "FT.EXPLAIN idx3 test DIALECT 1\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "test") != null);
}

test "FT.EXPLAIN: dialect 2" {
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
    const explain_cmd = "FT.EXPLAIN idx4 example DIALECT 2\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should still work even if dialect is not fully used in stub
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
}

test "FT.EXPLAIN: empty query string" {
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
    const explain_cmd = "FT.EXPLAIN idx5 \"\"\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should still return TERM even if query is empty
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
}

test "FT.EXPLAIN: response format validation" {
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

    // Explain with query
    const explain_cmd = "FT.EXPLAIN idx6 search\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Response must contain proper formatting with braces and newlines
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM {") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "}") != null);
    // Check for newline indentation (format: "TERM {\n  search\n}\n")
    try std.testing.expect(std.mem.indexOf(u8, result, "search") != null);
}

test "FT.EXPLAIN: JSON index support" {
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
    const explain_cmd = "FT.EXPLAIN json_idx document\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "document") != null);
}

test "FT.EXPLAIN: works with multiple fields" {
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

    // Create index with multiple fields
    const create_cmd = "FT.CREATE multifield ON HASH SCHEMA title TEXT content TEXT author TEXT tags TAG\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Explain on multi-field index
    const explain_cmd = "FT.EXPLAIN multifield data\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "data") != null);
}

test "FT.EXPLAIN: case sensitivity of DIALECT keyword" {
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

    // Try lowercase 'dialect' (should still work - command processing is case-insensitive)
    const explain_cmd = "FT.EXPLAIN idx7 test dialect 1\r\n";
    const parsed_explain = try parseCommand(allocator, explain_cmd);
    defer parsed_explain.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_explain.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should work (RESP protocol converts to uppercase internally)
    try std.testing.expect(std.mem.indexOf(u8, result, "TERM") != null);
}
