const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const parseCommand = @import("../src/protocol/parser.zig").parseCommand;
const processCommand = @import("../src/commands/strings.zig").processCommand;
const TxState = @import("../src/commands/strings.zig").TxState;
const ReplicationState = @import("../src/commands/strings.zig").ReplicationState;
const ScriptStore = @import("../src/commands/strings.zig").ScriptStore;
const PubSubState = @import("../src/storage/pubsub.zig").PubSubState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;

test "FT.PROFILE SEARCH: basic profiling returns 2-element array" {
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

    // Profile search with wildcard query
    const profile_cmd = "FT.PROFILE products SEARCH QUERY *\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Response should be 2-element array: *2
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);

    // Should contain profile metrics
    try std.testing.expect(std.mem.indexOf(u8, result, "Total profile time") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Parsing time") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Pipeline creation time") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Iterators profile") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Result processors profile") != null);
}

test "FT.PROFILE SEARCH: returns results and profile data" {
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

    // Profile search
    const profile_cmd = "FT.PROFILE idx SEARCH QUERY test\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should have 2-element outer array
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);

    // First element is search results (array starting with count)
    try std.testing.expect(std.mem.indexOf(u8, result, "WILDCARD") != null); // Iterator type
    try std.testing.expect(std.mem.indexOf(u8, result, "Counter") != null);  // Processor type for SEARCH
}

test "FT.PROFILE SEARCH: with LIMITED flag reduces output" {
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

    // Profile with LIMITED flag
    const profile_cmd = "FT.PROFILE idx2 SEARCH LIMITED QUERY query\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should still be 2-element array with profile data
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Total profile time") != null);
}

test "FT.PROFILE AGGREGATE: basic aggregation profiling" {
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
    const create_cmd = "FT.CREATE agg_idx ON HASH SCHEMA brand TEXT category TEXT\r\n";
    const parsed_create = try parseCommand(allocator, create_cmd);
    defer parsed_create.deinit(allocator);

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();

    const create_result = try processCommand(arena1.allocator(), parsed_create.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(create_result);

    // Profile aggregate
    const profile_cmd = "FT.PROFILE agg_idx AGGREGATE QUERY *\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should be 2-element array
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);

    // AGGREGATE uses "Grouper" processor
    try std.testing.expect(std.mem.indexOf(u8, result, "Grouper") != null);
}

test "FT.PROFILE: error on wrong number of arguments" {
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

    // Too few arguments
    const profile_cmd = "FT.PROFILE idx SEARCH\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR wrong number of arguments") != null);
}

test "FT.PROFILE: error on unknown index name" {
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

    // Profile on non-existent index
    const profile_cmd = "FT.PROFILE nonexistent SEARCH QUERY test\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR Unknown index name") != null);
}

test "FT.PROFILE: error on invalid query type" {
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

    // Invalid query type
    const profile_cmd = "FT.PROFILE idx3 INVALID QUERY test\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR syntax error, expected SEARCH or AGGREGATE") != null);
}

test "FT.PROFILE: error on missing QUERY keyword" {
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

    // Missing QUERY keyword
    const profile_cmd = "FT.PROFILE idx4 SEARCH test\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR syntax error, expected QUERY keyword") != null);
}

test "FT.PROFILE AGGREGATE: with LIMITED flag" {
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

    // Profile aggregate with LIMITED
    const profile_cmd = "FT.PROFILE idx5 AGGREGATE LIMITED QUERY *\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should be 2-element array with Grouper processor
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Grouper") != null);
}

test "FT.PROFILE: profile data contains all required fields" {
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

    // Profile search
    const profile_cmd = "FT.PROFILE idx6 SEARCH QUERY test\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Verify all required profile fields are present
    try std.testing.expect(std.mem.indexOf(u8, result, "Total profile time") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Parsing time") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Pipeline creation time") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Warning") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Iterators profile") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Result processors profile") != null);
}

test "FT.PROFILE: iterator tree includes WILDCARD type" {
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

    // Profile search
    const profile_cmd = "FT.PROFILE idx7 SEARCH QUERY test\r\n";
    const parsed_profile = try parseCommand(allocator, profile_cmd);
    defer parsed_profile.deinit(allocator);

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const result = try processCommand(arena2.allocator(), parsed_profile.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Verify iterator tree has WILDCARD type
    try std.testing.expect(std.mem.indexOf(u8, result, "Type") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "WILDCARD") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Query type") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Number of reading operations") != null);
}
