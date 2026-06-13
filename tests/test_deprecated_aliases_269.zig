const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const commands = @import("../src/commands/strings.zig");
const storage_mod = @import("../src/storage/memory.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");
const repl_mod = @import("../src/storage/replication.zig");
const client_mod = @import("../src/commands/client.zig");
const scripting_mod = @import("../src/storage/scripting.zig");
const cluster_mod = @import("../src/storage/cluster.zig");

const Parser = protocol.Parser;
const Writer = writer_mod.Writer;
const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;
const PubSub = pubsub_mod.PubSub;
const ReplicationState = repl_mod.ReplicationState;
const ClientRegistry = client_mod.ClientRegistry;
const ScriptStore = scripting_mod.ScriptStore;
const ClusterState = cluster_mod.ClusterState;

/// Integration test for HMSET (deprecated alias for HSET)
test "deprecated aliases - HMSET redirects to HSET" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = try PubSub.init(allocator);
    defer ps.deinit();

    // HMSET key field1 value1 field2 value2
    const cmd_str = "*5\r\n$5\r\nHMSET\r\n$4\r\nuser\r\n$4\r\nname\r\n$5\r\nAlice\r\n$3\r\nage\r\n$2\r\n30\r\n";
    var parser = Parser.init(allocator);
    const cmd = try parser.parse(cmd_str);

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:12345", -1, "127.0.0.1:6379");

    var tx = commands.TxState.init(allocator);
    defer tx.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var databases: [1]Storage = undefined;
    databases[0] = try Storage.init(allocator);
    defer databases[0].deinit();

    const response = try commands.executeCommand(
        allocator,
        &databases[0],
        cmd,
        null,
        &ps,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
        &databases,
        1,
    );
    defer allocator.free(response);

    // HMSET returns +OK
    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Verify values were set
    const name = storage.hget("user", "name").?;
    try std.testing.expectEqualStrings("Alice", name);

    const age = storage.hget("user", "age").?;
    try std.testing.expectEqualStrings("30", age);
}

/// Integration test for RPOPLPUSH (deprecated alias for LMOVE RIGHT LEFT)
test "deprecated aliases - RPOPLPUSH redirects to LMOVE" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = try PubSub.init(allocator);
    defer ps.deinit();

    // Setup: RPUSH source a b c
    _ = try storage.rpush(allocator, "source", &[_][]const u8{ "a", "b", "c" });

    // RPOPLPUSH source dest
    const cmd_str = "*3\r\n$10\r\nRPOPLPUSH\r\n$6\r\nsource\r\n$4\r\ndest\r\n";
    var parser = Parser.init(allocator);
    const cmd = try parser.parse(cmd_str);

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:12345", -1, "127.0.0.1:6379");

    var tx = commands.TxState.init(allocator);
    defer tx.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var databases: [1]Storage = undefined;
    databases[0] = try Storage.init(allocator);
    defer databases[0].deinit();

    // Copy source list
    _ = try databases[0].rpush(allocator, "source", &[_][]const u8{ "a", "b", "c" });

    const response = try commands.executeCommand(
        allocator,
        &databases[0],
        cmd,
        null,
        &ps,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
        &databases,
        1,
    );
    defer allocator.free(response);

    // Should return bulk string "c" (rightmost element)
    try std.testing.expectEqualStrings("$1\r\nc\r\n", response);

    // Verify source list has 2 elements
    try std.testing.expectEqual(@as(usize, 2), databases[0].llen("source").?);

    // Verify dest list has 1 element at head
    const dest_elem = databases[0].lindex("dest", 0).?;
    try std.testing.expectEqualStrings("c", dest_elem);
}

/// Integration test for SLAVEOF (deprecated alias for REPLICAOF)
test "deprecated aliases - SLAVEOF NO ONE redirects to REPLICAOF" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.init(allocator, 6379);
    defer repl.deinit();

    var ps = try PubSub.init(allocator);
    defer ps.deinit();

    // SLAVEOF NO ONE
    const cmd_str = "*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n";
    var parser = Parser.init(allocator);
    const cmd = try parser.parse(cmd_str);

    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:12345", -1, "127.0.0.1:6379");

    var tx = commands.TxState.init(allocator);
    defer tx.deinit();

    var script_store = try ScriptStore.init(allocator);
    defer script_store.deinit();

    var databases: [1]Storage = undefined;
    databases[0] = try Storage.init(allocator);
    defer databases[0].deinit();

    const response = try commands.executeCommand(
        allocator,
        &databases[0],
        cmd,
        null,
        &ps,
        1,
        &tx,
        &repl,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
        &databases,
        1,
    );
    defer allocator.free(response);

    // Should return +OK
    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Verify role changed to primary
    try std.testing.expectEqual(ReplicationState.Role.primary, repl.role);
}
