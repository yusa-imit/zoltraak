const std = @import("std");
const parser_mod = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const cmd_mod = @import("../src/commands/strings.zig");
const storage_mod = @import("../src/storage/memory.zig");
const repl_mod = @import("../src/storage/replication.zig");

const Parser = parser_mod.Parser;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const ReplicationState = repl_mod.ReplicationState;

// ── FAILOVER Integration Tests ───────────────────────────────────────────────

test "FAILOVER - basic command" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Parse FAILOVER command
    const input = "*1\r\n$8\r\nFAILOVER\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const array = try parser.parse(input);
    defer parser_mod.freeArray(allocator, array);

    // Execute command
    const result = try cmd_mod.executeCommand(
        allocator,
        array,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result);

    // Verify result
    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqual(repl_mod.FailoverState.waiting_for_sync, repl.failover_state);
}

test "FAILOVER - with TIMEOUT" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Parse FAILOVER TIMEOUT 5000 command
    const input = "*3\r\n$8\r\nFAILOVER\r\n$7\r\nTIMEOUT\r\n$4\r\n5000\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const array = try parser.parse(input);
    defer parser_mod.freeArray(allocator, array);

    // Execute command
    const result = try cmd_mod.executeCommand(
        allocator,
        array,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result);

    // Verify result
    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqual(repl_mod.FailoverState.waiting_for_sync, repl.failover_state);
    try std.testing.expectEqual(@as(u64, 5000), repl.failover_timeout_ms);
}

test "FAILOVER - ABORT without ongoing failover" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Parse FAILOVER ABORT command
    const input = "*2\r\n$8\r\nFAILOVER\r\n$5\r\nABORT\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const array = try parser.parse(input);
    defer parser_mod.freeArray(allocator, array);

    // Execute command
    const result = try cmd_mod.executeCommand(
        allocator,
        array,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result);

    // Verify error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "No failover") != null);
}

test "FAILOVER - ABORT cancels ongoing failover" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Start failover
    const input1 = "*1\r\n$8\r\nFAILOVER\r\n";
    var parser1 = Parser.init(allocator);
    defer parser1.deinit();

    const array1 = try parser1.parse(input1);
    defer parser_mod.freeArray(allocator, array1);

    const result1 = try cmd_mod.executeCommand(
        allocator,
        array1,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result1);

    try std.testing.expectEqualStrings("+OK\r\n", result1);

    // Abort failover
    const input2 = "*2\r\n$8\r\nFAILOVER\r\n$5\r\nABORT\r\n";
    var parser2 = Parser.init(allocator);
    defer parser2.deinit();

    const array2 = try parser2.parse(input2);
    defer parser_mod.freeArray(allocator, array2);

    const result2 = try cmd_mod.executeCommand(
        allocator,
        array2,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result2);

    // Verify abort succeeded
    try std.testing.expectEqualStrings("+OK\r\n", result2);
    try std.testing.expectEqual(repl_mod.FailoverState.no_failover, repl.failover_state);
}

test "FAILOVER - already in progress" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Start first failover
    const input1 = "*1\r\n$8\r\nFAILOVER\r\n";
    var parser1 = Parser.init(allocator);
    defer parser1.deinit();

    const array1 = try parser1.parse(input1);
    defer parser_mod.freeArray(allocator, array1);

    const result1 = try cmd_mod.executeCommand(
        allocator,
        array1,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result1);

    // Try to start second failover
    const input2 = "*1\r\n$8\r\nFAILOVER\r\n";
    var parser2 = Parser.init(allocator);
    defer parser2.deinit();

    const array2 = try parser2.parse(input2);
    defer parser_mod.freeArray(allocator, array2);

    const result2 = try cmd_mod.executeCommand(
        allocator,
        array2,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result2);

    // Verify error
    try std.testing.expect(std.mem.startsWith(u8, result2, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result2, "already in progress") != null);
}

test "FAILOVER - on replica fails" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initReplica(allocator, "127.0.0.1", 6379);
    defer repl.deinit();

    // Parse FAILOVER command
    const input = "*1\r\n$8\r\nFAILOVER\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const array = try parser.parse(input);
    defer parser_mod.freeArray(allocator, array);

    // Execute command
    const result = try cmd_mod.executeCommand(
        allocator,
        array,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result);

    // Verify error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "replica") != null);
}

test "FAILOVER - INFO replication shows master_failover_state" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Parse INFO replication command
    const input = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const array = try parser.parse(input);
    defer parser_mod.freeArray(allocator, array);

    // Execute command
    const result = try cmd_mod.executeCommand(
        allocator,
        array,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result);

    // Verify master_failover_state is present
    try std.testing.expect(std.mem.indexOf(u8, result, "master_failover_state:no-failover") != null);
}

test "FAILOVER - INFO replication shows waiting-for-sync during failover" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Start failover
    const input1 = "*1\r\n$8\r\nFAILOVER\r\n";
    var parser1 = Parser.init(allocator);
    defer parser1.deinit();

    const array1 = try parser1.parse(input1);
    defer parser_mod.freeArray(allocator, array1);

    const result1 = try cmd_mod.executeCommand(
        allocator,
        array1,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result1);

    // Get INFO replication
    const input2 = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    var parser2 = Parser.init(allocator);
    defer parser2.deinit();

    const array2 = try parser2.parse(input2);
    defer parser_mod.freeArray(allocator, array2);

    const result2 = try cmd_mod.executeCommand(
        allocator,
        array2,
        &storage,
        &repl,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
    );
    defer allocator.free(result2);

    // Verify master_failover_state shows waiting-for-sync
    try std.testing.expect(std.mem.indexOf(u8, result2, "master_failover_state:waiting-for-sync") != null);
}
