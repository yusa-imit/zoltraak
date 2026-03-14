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

// ── ROLE Integration Tests ─────────────────────────────────────────────────

test "ROLE - standalone server returns master role with no replicas" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result structure: ["master", 0, []]
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nmaster\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "ROLE - master with replicas shows replica list" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Add some replicas to the replica list
    {
        const replica1: repl_mod.ReplicaInfo = .{
            .port = 6380,
            .repl_offset = 100,
            .primary_stream = 100,
        };
        try repl.replicas.append(allocator, replica1);

        const replica2: repl_mod.ReplicaInfo = .{
            .port = 6381,
            .repl_offset = 150,
            .primary_stream = 101,
        };
        try repl.replicas.append(allocator, replica2);
    }

    repl.repl_offset = 200;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result structure: ["master", 200, [[127.0.0.1, 6380, 100], [127.0.0.1, 6381, 150]]]
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nmaster\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":200\r\n") != null);

    // Verify replica list array starts with *2 (2 replicas)
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);

    // Verify first replica info
    try std.testing.expect(std.mem.indexOf(u8, result, "127.0.0.1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$4\r\n6380\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$3\r\n100\r\n") != null);

    // Verify second replica info
    try std.testing.expect(std.mem.indexOf(u8, result, "$4\r\n6381\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$3\r\n150\r\n") != null);
}

test "ROLE - replica not connected returns connect state" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Convert to replica (not connected, primary_stream = null)
    repl.role = .replica;
    repl.primary_host = try allocator.dupe(u8, "192.168.1.100");
    repl.primary_port = 6379;
    repl.primary_stream = null;
    repl.primary_link_up = false;
    repl.repl_offset = 0;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result structure: ["slave", "192.168.1.100", 6379, "connect", -1]
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$15\r\n192.168.1.100\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$7\r\nconnect\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":-1\r\n") != null);
}

test "ROLE - replica connecting returns connecting state" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Convert to replica (stream exists but link is down)
    repl.role = .replica;
    repl.primary_host = try allocator.dupe(u8, "192.168.1.50");
    repl.primary_port = 6379;
    repl.primary_stream = 200; // Stream exists (non-null)
    repl.primary_link_up = false; // But link is down
    repl.repl_offset = 500;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result structure: ["slave", "192.168.1.50", 6379, "connecting", 500]
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$13\r\n192.168.1.50\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$10\r\nconnecting\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":500\r\n") != null);
}

test "ROLE - replica fully connected returns connected state" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Convert to replica (stream exists and link is up)
    repl.role = .replica;
    repl.primary_host = try allocator.dupe(u8, "10.0.0.1");
    repl.primary_port = 6379;
    repl.primary_stream = 300; // Stream exists
    repl.primary_link_up = true; // Link is up
    repl.repl_offset = 1000;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result structure: ["slave", "10.0.0.1", 6379, "connected", 1000]
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$9\r\n10.0.0.1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$9\r\nconnected\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":1000\r\n") != null);
}

test "ROLE - with arguments returns error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Parse ROLE command with extra argument
    const input = "*2\r\n$4\r\nROLE\r\n$5\r\nextra\r\n";
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

    // Verify error response
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "ROLE - primary master role with multiple replicas complete structure" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Add three replicas
    {
        const replica1: repl_mod.ReplicaInfo = .{
            .port = 6380,
            .repl_offset = 50,
            .primary_stream = 100,
        };
        try repl.replicas.append(allocator, replica1);

        const replica2: repl_mod.ReplicaInfo = .{
            .port = 6381,
            .repl_offset = 75,
            .primary_stream = 101,
        };
        try repl.replicas.append(allocator, replica2);

        const replica3: repl_mod.ReplicaInfo = .{
            .port = 6382,
            .repl_offset = 200,
            .primary_stream = 102,
        };
        try repl.replicas.append(allocator, replica3);
    }

    repl.repl_offset = 500;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify full structure
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));

    // Should have 3-element master array
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nmaster\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":500\r\n") != null);

    // Replica list should have 3 entries
    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);

    // Verify all replica ports exist
    try std.testing.expect(std.mem.indexOf(u8, result, "6380") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "6381") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "6382") != null);

    // Verify IP addresses (all should be 127.0.0.1)
    const ip_count = blk: {
        var count: u32 = 0;
        var pos: ?usize = 0;
        while (pos) |p| : (pos = std.mem.indexOf(u8, result[p + 1 ..], "127.0.0.1")) {
            count += 1;
            if (pos.? + 1 >= result.len) break;
        }
        break :blk count;
    };
    // Should have at least 3 IP entries (one for each replica)
    try std.testing.expect(ip_count >= 3);
}

test "ROLE - standalone with offset progression" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Simulate offset progression on master
    repl.repl_offset = 9999;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result has correct offset
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, ":9999\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nmaster\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "ROLE - replica without primary_host uses fallback" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Convert to replica but don't set primary_host
    repl.role = .replica;
    repl.primary_host = null;
    repl.primary_port = 6379;
    repl.primary_stream = null;
    repl.primary_link_up = false;
    repl.repl_offset = 0;

    // Parse ROLE command
    const input = "*1\r\n$4\r\nROLE\r\n";
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

    // Verify result uses fallback IP (127.0.0.1)
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$9\r\n127.0.0.1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$7\r\nconnect\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":-1\r\n") != null);
}
