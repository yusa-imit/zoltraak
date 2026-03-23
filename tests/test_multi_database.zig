const std = @import("std");
const testing = std.testing;
const server_mod = @import("../src/server.zig");
const storage_mod = @import("../src/storage/memory.zig");
const commands = @import("../src/commands/strings.zig");
const client_mod = @import("../src/commands/client.zig");
const protocol = @import("../src/protocol/parser.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");
const repl_mod = @import("../src/storage/replication.zig");
const scripting_mod = @import("../src/storage/scripting.zig");

const Server = server_mod.Server;
const Storage = storage_mod.Storage;
const ClientRegistry = client_mod.ClientRegistry;
const RespValue = protocol.RespValue;
const Writer = @import("../src/protocol/writer.zig").Writer;

/// Helper to create a test server
fn createTestServer(allocator: std.mem.Allocator) !*Server {
    const config = server_mod.Config{
        .host = "127.0.0.1",
        .port = 6380,
    };

    return try Server.init(allocator, config);
}

/// Helper to execute a command with string args
fn executeCommandWithArgs(
    allocator: std.mem.Allocator,
    server: *Server,
    client_id: u64,
    args_strs: []const []const u8,
) ![]const u8 {
    // Build RespValue array from string args
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var args = std.ArrayList(RespValue).init(arena_alloc);
    for (args_strs) |arg_str| {
        try args.append(RespValue{ .bulk_string = arg_str });
    }

    const cmd = RespValue{ .array = try args.toOwnedSlice(arena_alloc) };

    // Get selected database for this client
    const selected_db = server.client_registry.getSelectedDb(client_id);
    const storage = &server.databases[@intCast(selected_db)];

    // Execute command with full signature
    return try commands.executeCommand(
        allocator,
        storage,
        cmd,
        null, // aof
        &server.pubsub,
        0, // subscriber_id
        &.{}, // tx - dummy
        null, // repl
        6379, // my_port
        null, // replica_stream
        null, // replica_idx
        &server.client_registry,
        client_id,
        &server.script_store,
        null, // shutdown_state
        server.databases,
        server.num_databases,
    );
}

test "multi-database: database isolation - operations on DB 0 don't affect DB 1" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    // Create two clients
    const client1_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client1_id);

    const client2_id = try server.client_registry.addClient("127.0.0.1", 12346, null);
    defer _ = server.client_registry.removeClient(client2_id);

    // Client 1: SET key "val0" in DB 0 (default)
    var set_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client1_id,
        &[_][]const u8{ "SET", "testkey", "val0" },
    );
    defer allocator.free(set_result1);

    // Client 2: SELECT 1
    try server.client_registry.setSelectedDb(client2_id, 1);

    // Client 2: GET testkey in DB 1 → should be nil
    var get_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client2_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result1);

    // Result should be "$-1\r\n" (nil in RESP3)
    try testing.expectEqualStrings("$-1\r\n", get_result1);

    // Client 2: SET testkey "val1" in DB 1
    var set_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client2_id,
        &[_][]const u8{ "SET", "testkey", "val1" },
    );
    defer allocator.free(set_result2);

    // Client 1: GET testkey in DB 0 → should still be "val0"
    var get_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client1_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result2);

    try testing.expect(std.mem.indexOf(u8, get_result2, "val0") != null);
}

test "multi-database: SELECT changes active database" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // Default: DB 0
    try testing.expectEqual(@as(u16, 0), server.client_registry.getSelectedDb(client_id));

    // SELECT 5
    try server.client_registry.setSelectedDb(client_id, 5);
    try testing.expectEqual(@as(u16, 5), server.client_registry.getSelectedDb(client_id));

    // SELECT 15
    try server.client_registry.setSelectedDb(client_id, 15);
    try testing.expectEqual(@as(u16, 15), server.client_registry.getSelectedDb(client_id));

    // SELECT 0 (back to default)
    try server.client_registry.setSelectedDb(client_id, 0);
    try testing.expectEqual(@as(u16, 0), server.client_registry.getSelectedDb(client_id));
}

test "multi-database: FLUSHDB only flushes selected database" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // SET key1 in DB 0
    var set_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "key1", "val1" },
    );
    defer allocator.free(set_result1);

    // SELECT 1, SET key2
    try server.client_registry.setSelectedDb(client_id, 1);
    var set_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "key2", "val2" },
    );
    defer allocator.free(set_result2);

    // FLUSHDB on DB 1
    var flush_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{"FLUSHDB"},
    );
    defer allocator.free(flush_result);

    // SELECT 0, GET key1 → should still exist
    try server.client_registry.setSelectedDb(client_id, 0);
    var get_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "GET", "key1" },
    );
    defer allocator.free(get_result);

    try testing.expect(std.mem.indexOf(u8, get_result, "val1") != null);
}

test "multi-database: DBSIZE counts only selected database" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // SET 5 keys in DB 0
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "key{d}", .{i});
        defer allocator.free(key);
        var set_result = try executeCommandWithArgs(
            allocator,
            server,
            client_id,
            &[_][]const u8{ "SET", key, "val" },
        );
        allocator.free(set_result);
    }

    // DBSIZE on DB 0 → 5
    var dbsize_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{"DBSIZE"},
    );
    defer allocator.free(dbsize_result1);

    try testing.expect(std.mem.indexOf(u8, dbsize_result1, ":5\r\n") != null);

    // SELECT 1, SET 3 keys
    try server.client_registry.setSelectedDb(client_id, 1);
    var j: usize = 0;
    while (j < 3) : (j += 1) {
        const key = try std.fmt.allocPrint(allocator, "key{d}", .{j});
        defer allocator.free(key);
        var set_result = try executeCommandWithArgs(
            allocator,
            server,
            client_id,
            &[_][]const u8{ "SET", key, "val" },
        );
        allocator.free(set_result);
    }

    // DBSIZE on DB 1 → 3
    var dbsize_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{"DBSIZE"},
    );
    defer allocator.free(dbsize_result2);

    try testing.expect(std.mem.indexOf(u8, dbsize_result2, ":3\r\n") != null);
}

test "multi-database: SWAPDB atomically swaps two databases" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // SET testkey "val0" in DB 0
    var set_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "testkey", "val0" },
    );
    defer allocator.free(set_result1);

    // SELECT 1, SET testkey "val1"
    try server.client_registry.setSelectedDb(client_id, 1);
    var set_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "testkey", "val1" },
    );
    defer allocator.free(set_result2);

    // SWAPDB 0 1
    var swap_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SWAPDB", "0", "1" },
    );
    defer allocator.free(swap_result);

    try testing.expectEqualStrings("+OK\r\n", swap_result);

    // SELECT 0, GET testkey → should now be "val1" (swapped from DB 1)
    try server.client_registry.setSelectedDb(client_id, 0);
    var get_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result1);

    try testing.expect(std.mem.indexOf(u8, get_result1, "val1") != null);

    // SELECT 1, GET testkey → should now be "val0" (swapped from DB 0)
    try server.client_registry.setSelectedDb(client_id, 1);
    var get_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result2);

    try testing.expect(std.mem.indexOf(u8, get_result2, "val0") != null);
}

test "multi-database: MOVE transfers key across databases" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // SET testkey "myvalue" in DB 0
    var set_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "testkey", "myvalue" },
    );
    defer allocator.free(set_result);

    // MOVE testkey 1 → returns 1 (success)
    var move_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "MOVE", "testkey", "1" },
    );
    defer allocator.free(move_result);

    try testing.expect(std.mem.indexOf(u8, move_result, ":1\r\n") != null);

    // GET testkey in DB 0 → nil (moved out)
    var get_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result1);

    try testing.expectEqualStrings("$-1\r\n", get_result1);

    // SELECT 1, GET testkey → "myvalue" (moved in)
    try server.client_registry.setSelectedDb(client_id, 1);
    var get_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result2);

    try testing.expect(std.mem.indexOf(u8, get_result2, "myvalue") != null);
}

test "multi-database: MOVE non-existent key returns 0" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // MOVE nonexistent 1 → returns 0 (key doesn't exist)
    var move_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "MOVE", "nonexistent", "1" },
    );
    defer allocator.free(move_result);

    try testing.expect(std.mem.indexOf(u8, move_result, ":0\r\n") != null);
}

test "multi-database: MOVE to database with existing key returns 0" {
    const allocator = testing.allocator;

    const server = try createTestServer(allocator);
    defer server.deinit();

    const client_id = try server.client_registry.addClient("127.0.0.1", 12345, null);
    defer _ = server.client_registry.removeClient(client_id);

    // SET testkey "val0" in DB 0
    var set_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "testkey", "val0" },
    );
    defer allocator.free(set_result1);

    // SELECT 1, SET testkey "val1" (key already exists in destination)
    try server.client_registry.setSelectedDb(client_id, 1);
    var set_result2 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "SET", "testkey", "val1" },
    );
    defer allocator.free(set_result2);

    // SELECT 0, MOVE testkey 1 → returns 0 (destination key exists)
    try server.client_registry.setSelectedDb(client_id, 0);
    var move_result = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "MOVE", "testkey", "1" },
    );
    defer allocator.free(move_result);

    try testing.expect(std.mem.indexOf(u8, move_result, ":0\r\n") != null);

    // Both keys should remain unchanged
    var get_result1 = try executeCommandWithArgs(
        allocator,
        server,
        client_id,
        &[_][]const u8{ "GET", "testkey" },
    );
    defer allocator.free(get_result1);
    try testing.expect(std.mem.indexOf(u8, get_result1, "val0") != null);
}
