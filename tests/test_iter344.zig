const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const ClientRegistry = zoltraak.ClientRegistry;
const BlockingQueue = zoltraak.BlockingQueue;
const client_cmds = zoltraak.client;

// Iteration 344: MULTI/EXEC allocator mismatch fix + CLIENT SETNAME validation
//
// Bug 1: MULTI/EXEC "Invalid free" panic — queued command bytes were allocated
//   with the per-request arena allocator but freed with tx.allocator (GPA).
//   Fix: use tx.allocator when serializing queued command bytes.
//
// Bug 2: CLIENT SETNAME accepted non-printable ASCII characters (newlines,
//   tabs, control chars). Redis only allows printable ASCII (0x21-0x7E).
//   Fix: validate each byte is in range [0x21, 0x7E] (or empty string).

// ── CLIENT SETNAME validation tests ──────────────────────────────────────────

test "CLIENT SETNAME - rejects tab character" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var args = std.ArrayList(RespValue){};
    defer args.deinit(aa);
    try args.append(aa, RespValue{ .bulk_string = "SETNAME" });
    // Name contains a tab character (0x09 < 0x21)
    try args.append(aa, RespValue{ .bulk_string = "my\tclient" });

    const response = try client_cmds.cmdClient(allocator, &registry, client_id, args.items, .RESP2, &blocking_queue);
    defer allocator.free(response);

    // Must reject with error
    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT SETNAME - rejects newline character" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var args = std.ArrayList(RespValue){};
    defer args.deinit(aa);
    try args.append(aa, RespValue{ .bulk_string = "SETNAME" });
    // Name contains newline (0x0A < 0x21)
    try args.append(aa, RespValue{ .bulk_string = "my\nclient" });

    const response = try client_cmds.cmdClient(allocator, &registry, client_id, args.items, .RESP2, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT SETNAME - rejects DEL character (0x7F)" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var args = std.ArrayList(RespValue){};
    defer args.deinit(aa);
    try args.append(aa, RespValue{ .bulk_string = "SETNAME" });
    // Name contains DEL (0x7F > 0x7E)
    try args.append(aa, RespValue{ .bulk_string = "my\x7fclient" });

    const response = try client_cmds.cmdClient(allocator, &registry, client_id, args.items, .RESP2, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT SETNAME - accepts printable ASCII including punctuation" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var args = std.ArrayList(RespValue){};
    defer args.deinit(aa);
    try args.append(aa, RespValue{ .bulk_string = "SETNAME" });
    // All printable ASCII (hyphen, underscore, alphanumeric)
    try args.append(aa, RespValue{ .bulk_string = "my-client_123" });

    const response = try client_cmds.cmdClient(allocator, &registry, client_id, args.items, .RESP2, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT SETNAME - accepts empty string (resets name)" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42, "127.0.0.1:6379");

    // First set a name
    try registry.setClientName(client_id, "existing-name");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var args = std.ArrayList(RespValue){};
    defer args.deinit(aa);
    try args.append(aa, RespValue{ .bulk_string = "SETNAME" });
    try args.append(aa, RespValue{ .bulk_string = "" });

    const response = try client_cmds.cmdClient(allocator, &registry, client_id, args.items, .RESP2, &blocking_queue);
    defer allocator.free(response);

    // Empty string resets the name — should succeed
    try std.testing.expectEqualStrings("+OK\r\n", response);
}

// ── MULTI/EXEC allocator tests ────────────────────────────────────────────────

test "MULTI/EXEC - queued commands execute correctly with WRONGTYPE error mid-transaction" {
    const allocator = std.testing.allocator;

    // Use an arena for the tx (simulating self.allocator in server)
    // and std.testing.allocator for storage
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mystr", "hello", null);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const commands = zoltraak.commands;
    var tx = commands.TxState.init(allocator);
    defer tx.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:11111", 42, "127.0.0.1:6379");

    var script_store = commands.ScriptStore.init(allocator);
    defer script_store.deinit();

    // MULTI
    {
        const multi_args = [_]RespValue{.{ .bulk_string = "MULTI" }};
        const multi_cmd = RespValue{ .array = &multi_args };
        const r = try commands.executeCommand(
            allocator, storage, multi_cmd, null, &ps, 0, &tx, null, 6379,
            null, null, &registry, client_id, &script_store, null,
            @as([]Storage, &.{}), 0,
        );
        defer allocator.free(r);
        try std.testing.expectEqualStrings("+OK\r\n", r);
    }
    try std.testing.expect(tx.active);

    // Queue: GET mystr
    {
        const get_args = [_]RespValue{
            .{ .bulk_string = "GET" },
            .{ .bulk_string = "mystr" },
        };
        const get_cmd = RespValue{ .array = &get_args };
        const r = try commands.executeCommand(
            allocator, storage, get_cmd, null, &ps, 0, &tx, null, 6379,
            null, null, &registry, client_id, &script_store, null,
            @as([]Storage, &.{}), 0,
        );
        defer allocator.free(r);
        try std.testing.expectEqualStrings("+QUEUED\r\n", r);
    }

    // Queue: LPUSH mystr a  (WRONGTYPE — mystr is a string, not a list)
    {
        const lpush_args = [_]RespValue{
            .{ .bulk_string = "LPUSH" },
            .{ .bulk_string = "mystr" },
            .{ .bulk_string = "a" },
        };
        const lpush_cmd = RespValue{ .array = &lpush_args };
        const r = try commands.executeCommand(
            allocator, storage, lpush_cmd, null, &ps, 0, &tx, null, 6379,
            null, null, &registry, client_id, &script_store, null,
            @as([]Storage, &.{}), 0,
        );
        defer allocator.free(r);
        try std.testing.expectEqualStrings("+QUEUED\r\n", r);
    }

    // EXEC — should return array with 2 results without panicking
    {
        const exec_args = [_]RespValue{.{ .bulk_string = "EXEC" }};
        const exec_cmd = RespValue{ .array = &exec_args };
        const r = try commands.executeCommand(
            allocator, storage, exec_cmd, null, &ps, 0, &tx, null, 6379,
            null, null, &registry, client_id, &script_store, null,
            @as([]Storage, &.{}), 0,
        );
        defer allocator.free(r);

        // Result: *2\r\n + bulk_string "hello" + WRONGTYPE error
        try std.testing.expect(std.mem.startsWith(u8, r, "*2\r\n"));
        try std.testing.expect(std.mem.indexOf(u8, r, "hello") != null);
        try std.testing.expect(std.mem.indexOf(u8, r, "WRONGTYPE") != null);
    }

    // tx.active should be false after EXEC
    try std.testing.expect(!tx.active);
}

test "MULTI/EXEC - empty transaction returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const commands = zoltraak.commands;
    var tx = commands.TxState.init(allocator);
    defer tx.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:11111", 42, "127.0.0.1:6379");

    var script_store = commands.ScriptStore.init(allocator);
    defer script_store.deinit();

    // MULTI
    {
        const multi_args = [_]RespValue{.{ .bulk_string = "MULTI" }};
        const multi_cmd = RespValue{ .array = &multi_args };
        const r = try commands.executeCommand(
            allocator, storage, multi_cmd, null, &ps, 0, &tx, null, 6379,
            null, null, &registry, client_id, &script_store, null,
            @as([]Storage, &.{}), 0,
        );
        defer allocator.free(r);
        try std.testing.expectEqualStrings("+OK\r\n", r);
    }

    // EXEC with no queued commands
    {
        const exec_args = [_]RespValue{.{ .bulk_string = "EXEC" }};
        const exec_cmd = RespValue{ .array = &exec_args };
        const r = try commands.executeCommand(
            allocator, storage, exec_cmd, null, &ps, 0, &tx, null, 6379,
            null, null, &registry, client_id, &script_store, null,
            @as([]Storage, &.{}), 0,
        );
        defer allocator.free(r);
        try std.testing.expectEqualStrings("*0\r\n", r);
    }
}
