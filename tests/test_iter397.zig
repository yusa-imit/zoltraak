// Iteration 397: RESP3 verbatim string for LOLWUT + sailor v2.75.0
//
// Redis protocol behavior:
// - LOLWUT RESP2: returns bulk string ($N\r\n...) with computer art
// - LOLWUT RESP3: returns verbatim string (=N\r\ntxt:...) with "txt" encoding hint
//
// sailor v2.75.0: WaterfallChart widget added (library update, no API changes)

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const transactions_mod = zoltraak.transactions_commands;

fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| resp_args[i] = .{ .bulk_string = a };
    const cmd = RespValue{ .array = resp_args };
    var tx = transactions_mod.TxState.init(allocator);
    defer tx.deinit();
    var script_store = scripting.ScriptStore.init(allocator);
    defer script_store.deinit();
    var databases = [_]Storage{storage.*};
    return commands.executeCommand(
        allocator,
        storage,
        cmd,
        null,
        ps,
        0,
        &tx,
        null,
        6379,
        null,
        null,
        client_registry,
        client_id,
        &script_store,
        null,
        &databases,
        1,
    );
}

fn setup(allocator: std.mem.Allocator, port_str: []const u8) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient(port_str, 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ─── LOLWUT RESP2 ─────��───────────────────────────────────────────────────────

test "iter397 - LOLWUT RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9840");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT",
    });
    defer allocator.free(resp);

    // RESP2: bulk string starts with '$'
    try testing.expect(resp[0] == '$');
    // Must NOT be RESP3 verbatim string format
    try testing.expect(resp[0] != '=');
    // Contains Zoltraak art
    try testing.expect(std.mem.indexOf(u8, resp, "Zoltraak") != null);
}

test "iter397 - LOLWUT RESP2 VERSION 5 returns bulk string with Redis 5 art" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9841");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT", "VERSION", "5",
    });
    defer allocator.free(resp);

    // RESP2: bulk string
    try testing.expect(resp[0] == '$');
    try testing.expect(std.mem.indexOf(u8, resp, "Redis 5") != null);
}

// ─── LOLWUT RESP3 ──���───────────────��──────────────────────────────────────────

test "iter397 - LOLWUT RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9842");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT",
    });
    defer allocator.free(resp);

    // RESP3: verbatim string starts with '='
    try testing.expect(resp[0] == '=');
    // Must NOT be bulk string format
    try testing.expect(resp[0] != '$');
    // Contains "txt:" encoding hint
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    // Contains the art content
    try testing.expect(std.mem.indexOf(u8, resp, "Zoltraak") != null);
}

test "iter397 - LOLWUT RESP3 verbatim string format is correct" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9843");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT",
    });
    defer allocator.free(resp);

    // Format: =<len>\r\ntxt:<data>\r\n
    // Verify starts with '=' followed by a digit (length)
    try testing.expect(resp[0] == '=');
    try testing.expect(std.ascii.isDigit(resp[1]));
    // Find the \r\n separator after the length
    const crlf_pos = std.mem.indexOf(u8, resp, "\r\n") orelse unreachable;
    // After \r\n, the "txt:" prefix must appear
    const after_header = resp[crlf_pos + 2 ..];
    try testing.expect(std.mem.startsWith(u8, after_header, "txt:"));
}

test "iter397 - LOLWUT RESP3 VERSION 5 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9844");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT", "VERSION", "5",
    });
    defer allocator.free(resp);

    // RESP3 verbatim string with txt encoding
    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "Redis 5") != null);
}

test "iter397 - LOLWUT RESP3 VERSION 7 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9845");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT", "VERSION", "7",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "Redis 7") != null);
}

test "iter397 - LOLWUT RESP3 invalid VERSION returns error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9846");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT", "VERSION", "notanumber",
    });
    defer allocator.free(resp);

    // Error responses start with '-'
    try testing.expect(resp[0] == '-');
    try testing.expect(std.mem.indexOf(u8, resp, "ERR") != null);
}

test "iter397 - LOLWUT RESP2 vs RESP3 format differs" {
    const allocator = testing.allocator;

    // RESP2 client
    var ctx2 = try setup(allocator, "9847");
    defer ctx2.storage.deinit();
    defer ctx2.registry.deinit();
    defer ctx2.ps.deinit();

    const resp2 = try execCmd(allocator, ctx2.storage, &ctx2.registry, ctx2.client_id, &ctx2.ps, &.{
        "LOLWUT",
    });
    defer allocator.free(resp2);

    // RESP3 client
    var ctx3 = try setup(allocator, "9848");
    defer ctx3.storage.deinit();
    defer ctx3.registry.deinit();
    defer ctx3.ps.deinit();

    allocator.free(try execCmd(allocator, ctx3.storage, &ctx3.registry, ctx3.client_id, &ctx3.ps, &.{
        "HELLO", "3",
    }));

    const resp3 = try execCmd(allocator, ctx3.storage, &ctx3.registry, ctx3.client_id, &ctx3.ps, &.{
        "LOLWUT",
    });
    defer allocator.free(resp3);

    // Different encoding types
    try testing.expect(resp2[0] == '$'); // RESP2: bulk string
    try testing.expect(resp3[0] == '='); // RESP3: verbatim string

    // Both contain same art content
    try testing.expect(std.mem.indexOf(u8, resp2, "Zoltraak") != null);
    try testing.expect(std.mem.indexOf(u8, resp3, "Zoltraak") != null);
}

test "iter397 - LOLWUT RESP3 unknown version returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9849");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LOLWUT", "VERSION", "42",
    });
    defer allocator.free(resp);

    // RESP3 verbatim string even for unknown version
    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    // Generic art contains the version number
    try testing.expect(std.mem.indexOf(u8, resp, "42") != null);
}
