// Iteration 392: RESP3 verbatim string for INFO + RESP3 map for XCLAIM/XAUTOCLAIM
//
// Redis protocol behavior:
// - INFO: RESP2 returns bulk string ($N\r\n...), RESP3 returns verbatim string (=N\r\ntxt:...)
// - XCLAIM: RESP2 returns entry fields as flat array (*N), RESP3 returns fields as map (%N)
// - XAUTOCLAIM: same as XCLAIM for field encoding in returned entries
//
// Affected commands: INFO, XCLAIM, XAUTOCLAIM

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

// ─── INFO RESP3 verbatim string ───────────────────────────────────────────────

test "iter392 - INFO RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9800");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{"INFO"});
    defer allocator.free(resp);

    // RESP2: bulk string starts with '$'
    try testing.expect(resp[0] == '$');
    // Must contain INFO sections
    try testing.expect(std.mem.indexOf(u8, resp, "# Server") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "redis_version:") != null);
}

test "iter392 - INFO RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9801");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{"INFO"});
    defer allocator.free(resp);

    // RESP3: verbatim string starts with '='
    try testing.expect(resp[0] == '=');
    // Must contain 'txt:' encoding prefix
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    // Content must still have INFO sections
    try testing.expect(std.mem.indexOf(u8, resp, "# Server") != null);
}

test "iter392 - INFO RESP3 verbatim format is correct" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9802");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{"INFO"});
    defer allocator.free(resp);

    // Verbatim format: =<len>\r\ntxt:<content>\r\n
    try testing.expect(resp[0] == '=');
    const crlf_pos = std.mem.indexOf(u8, resp, "\r\n") orelse return error.NoCRLF;
    const content_start = crlf_pos + 2;
    try testing.expect(content_start + 4 <= resp.len);
    try testing.expectEqualStrings("txt:", resp[content_start .. content_start + 4]);
}

test "iter392 - INFO SERVER section RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9803");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "INFO", "server" });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "# Server") != null);
}

// ─── XCLAIM RESP3 map for fields ─────────────────────────────────────────────

test "iter392 - XCLAIM RESP2 returns flat array for entry fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9804");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Setup: XADD -> XGROUP CREATE -> XREADGROUP (puts messages in pending)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "field1", "value1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "consumer1", "COUNT", "1", "STREAMS", "mystream", ">" }));

    // XCLAIM the message (min-idle-time=0 to force claim)
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XCLAIM", "mystream", "grp", "consumer2", "0", "1000-0" });
    defer allocator.free(resp);

    // RESP2: returns array starting with '*'
    try testing.expect(resp[0] == '*');
    // Entry format: *2\r\n$<id_len>\r\n<id>\r\n*<field_count>\r\n...
    // Check it's a 1-element array with the claimed entry
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    // Fields are flat array: '*N\r\n' (not '%N\r\n')
    const field_section = std.mem.indexOf(u8, resp, "1000-0\r\n") orelse return error.NoId;
    const after_id = resp[field_section + "1000-0\r\n".len ..];
    try testing.expect(after_id[0] == '*'); // flat array
}

test "iter392 - XCLAIM RESP3 returns map for entry fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9805");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "field1", "value1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "consumer1", "COUNT", "1", "STREAMS", "mystream", ">" }));

    // Switch to RESP3
    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XCLAIM", "mystream", "grp", "consumer2", "0", "1000-0" });
    defer allocator.free(resp);

    // RESP3: still an array of entries but each entry's fields are a map
    try testing.expect(resp[0] == '*');
    // Entry contains the ID
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    // Fields section should use '%N\r\n' (map) instead of '*N\r\n'
    const field_section = std.mem.indexOf(u8, resp, "1000-0\r\n") orelse return error.NoId;
    const after_id = resp[field_section + "1000-0\r\n".len ..];
    try testing.expect(after_id[0] == '%'); // map type
}

test "iter392 - XCLAIM JUSTID RESP3 returns only IDs (no field map)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9806");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "1", "STREAMS", "mystream", ">" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XCLAIM", "mystream", "grp", "c2", "0", "1000-0", "JUSTID" });
    defer allocator.free(resp);

    // JUSTID returns just the IDs, no map
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    // Should NOT contain '%' map type (no fields section)
    try testing.expect(std.mem.indexOf(u8, resp, "%") == null);
}

// ─── XAUTOCLAIM RESP3 map for fields ─────────────────────────────────────────

test "iter392 - XAUTOCLAIM RESP2 returns flat array for entry fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9807");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "f1", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "10", "STREAMS", "mystream", ">" }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XAUTOCLAIM", "mystream", "grp", "c2", "0", "0-0" });
    defer allocator.free(resp);

    // XAUTOCLAIM returns 3-element array: [next-id, entries, deleted-ids]
    try testing.expect(resp[0] == '*');
    // Contains the entry ID
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    // Fields section uses flat array '*N\r\n' in RESP2
    const field_section = std.mem.indexOf(u8, resp, "1000-0\r\n") orelse return error.NoId;
    const after_id = resp[field_section + "1000-0\r\n".len ..];
    try testing.expect(after_id[0] == '*'); // flat array
}

test "iter392 - XAUTOCLAIM RESP3 returns map for entry fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9808");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "f1", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "10", "STREAMS", "mystream", ">" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XAUTOCLAIM", "mystream", "grp", "c2", "0", "0-0" });
    defer allocator.free(resp);

    // XAUTOCLAIM returns 3-element array: [next-id, entries, deleted-ids]
    try testing.expect(resp[0] == '*');
    // Contains the entry ID
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    // Fields section uses map '%N\r\n' in RESP3
    const field_section = std.mem.indexOf(u8, resp, "1000-0\r\n") orelse return error.NoId;
    const after_id = resp[field_section + "1000-0\r\n".len ..];
    try testing.expect(after_id[0] == '%'); // map type
}

test "iter392 - XAUTOCLAIM JUSTID RESP3 returns only IDs (no field map)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9809");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "f1", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "10", "STREAMS", "mystream", ">" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XAUTOCLAIM", "mystream", "grp", "c2", "0", "0-0", "JUSTID" });
    defer allocator.free(resp);

    // JUSTID returns IDs only, no map
    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    // Should NOT contain '%' map type
    try testing.expect(std.mem.indexOf(u8, resp, "%") == null);
}

test "iter392 - XAUTOCLAIM multiple entries RESP3 all use map fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9810");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "1000-0", "f1", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XADD", "mystream", "2000-0", "f2", "v2" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XGROUP", "CREATE", "mystream", "grp", "0" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "10", "STREAMS", "mystream", ">" }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps,
        &.{ "XAUTOCLAIM", "mystream", "grp", "c2", "0", "0-0" });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '*');
    try testing.expect(std.mem.indexOf(u8, resp, "1000-0") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "2000-0") != null);
    // Both entries should have map fields '%' in RESP3
    try testing.expect(std.mem.indexOf(u8, resp, "%") != null);
}
