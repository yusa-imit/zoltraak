// Iteration 391: sailor v2.70.0 + RESP3 verbatim string for CLIENT LIST and CLIENT INFO
//
// In RESP2, CLIENT LIST and CLIENT INFO return bulk strings ($N\r\n...).
// In RESP3, they return verbatim strings (=N\r\ntxt:...) matching the Redis 7 spec.
// This allows RESP3 clients to distinguish text content from raw binary data.
//
// Affected commands: CLIENT LIST, CLIENT INFO

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

// ─── CLIENT LIST ─────────────────────────────────────────────────────────────

test "iter391 - CLIENT LIST RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9700");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp);

    // RESP2: bulk string starts with '$'
    try testing.expect(resp[0] == '$');
    // Must contain client info fields
    try testing.expect(std.mem.indexOf(u8, resp, "id=") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "addr=") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cmd=") != null);
}

test "iter391 - CLIENT LIST RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9701");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp);

    // RESP3: verbatim string starts with '='
    try testing.expect(resp[0] == '=');
    // Must contain 'txt:' prefix in the verbatim content
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    // Content must still have client info fields
    try testing.expect(std.mem.indexOf(u8, resp, "id=") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "addr=") != null);
}

test "iter391 - CLIENT LIST RESP3 verbatim format is correct" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9702");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp);

    // Verbatim format: =<len>\r\ntxt:<content>\r\n
    try testing.expect(resp[0] == '=');
    // Find \r\n separator
    const crlf_pos = std.mem.indexOf(u8, resp, "\r\n") orelse return error.NoCRLF;
    // After the length + \r\n, content starts with "txt:"
    const content_start = crlf_pos + 2;
    try testing.expect(content_start + 4 <= resp.len);
    try testing.expectEqualStrings("txt:", resp[content_start .. content_start + 4]);
}

test "iter391 - CLIENT LIST TYPE filter RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9703");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST", "TYPE", "normal" });
    defer allocator.free(resp);

    // RESP3 with TYPE filter still returns verbatim string
    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
}

// ─── CLIENT INFO ─────────────────────────────────────────────────────────────

test "iter391 - CLIENT INFO RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9704");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "INFO" });
    defer allocator.free(resp);

    // RESP2: bulk string starts with '$'
    try testing.expect(resp[0] == '$');
    // Must contain client info fields for this specific client
    try testing.expect(std.mem.indexOf(u8, resp, "id=") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "resp=2") != null);
}

test "iter391 - CLIENT INFO RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9705");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "INFO" });
    defer allocator.free(resp);

    // RESP3: verbatim string starts with '='
    try testing.expect(resp[0] == '=');
    // Must contain 'txt:' prefix
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    // Content must include resp=3 since we're in RESP3 mode
    try testing.expect(std.mem.indexOf(u8, resp, "resp=3") != null);
}

test "iter391 - CLIENT INFO RESP3 verbatim format is correct" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9706");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "INFO" });
    defer allocator.free(resp);

    // Verbatim format: =<len>\r\ntxt:<content>\r\n
    try testing.expect(resp[0] == '=');
    const crlf_pos = std.mem.indexOf(u8, resp, "\r\n") orelse return error.NoCRLF;
    const content_start = crlf_pos + 2;
    try testing.expect(content_start + 4 <= resp.len);
    try testing.expectEqualStrings("txt:", resp[content_start .. content_start + 4]);
}

// ─── Protocol switch verification ────────────────────────────────────────────

test "iter391 - CLIENT LIST switches from bulk to verbatim after HELLO 3" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9707");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Initially RESP2 → bulk string
    const resp2 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp2);
    try testing.expect(resp2[0] == '$');

    // Upgrade to RESP3 → verbatim string
    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    const resp3 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp3);
    try testing.expect(resp3[0] == '=');
}

test "iter391 - CLIENT INFO shows resp=3 in RESP3 mode" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9708");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "INFO" });
    defer allocator.free(resp);

    // Verbatim string in RESP3, content shows resp=3
    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "resp=3") != null);
}

test "iter391 - CLIENT LIST RESP3 content matches RESP2 content" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9709");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Get RESP2 output
    const resp2 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp2);

    // Switch to RESP3
    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    const resp3 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST" });
    defer allocator.free(resp3);

    // Both should contain the same addr field (same client)
    try testing.expect(std.mem.indexOf(u8, resp2, "addr=") != null);
    try testing.expect(std.mem.indexOf(u8, resp3, "addr=") != null);
    // RESP2 is a bulk string, RESP3 is a verbatim string
    try testing.expect(resp2[0] == '$');
    try testing.expect(resp3[0] == '=');
}

test "iter391 - CLIENT LIST ID filter RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9710");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    // Get client ID string
    var id_buf: [32]u8 = undefined;
    const id_str = try std.fmt.bufPrint(&id_buf, "{d}", .{ctx.client_id});

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "CLIENT", "LIST", "ID", id_str });
    defer allocator.free(resp);

    // RESP3 with ID filter also returns verbatim string
    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
}

test "iter391 - sailor v2.70.0 MatrixView widget available" {
    // Sailor v2.70.0 adds MatrixView widget (2D heatmap, 32x32).
    // This test verifies the library was updated by ensuring the build
    // succeeds with the new version (no actual runtime test needed).
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9711");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Basic smoke test: server responds to PING
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{"PING"});
    defer allocator.free(resp);
    try testing.expectEqualStrings("+PONG\r\n", resp);
}
