// Iteration 386: sailor v2.66.0 + RESP3 map type for COMMAND DOCS
//
// In RESP2, COMMAND DOCS returns a flat array *N where N = count*2 (name, doc_entry alternating).
// In RESP3, COMMAND DOCS returns a map %N where N = count (name → doc_entry map pairs).
//
// Each doc entry in RESP2 is a flat array *M (2M items for M field-value pairs).
// Each doc entry in RESP3 is a map %M (M field-value pairs).
//
// Fields: summary, since, group, complexity (always); doc_flags, replaced_by (optional); arguments (always).
// Basic command (no optional fields): RESP2=*10, RESP3=%5.
// Deprecated command (doc_flags+replaced_by): RESP2=*14, RESP3=%7.

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

// ─── COMMAND DOCS RESP2 flat array ──────────────────────────────────────────

test "iter386 - COMMAND DOCS RESP2 returns flat array for specific command" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8600");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // RESP2 by default — COMMAND DOCS get
    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "get" });
    defer allocator.free(result);

    // RESP2: outer flat array *2\r\n (1 command × 2 items = 2)
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must NOT use map type for outer
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") == null);
    // Doc entry is flat array *10\r\n (5 pairs × 2)
    try testing.expect(std.mem.indexOf(u8, result, "*10\r\n") != null);
    // Must NOT use map type for doc entry
    try testing.expect(std.mem.indexOf(u8, result, "%5\r\n") == null);
    // Must contain field names
    try testing.expect(std.mem.indexOf(u8, result, "summary") != null);
    try testing.expect(std.mem.indexOf(u8, result, "group") != null);
}

// ─── COMMAND DOCS RESP3 outer map ────────────────────────────────────────────

test "iter386 - COMMAND DOCS RESP3 returns outer map for specific command" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8601");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "get" });
    defer allocator.free(result);

    // RESP3: outer map %1\r\n (1 command)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    // Must NOT use flat array for outer
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") == null);
    // Must contain field names
    try testing.expect(std.mem.indexOf(u8, result, "summary") != null);
    try testing.expect(std.mem.indexOf(u8, result, "group") != null);
}

// ─── COMMAND DOCS RESP3 inner doc entry as map ────────────────────────────────

test "iter386 - COMMAND DOCS RESP3 doc entry is a map not flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8602");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "set" });
    defer allocator.free(result);

    // RESP3 inner doc entry: %5\r\n (5 pairs: summary, since, group, complexity, arguments)
    try testing.expect(std.mem.indexOf(u8, result, "%5\r\n") != null);
    // Must NOT use flat array for inner doc
    try testing.expect(std.mem.indexOf(u8, result, "*10\r\n") == null);
    // Field names must be present
    try testing.expect(std.mem.indexOf(u8, result, "summary") != null);
    try testing.expect(std.mem.indexOf(u8, result, "since") != null);
    try testing.expect(std.mem.indexOf(u8, result, "complexity") != null);
    try testing.expect(std.mem.indexOf(u8, result, "arguments") != null);
}

// ─── COMMAND DOCS RESP3 unknown command returns empty map ────────────────────

test "iter386 - COMMAND DOCS RESP3 unknown command returns empty map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8603");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "totally_unknown_command_xyz" });
    defer allocator.free(result);

    // RESP3: empty map %0\r\n (not *0\r\n)
    try testing.expect(std.mem.startsWith(u8, result, "%0\r\n"));
    try testing.expect(!std.mem.startsWith(u8, result, "*0\r\n"));
}

// ─── COMMAND DOCS RESP2 unknown command returns empty array ──────────────────

test "iter386 - COMMAND DOCS RESP2 unknown command returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8604");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "totally_unknown_command_xyz" });
    defer allocator.free(result);

    // RESP2: empty array *0\r\n
    try testing.expect(std.mem.startsWith(u8, result, "*0\r\n"));
}

// ─── COMMAND DOCS RESP3 multiple commands ────────────────────────────────────

test "iter386 - COMMAND DOCS RESP3 multiple commands returns map with multiple entries" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8605");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "get", "set", "hset" });
    defer allocator.free(result);

    // RESP3: outer map %3\r\n (3 commands)
    try testing.expect(std.mem.startsWith(u8, result, "%3\r\n"));
    // Must contain all 3 command names
    try testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try testing.expect(std.mem.indexOf(u8, result, "set") != null);
    try testing.expect(std.mem.indexOf(u8, result, "hset") != null);
}

// ─── COMMAND DOCS RESP3 deprecated command has doc_flags and replaced_by ─────

test "iter386 - COMMAND DOCS RESP3 deprecated command returns map with doc_flags" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8606");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // GETSET is deprecated (has doc_flags + replaced_by)
    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "getset" });
    defer allocator.free(result);

    // RESP3: outer map %1\r\n (1 command)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    // Inner doc entry has 7 pairs: summary, since, group, complexity, doc_flags, replaced_by, arguments
    try testing.expect(std.mem.indexOf(u8, result, "%7\r\n") != null);
    // Must NOT use *14\r\n (RESP2 flat array for 7 pairs)
    try testing.expect(std.mem.indexOf(u8, result, "*14\r\n") == null);
    // Must contain deprecated info
    try testing.expect(std.mem.indexOf(u8, result, "deprecated") != null);
    try testing.expect(std.mem.indexOf(u8, result, "replaced_by") != null);
}

// ─── COMMAND DOCS RESP3 no-args returns map of all commands ──────────────────

test "iter386 - COMMAND DOCS RESP3 no-args returns large map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8607");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS" });
    defer allocator.free(result);

    // RESP3: outer map starts with %N\r\n (N > 0)
    try testing.expect(std.mem.startsWith(u8, result, "%"));
    // Must NOT start with * (flat array)
    try testing.expect(!std.mem.startsWith(u8, result, "*"));
    // Large response covering many commands
    try testing.expect(result.len > 1000);
    // Contains summary and group fields
    try testing.expect(std.mem.indexOf(u8, result, "summary") != null);
    try testing.expect(std.mem.indexOf(u8, result, "group") != null);
}

// ─── COMMAND DOCS RESP2 no-args still returns flat array ─────────────────────

test "iter386 - COMMAND DOCS RESP2 no-args returns flat array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8608");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // RESP2 by default
    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS" });
    defer allocator.free(result);

    // RESP2: starts with *N\r\n
    try testing.expect(std.mem.startsWith(u8, result, "*"));
    // Must NOT start with % (map type)
    try testing.expect(!std.mem.startsWith(u8, result, "%"));
    // Large response
    try testing.expect(result.len > 1000);
}

// ─── COMMAND DOCS RESP3 partial match (1 found, 1 unknown) ───────────────────

test "iter386 - COMMAND DOCS RESP3 partial match returns map with found commands only" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8609");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // Ask for "get" (known) and "unknown_xyz" (not in docs)
    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "DOCS", "get", "unknown_xyz" });
    defer allocator.free(result);

    // RESP3: outer map %1\r\n (only 1 found)
    try testing.expect(std.mem.startsWith(u8, result, "%1\r\n"));
    // Must contain "get"
    try testing.expect(std.mem.indexOf(u8, result, "get") != null);
}
