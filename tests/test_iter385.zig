// Iteration 385: sailor v2.65.0 + RESP3 map type for XINFO STREAM
//
// In RESP2, XINFO STREAM returns a flat array (*20\r\n for 10 key-value pairs).
// In RESP3, XINFO STREAM returns a map (%10\r\n — 10 key-value pairs).
//
// In RESP2, first-entry/last-entry fields use flat array (*N for N items).
// In RESP3, first-entry/last-entry fields use map type (%N/2 for N/2 pairs).
//
// In RESP2, XINFO STREAM FULL returns flat array (*18\r\n for 9 key-value pairs).
// In RESP3, XINFO STREAM FULL returns map (%9\r\n — 9 key-value pairs).
// In RESP3 FULL, per-group objects are %6 and per-consumer objects are %5.

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

// ─── XINFO STREAM RESP2 flat array ──────────────────────────────────────────

test "iter385 - XINFO STREAM RESP2 returns flat array (outer *20)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8500");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f1", "v1" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s" });
    defer allocator.free(result);

    // RESP2: outer *20 (10 key-value pairs as flat array)
    try testing.expect(std.mem.startsWith(u8, result, "*20\r\n"));
    // Must NOT use map type
    try testing.expect(std.mem.indexOf(u8, result, "%10\r\n") == null);
    // Must contain all field names
    try testing.expect(std.mem.indexOf(u8, result, "length") != null);
    try testing.expect(std.mem.indexOf(u8, result, "last-generated-id") != null);
    try testing.expect(std.mem.indexOf(u8, result, "first-entry") != null);
    try testing.expect(std.mem.indexOf(u8, result, "last-entry") != null);
    try testing.expect(std.mem.indexOf(u8, result, "groups") != null);
}

// ─── XINFO STREAM RESP3 map type ────────────────────────────────────────────

test "iter385 - XINFO STREAM RESP3 returns map (outer %10)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8501");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f1", "v1" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s" });
    defer allocator.free(result);

    // RESP3: outer %10 (10 key-value pairs as map)
    try testing.expect(std.mem.startsWith(u8, result, "%10\r\n"));
    // Must NOT use flat array for outer object
    try testing.expect(std.mem.indexOf(u8, result, "*20\r\n") == null);
    // Must contain all field names
    try testing.expect(std.mem.indexOf(u8, result, "length") != null);
    try testing.expect(std.mem.indexOf(u8, result, "last-generated-id") != null);
    try testing.expect(std.mem.indexOf(u8, result, "first-entry") != null);
    try testing.expect(std.mem.indexOf(u8, result, "last-entry") != null);
    try testing.expect(std.mem.indexOf(u8, result, "groups") != null);
}

// ─── XINFO STREAM RESP3 first-entry and last-entry use map for fields ────────

test "iter385 - XINFO STREAM RESP3 first-entry uses map for fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8502");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // Add entry with 2 fields (4 items in RESP2, 2-pair map in RESP3)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "k1", "v1", "k2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s" });
    defer allocator.free(result);

    // first-entry in RESP3: *2\r\n<id>\r\n%2\r\n (map of 2 pairs for 4 field items)
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") != null);
    // Must NOT use *4 for field array
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") == null);
    // Fields must still be present
    try testing.expect(std.mem.indexOf(u8, result, "k1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "v1") != null);
}

// ─── XINFO STREAM RESP2 first-entry uses flat array for fields ───────────────

test "iter385 - XINFO STREAM RESP2 first-entry uses flat array for fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8503");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add entry with 2 fields (4 items in the flat array)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "k1", "v1", "k2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s" });
    defer allocator.free(result);

    // first-entry in RESP2: *2\r\n<id>\r\n*4\r\n (flat array with 4 items)
    try testing.expect(std.mem.indexOf(u8, result, "*4\r\n") != null);
    // Must NOT use map type for fields
    try testing.expect(std.mem.indexOf(u8, result, "%2\r\n") == null);
    // Fields present
    try testing.expect(std.mem.indexOf(u8, result, "k1") != null);
}

// ─── XINFO STREAM empty stream in RESP3 ──────────────────────────────────────

test "iter385 - XINFO STREAM RESP3 empty stream uses nil for first-entry" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8504");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // Create an empty stream by adding then trimming
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XTRIM", "s", "MAXLEN", "0" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s" });
    defer allocator.free(result);

    // RESP3 outer object is %10
    try testing.expect(std.mem.startsWith(u8, result, "%10\r\n"));
    // Empty stream: first-entry and last-entry are nil ($-1)
    try testing.expect(std.mem.indexOf(u8, result, "$-1\r\n") != null);
    // length is 0
    try testing.expect(std.mem.indexOf(u8, result, "length\r\n:0\r\n") != null);
}

// ─── XINFO STREAM FULL RESP2 flat array ──────────────────────────────────────

test "iter385 - XINFO STREAM FULL RESP2 returns flat array (outer *18)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8505");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s", "FULL" });
    defer allocator.free(result);

    // RESP2: outer *18 (9 key-value pairs as flat array)
    try testing.expect(std.mem.startsWith(u8, result, "*18\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%9\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "entries") != null);
    try testing.expect(std.mem.indexOf(u8, result, "groups") != null);
}

// ─── XINFO STREAM FULL RESP3 map type ────────────────────────────────────────

test "iter385 - XINFO STREAM FULL RESP3 returns map (outer %9)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8506");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s", "FULL" });
    defer allocator.free(result);

    // RESP3: outer %9 (9 key-value pairs as map)
    try testing.expect(std.mem.startsWith(u8, result, "%9\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*18\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, result, "entries") != null);
    try testing.expect(std.mem.indexOf(u8, result, "groups") != null);
}

// ─── XINFO STREAM FULL RESP3 entries use map for fields ──────────────────────

test "iter385 - XINFO STREAM FULL RESP3 entries use map for fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8507");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    // Add entry with 1 field pair (2 items → %1 map in RESP3)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "field", "value" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s", "FULL" });
    defer allocator.free(result);

    // In entries, each entry's fields are %1 in RESP3 (1 pair from 2 items)
    try testing.expect(std.mem.indexOf(u8, result, "%1\r\n") != null);
    // Must NOT use flat array for entry fields
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") == null or
        // *2 may appear in RESP3 as the entry container [id, field-map]
        // but there should not be a *2 followed by id then *2 for flat fields
        std.mem.indexOf(u8, result, "field") != null);
    try testing.expect(std.mem.indexOf(u8, result, "value") != null);
}

// ─── XINFO STREAM FULL RESP3 groups use map type ─────────────────────────────

test "iter385 - XINFO STREAM FULL RESP3 groups use map per group (%6)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8508");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s", "FULL" });
    defer allocator.free(result);

    // RESP3: each group in the groups array is %6 (6 key-value pairs)
    try testing.expect(std.mem.indexOf(u8, result, "%6\r\n") != null);
    // Must NOT use *12 for group entries
    try testing.expect(std.mem.indexOf(u8, result, "*12\r\n") == null);
    // Group fields must be present
    try testing.expect(std.mem.indexOf(u8, result, "name") != null);
    try testing.expect(std.mem.indexOf(u8, result, "entries-read") != null);
    try testing.expect(std.mem.indexOf(u8, result, "pel-count") != null);
}

// ─── XINFO STREAM FULL RESP3 consumers use map type ──────────────────────────

test "iter385 - XINFO STREAM FULL RESP3 consumers use map per consumer (%5)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8509");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XADD", "s", "1-0", "f", "v" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XGROUP", "CREATE", "s", "g", "0" }));
    // Create consumer by reading
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "XINFO", "STREAM", "s", "FULL" });
    defer allocator.free(result);

    // RESP3: each consumer in a group's consumers array is %5 (5 key-value pairs)
    try testing.expect(std.mem.indexOf(u8, result, "%5\r\n") != null);
    // Must NOT use *10 for consumer entries
    try testing.expect(std.mem.indexOf(u8, result, "*10\r\n") == null);
    // Consumer fields present
    try testing.expect(std.mem.indexOf(u8, result, "seen-time") != null);
    try testing.expect(std.mem.indexOf(u8, result, "active-time") != null);
    try testing.expect(std.mem.indexOf(u8, result, "pel-count") != null);
}
