// Iteration 356: SPOP key 0 fix + sailor v2.40.0
//
// Bug: `SPOP key 0` incorrectly popped 1 element instead of returning empty array.
// Root cause: storage.spop() uses count=0 as a sentinel for "single pop mode",
// so SPOP key 0 was treated as SPOP key (pop 1). Fixed by early-returning in
// cmdSpop when has_count=true and count=0.
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
    for (args, 0..) |a, i| {
        resp_args[i] = .{ .bulk_string = a };
    }
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

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9200", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ── SPOP key 0 fix tests ─────────────────────────────────────────────────────

test "iter356 - SPOP key 0 returns empty array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // Populate set
    const sadd = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "myset", "a", "b", "c" });
    defer allocator.free(sadd);
    try testing.expectEqualStrings(":3\r\n", sadd);

    // SPOP key 0 must return empty array
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "myset", "0" });
    defer allocator.free(result);
    try testing.expectEqualStrings("*0\r\n", result);
}

test "iter356 - SPOP key 0 does not remove any elements" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const sadd = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "myset2", "x", "y", "z" });
    defer allocator.free(sadd);

    // SPOP key 0 — must not remove elements
    const spop_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "myset2", "0" });
    defer allocator.free(spop_result);
    try testing.expectEqualStrings("*0\r\n", spop_result);

    // Verify set still has all 3 elements
    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SCARD", "myset2" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":3\r\n", card);
}

test "iter356 - SPOP nonexistent key 0 returns empty array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "nosuchkey", "0" });
    defer allocator.free(result);
    try testing.expectEqualStrings("*0\r\n", result);
}

test "iter356 - SPOP key count still works for count > 0" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const sadd = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "myset3", "p", "q", "r", "s" });
    defer allocator.free(sadd);
    try testing.expectEqualStrings(":4\r\n", sadd);

    // Pop 2 elements — result is an array of 2 bulk strings
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "myset3", "2" });
    defer allocator.free(result);
    // Should be an array response, not empty
    try testing.expect(result.len > 4);
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));

    // Set should have 2 elements remaining
    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SCARD", "myset3" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":2\r\n", card);
}

test "iter356 - SPOP key without count still pops 1 element as bulk string" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const sadd = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "myset4", "only" });
    defer allocator.free(sadd);

    // SPOP without count argument — must return a bulk string (not array)
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "myset4" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "$"));
    try testing.expect(std.mem.indexOf(u8, result, "only") != null);

    // Set should now be empty
    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SCARD", "myset4" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":0\r\n", card);
}

test "iter356 - SPOP key count larger than set returns all elements" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const sadd = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "myset5", "a", "b" });
    defer allocator.free(sadd);

    // Request 10 from a set of 2 — should return all 2
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "myset5", "10" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));

    const card = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SCARD", "myset5" });
    defer allocator.free(card);
    try testing.expectEqualStrings(":0\r\n", card);
}

test "iter356 - SPOP negative count returns error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const sadd6 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SADD", "myset6", "a" });
    defer allocator.free(sadd6);

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "SPOP", "myset6", "-1" });
    defer allocator.free(result);
    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}
