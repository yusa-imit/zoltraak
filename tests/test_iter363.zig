// Iteration 363: VADD/VSIM/VGETATTR/VSETATTR Redis 8.0 API compliance
//
// Rewrote Vector Set command parsers to use real Redis 8.0 calling conventions:
//   - VADD key [REDUCE dim] (VALUES num | FP32) f1..fn element [SETATTR blob] [opts]
//     Returns 1 if newly added, 0 if updated. One element per call.
//   - VSIM key (ELE element | VALUES num f1..fn) [COUNT n] [WITHSCORES] [WITHATTRIBS] [opts]
//     Returns elements sorted by similarity (descending). Empty array for nonexistent key.
//   - VGETATTR key member  (3-arg form, returns JSON blob or nil)
//   - VSETATTR key member blob  (4-arg form, stores blob)
//   - VSETATTR added to ALL_COMMANDS (arity 4)
//   - VADD arity updated from -5 to -6
//   - VGETATTR arity updated from 4 to 3
// COMMAND COUNT is now 270 (was 269, +1 for VSETATTR).

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

fn execDiscard(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) !void {
    const r = try execCmd(allocator, storage, client_registry, client_id, ps, args);
    allocator.free(r);
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

test "iter363 - COMMAND COUNT increased to 270" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "COUNT" });
    defer allocator.free(result);
    try testing.expectEqualStrings(":270\r\n", result);
}

test "iter363 - VADD Redis 8.0 syntax: VALUES num f1..fn element" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    // First add: returns 1 (new element)
    const r1 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "vec1" });
    defer allocator.free(r1);
    try testing.expectEqualStrings(":1\r\n", r1);
}

test "iter363 - VADD update existing element returns 0" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const r1 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" });
    defer allocator.free(r1);
    try testing.expectEqualStrings(":1\r\n", r1);

    // Update same element: returns 0
    const r2 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v1" });
    defer allocator.free(r2);
    try testing.expectEqualStrings(":0\r\n", r2);
}

test "iter363 - VADD dimensionality mismatch error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const r1 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "v1" });
    defer allocator.free(r1);

    // Wrong dimension (2 instead of 3)
    const r2 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v2" });
    defer allocator.free(r2);
    try testing.expect(std.mem.startsWith(u8, r2, "-ERR"));
}

test "iter363 - VADD SETATTR option" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const r1 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1", "SETATTR", "{\"label\":\"test\"}" });
    defer allocator.free(r1);
    try testing.expectEqualStrings(":1\r\n", r1);

    // Retrieve attribute
    const r2 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VGETATTR", "myvec", "v1" });
    defer allocator.free(r2);
    try testing.expect(std.mem.indexOf(u8, r2, "label") != null);
}

test "iter363 - VCARD after multiple adds" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" });
    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" });
    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "5.0", "6.0", "v3" });

    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VCARD", "myvec" });
    defer allocator.free(r);
    try testing.expectEqualStrings(":3\r\n", r);
}

test "iter363 - VSIM ELE basic search" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" });
    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "0.0", "1.0", "v2" });

    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VSIM", "myvec", "ELE", "v1", "COUNT", "2" });
    defer allocator.free(r);
    // Should return an array with v1 at top (score 1.0 — identical to itself)
    try testing.expect(std.mem.indexOf(u8, r, "v1") != null);
}

test "iter363 - VSIM nonexistent key returns empty array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VSIM", "nonexistent", "ELE", "v1" });
    defer allocator.free(r);
    try testing.expectEqualStrings("*0\r\n", r);
}

test "iter363 - VSIM VALUES query" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" });
    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "0.0", "1.0", "v2" });

    // Query with vector close to v1
    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VSIM", "myvec", "VALUES", "2", "0.99", "0.01", "COUNT", "1" });
    defer allocator.free(r);
    // Should return v1 as the closest match
    try testing.expect(std.mem.indexOf(u8, r, "v1") != null);
}

test "iter363 - VSIM WITHSCORES returns flat array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" });

    // WITHSCORES: returns [elem, score] flat array (2 elements for 1 result)
    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VSIM", "myvec", "ELE", "v1", "COUNT", "1", "WITHSCORES" });
    defer allocator.free(r);
    // Array header *2 for 1 result with score
    try testing.expect(std.mem.startsWith(u8, r, "*2\r\n"));
}

test "iter363 - VGETATTR Redis 8.0 3-arg form" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" });
    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VSETATTR", "myvec", "v1", "{\"score\":0.9}" });

    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VGETATTR", "myvec", "v1" });
    defer allocator.free(r);
    try testing.expect(std.mem.indexOf(u8, r, "score") != null);
}

test "iter363 - VGETATTR nonexistent member returns nil" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" });

    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VGETATTR", "myvec", "noexist" });
    defer allocator.free(r);
    try testing.expectEqualStrings("$-1\r\n", r);
}

test "iter363 - VSETATTR Redis 8.0 4-arg form" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    try execDiscard(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" });

    const r = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "VSETATTR", "myvec", "v1", "{\"x\":1}" });
    defer allocator.free(r);
    try testing.expectEqualStrings(":1\r\n", r);
}

test "iter363 - COMMAND INFO vadd has correct arity -6" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "COMMAND", "INFO", "vadd" });
    defer allocator.free(result);
    try testing.expect(std.mem.indexOf(u8, result, ":-6\r\n") != null);
}

test "iter363 - COMMAND INFO vgetattr has correct arity 3" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "COMMAND", "INFO", "vgetattr" });
    defer allocator.free(result);
    try testing.expect(std.mem.indexOf(u8, result, ":3\r\n") != null);
}

test "iter363 - COMMAND INFO vsetattr exists" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps,
        &.{ "COMMAND", "INFO", "vsetattr" });
    defer allocator.free(result);
    // Should not return nil array — vsetattr now in ALL_COMMANDS
    try testing.expect(std.mem.indexOf(u8, result, "vsetattr") != null);
}
