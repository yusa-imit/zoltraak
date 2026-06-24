// Iteration 374: Sailor v2.56.0 migration + Redis compatibility fixes
//
// Three bugs fixed:
// 1. CLIENT NO-EVICT / NO-TOUCH: calling with no argument returned current status
//    (non-standard). Redis 7.x requires exactly one argument (ON or OFF). Fixed
//    by changing `args.len > 2` to `args.len != 2` and removing the no-arg branch.
// 2. LPOS RANK negative: positions were reversed to ascending order before
//    returning. Redis spec says negative-RANK results are in descending order
//    (tail→head). Removed `std.mem.reverse` from the backward-scan branch.
// 3. SINTERCARD unknown options: extra tokens after the key list were silently
//    ignored. Redis rejects unknown options with "ERR syntax error". Added `else`
//    branch in the option-parse loop.

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
const sets_cmds = zoltraak.sets_commands;
const list_cmds = zoltraak.lists_commands;

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

// ─── CLIENT NO-EVICT arity fix ────────────────────────────────────────────────

test "iter374 - CLIENT NO-EVICT with no arg returns arity error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9101", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "CLIENT", "NO-EVICT" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR wrong number") != null);
}

test "iter374 - CLIENT NO-EVICT ON returns OK" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9102", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "CLIENT", "NO-EVICT", "ON" });
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
}

test "iter374 - CLIENT NO-EVICT with extra arg returns arity error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9103", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "CLIENT", "NO-EVICT", "ON", "EXTRA" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR wrong number") != null);
}

// ─── CLIENT NO-TOUCH arity fix ───────────────────────────────────────────────

test "iter374 - CLIENT NO-TOUCH with no arg returns arity error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9104", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "CLIENT", "NO-TOUCH" });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR wrong number") != null);
}

test "iter374 - CLIENT NO-TOUCH OFF returns OK" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9105", 10, "127.0.0.1:6379");
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "CLIENT", "NO-TOUCH", "OFF" });
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
}

// ─── LPOS negative RANK ordering fix ─────────────────────────────────────────

test "iter374 - LPOS RANK -1 COUNT 0 returns positions in descending order" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build list: [a, b, c, a, b, c, a] → indices 0-6
    _ = try storage.rpush("mylist", &[_][]const u8{ "a", "b", "c", "a", "b", "c", "a" }, null);

    // RANK -1 COUNT 0 → search from tail, return ALL occurrences of "a" in descending order
    // Expected: 6, 3, 0 (tail to head)
    const positions = try storage.lpos(allocator, "mylist", "a", -1, 0, 0);
    defer allocator.free(positions);

    try testing.expectEqual(@as(usize, 3), positions.len);
    try testing.expectEqual(@as(usize, 6), positions[0]);
    try testing.expectEqual(@as(usize, 3), positions[1]);
    try testing.expectEqual(@as(usize, 0), positions[2]);
}

test "iter374 - LPOS RANK -2 COUNT 0 skips first match from tail" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // list: [c, b, a, c, b, a, c] → indices 0-6
    _ = try storage.rpush("mylist2", &[_][]const u8{ "c", "b", "a", "c", "b", "a", "c" }, null);

    // RANK -2 COUNT 0 → skip first "c" from tail (index 6), return remaining "c" at 3 and 0
    const positions = try storage.lpos(allocator, "mylist2", "c", -2, 0, 0);
    defer allocator.free(positions);

    try testing.expectEqual(@as(usize, 2), positions.len);
    try testing.expectEqual(@as(usize, 3), positions[0]);
    try testing.expectEqual(@as(usize, 0), positions[1]);
}

test "iter374 - LPOS RANK 1 COUNT 0 still returns ascending order" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.rpush("mylist3", &[_][]const u8{ "x", "y", "x", "y", "x" }, null);

    // Positive RANK: ascending order (head to tail) — unchanged behavior
    const positions = try storage.lpos(allocator, "mylist3", "x", 1, 0, 0);
    defer allocator.free(positions);

    try testing.expectEqual(@as(usize, 3), positions.len);
    try testing.expectEqual(@as(usize, 0), positions[0]);
    try testing.expectEqual(@as(usize, 2), positions[1]);
    try testing.expectEqual(@as(usize, 4), positions[2]);
}

// ─── SINTERCARD unknown option rejection fix ─────────────────────────────────

test "iter374 - SINTERCARD with unknown option returns syntax error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    _ = try storage.sadd("myset", &[_][]const u8{"a"}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SINTERCARD" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "myset" },
        .{ .bulk_string = "UNKNOWN" },
    };
    const result = try sets_cmds.cmdSintercard(allocator, storage, &args);
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR syntax error") != null);
}

test "iter374 - SINTERCARD with LIMIT then garbage returns syntax error" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    _ = try storage.sadd("myset2", &[_][]const u8{"a"}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SINTERCARD" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "myset2" },
        .{ .bulk_string = "LIMIT" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "GARBAGE" },
    };
    const result = try sets_cmds.cmdSintercard(allocator, storage, &args);
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR syntax error") != null);
}

test "iter374 - SINTERCARD valid LIMIT still works" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    _ = try storage.sadd("setA", &[_][]const u8{ "a", "b", "c" }, null);
    _ = try storage.sadd("setB", &[_][]const u8{ "a", "b", "d" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SINTERCARD" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "setA" },
        .{ .bulk_string = "setB" },
        .{ .bulk_string = "LIMIT" },
        .{ .bulk_string = "1" },
    };
    const result = try sets_cmds.cmdSintercard(allocator, storage, &args);
    defer allocator.free(result);

    // intersection of {a,b,c} and {a,b,d} is {a,b} → cardinality 2, but LIMIT 1
    try testing.expectEqualStrings(":1\r\n", result);
}
