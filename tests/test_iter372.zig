// Iteration 372: table.unpack Lua compatibility + MONITOR real client address
//
// Tests that:
// 1. table.unpack works in EVAL scripts (Lua 5.2+ compat shim via LuaJIT unpack)
// 2. table.unpack with start/stop indices works correctly
// 3. MONITOR shows correct database for the executing client
// 4. Various Lua scripting edge cases with table library

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const ScriptStore = scripting.ScriptStore;
const transactions_mod = zoltraak.transactions_commands;

fn execCmdWithStore(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| resp_args[i] = .{ .bulk_string = a };
    const cmd = RespValue{ .array = resp_args };
    var tx = transactions_mod.TxState.init(allocator);
    defer tx.deinit();
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
        script_store,
        null,
        &databases,
        1,
    );
}

fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) ![]const u8 {
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    return execCmdWithStore(allocator, storage, client_registry, client_id, ps, &script_store, args);
}

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9201", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ── 1. table.unpack basic functionality ──────────────────────────────────────

test "EVAL table.unpack: basic unpack of array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // table.unpack({10, 20, 30}) should return 10, 20, 30
    // We collect them into a table to return as array
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local t = {10, 20, 30}
        \\return {table.unpack(t)}
        ,
        "0",
    });
    defer allocator.free(result);

    // Result should be an array containing 10, 20, 30
    try testing.expect(std.mem.indexOf(u8, result, "10") != null);
    try testing.expect(std.mem.indexOf(u8, result, "20") != null);
    try testing.expect(std.mem.indexOf(u8, result, "30") != null);
}

// ── 2. table.unpack with start index ─────────────────────────────────────────

test "EVAL table.unpack: with start index" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // table.unpack({10, 20, 30}, 2) returns 20, 30
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local t = {10, 20, 30}
        \\return {table.unpack(t, 2)}
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "20") != null);
    try testing.expect(std.mem.indexOf(u8, result, "30") != null);
    // 10 should NOT be in the result (we start at index 2)
    try testing.expect(std.mem.indexOf(u8, result, ":10\r\n") == null);
}

// ── 3. table.unpack with start and end indices ────────────────────────────────

test "EVAL table.unpack: with start and end indices" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // table.unpack({10, 20, 30}, 1, 2) returns 10, 20
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local t = {10, 20, 30}
        \\return {table.unpack(t, 1, 2)}
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "10") != null);
    try testing.expect(std.mem.indexOf(u8, result, "20") != null);
    // 30 should NOT be in result
    try testing.expect(std.mem.indexOf(u8, result, ":30\r\n") == null);
}

// ── 4. table.unpack on empty table ────────────────────────────────────────────

test "EVAL table.unpack: empty table returns empty" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\return {table.unpack({})}
        ,
        "0",
    });
    defer allocator.free(result);

    // Should return empty array
    try testing.expectEqualStrings("*0\r\n", result);
}

// ── 5. table.unpack equivalence with global unpack ────────────────────────────

test "EVAL table.unpack: equivalent to global unpack" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Both table.unpack and unpack should return the same result
    const result1 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local t = {'a', 'b', 'c'}
        \\return {table.unpack(t)}
        ,
        "0",
    });
    defer allocator.free(result1);

    const result2 = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local t = {'a', 'b', 'c'}
        \\return {unpack(t)}
        ,
        "0",
    });
    defer allocator.free(result2);

    try testing.expectEqualStrings(result1, result2);
}

// ── 6. table.unpack in function spread call ───────────────────────────────────

test "EVAL table.unpack: use as spread for string.format" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local args = {42, "world"}
        \\return string.format("num=%d str=%s", table.unpack(args))
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "num=42 str=world") != null);
}

// ── 7. table.unpack with string values ───────────────────────────────────────

test "EVAL table.unpack: string values returned correctly" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local keys = {"foo", "bar", "baz"}
        \\return {table.unpack(keys)}
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "foo") != null);
    try testing.expect(std.mem.indexOf(u8, result, "bar") != null);
    try testing.expect(std.mem.indexOf(u8, result, "baz") != null);
}

// ── 8. table.unpack used in variadic redis.call pattern ──────────────────────

test "EVAL table.unpack: redis.call with unpacked args" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Common pattern: redis.call(table.unpack(cmd_array))
    // SET mykey myvalue via table.unpack
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local cmd = {'SET', KEYS[1], ARGV[1]}
        \\redis.call(table.unpack(cmd))
        \\return redis.call('GET', KEYS[1])
        ,
        "1",
        "testkey372",
        "testval372",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "testval372") != null);
}

// ── 9. MONITOR shows client database index ────────────────────────────────────

test "MONITOR: client_registry.getSelectedDb is accessible" {
    // This is a unit test to verify the getSelectedDb returns correct value
    // Real MONITOR output testing would require a live server connection
    const allocator = testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:9202", 10, "127.0.0.1:6379");

    // Default DB should be 0
    try testing.expectEqual(@as(u16, 0), registry.getSelectedDb(client_id));
}

// ── 10. MONITOR: client_registry.getClientAddr returns address ────────────────

test "MONITOR: client_registry.getClientAddr returns registered address" {
    const allocator = testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const addr = "127.0.0.1:54321";
    const client_id = try registry.registerClient(addr, 10, "127.0.0.1:6379");

    const returned_addr = try registry.getClientAddr(client_id, allocator);
    defer allocator.free(returned_addr);

    try testing.expectEqualStrings(addr, returned_addr);
}

// ── 11. table.unpack: verify table module has unpack field ────────────────────

test "EVAL table module has unpack field" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Check that table.unpack is a function
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\return type(table.unpack)
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "function") != null);
}

// ── 12. table.unpack: single element table ───────────────────────────────────

test "EVAL table.unpack: single element table" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local first = table.unpack({42})
        \\return first
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "42") != null);
}
