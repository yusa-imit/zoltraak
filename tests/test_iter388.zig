// Iteration 388: sailor v2.67.0 + RESP3 set type for COMMAND LIST + RESP3 map type for COMMAND/COMMAND INFO
//
// COMMAND LIST RESP2: *N\r\n array of bulk string command names
// COMMAND LIST RESP3: ~N\r\n set of bulk string command names (names are unique → set semantics)
//
// COMMAND INFO RESP2: outer *N\r\n array, each entry is *10\r\n array
//   elements: name, arity, flags(*N), first_key, last_key, step, acl_cats(*N), tips(*0), key-specs(*0), subcmds(*0)
// COMMAND INFO RESP3: outer *N\r\n array, each entry is %10\r\n map
//   keys: name, arity, flags(~N set), first-key, last-key, step, acl-categories(~N set), tips(*0), key-specifications(*0), subcommands(*0)
//
// COMMAND RESP2: *N\r\n array, each entry is *10\r\n array
// COMMAND RESP3: *N\r\n array, each entry is %10\r\n map (flags/acl-categories as sets)

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

// ─── COMMAND LIST RESP2 ──────────────────────────────────────────────────────

test "iter388 - COMMAND LIST RESP2 returns array *N" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8800");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "LIST" });
    defer allocator.free(result);

    // RESP2: starts with * (array type)
    try testing.expect(std.mem.startsWith(u8, result, "*"));
    // Must NOT be set type
    try testing.expect(!std.mem.startsWith(u8, result, "~"));
    // Must contain "get" and "set" as bulk strings
    try testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try testing.expect(std.mem.indexOf(u8, result, "set") != null);
}

// ─── COMMAND LIST RESP3 ──────────────────────────────────────────────────────

test "iter388 - COMMAND LIST RESP3 returns set ~N" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8801");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "LIST" });
    defer allocator.free(result);

    // RESP3: starts with ~ (set type)
    try testing.expect(std.mem.startsWith(u8, result, "~"));
    // Must NOT use array type
    try testing.expect(!std.mem.startsWith(u8, result, "*"));
    // Must contain "get" and "set"
    try testing.expect(std.mem.indexOf(u8, result, "get") != null);
    try testing.expect(std.mem.indexOf(u8, result, "set") != null);
}

test "iter388 - COMMAND LIST RESP3 FILTERBY PATTERN returns set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8802");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "LIST", "FILTERBY", "PATTERN", "h*" });
    defer allocator.free(result);

    // RESP3: set type
    try testing.expect(std.mem.startsWith(u8, result, "~"));
    // Should contain hset, hget, etc.
    try testing.expect(std.mem.indexOf(u8, result, "hset") != null);
    try testing.expect(std.mem.indexOf(u8, result, "hget") != null);
}

test "iter388 - COMMAND LIST RESP3 MODULE filter returns empty set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8803");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "LIST", "FILTERBY", "MODULE", "nonexistent" });
    defer allocator.free(result);

    // RESP3: empty set
    try testing.expectEqualStrings("~0\r\n", result);
}

// ─── COMMAND INFO RESP2 ──────────────────────────────────────────────────────

test "iter388 - COMMAND INFO RESP2 returns array per entry" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8804");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "INFO", "get" });
    defer allocator.free(result);

    // RESP2: outer *1\r\n, each entry *10\r\n
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "*10\r\n") != null);
    // Flags as array
    try testing.expect(std.mem.indexOf(u8, result, "+readonly\r\n") != null);
    // Must NOT use map format
    try testing.expect(std.mem.indexOf(u8, result, "%10\r\n") == null);
}

// ─── COMMAND INFO RESP3 ──────────────────────────────────────────────────────

test "iter388 - COMMAND INFO RESP3 returns map per entry" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8805");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "INFO", "get" });
    defer allocator.free(result);

    // RESP3: outer *1\r\n, each entry %10\r\n (map)
    try testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "%10\r\n") != null);
    // Map key: "name"
    try testing.expect(std.mem.indexOf(u8, result, "$4\r\nname\r\n") != null);
    // Map key: "flags" with set value
    try testing.expect(std.mem.indexOf(u8, result, "$5\r\nflags\r\n~") != null);
    // Map key: "first-key" (hyphenated)
    try testing.expect(std.mem.indexOf(u8, result, "$9\r\nfirst-key\r\n") != null);
    // Map key: "last-key" (hyphenated)
    try testing.expect(std.mem.indexOf(u8, result, "$8\r\nlast-key\r\n") != null);
    // Map key: "acl-categories" (hyphenated) with set value
    try testing.expect(std.mem.indexOf(u8, result, "$14\r\nacl-categories\r\n~") != null);
    // Must NOT use RESP2 array entry format
    try testing.expect(std.mem.indexOf(u8, result, "*10\r\n") == null);
}

test "iter388 - COMMAND INFO RESP3 flags are set type" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8806");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "INFO", "get" });
    defer allocator.free(result);

    // flags key followed immediately by set header ~N
    try testing.expect(std.mem.indexOf(u8, result, "$5\r\nflags\r\n~") != null);
    // GET has readonly and fast flags → ~2\r\n
    try testing.expect(std.mem.indexOf(u8, result, "~2\r\n") != null);
}

test "iter388 - COMMAND INFO RESP3 unknown command returns nil" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8807");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "INFO", "no_such_command_xyz" });
    defer allocator.free(result);

    // Even in RESP3, unknown commands return nil bulk string
    try testing.expect(std.mem.indexOf(u8, result, "$-1\r\n") != null);
}

test "iter388 - COMMAND INFO RESP3 multiple commands return maps" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8808");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "COMMAND", "INFO", "get", "hset", "zadd" });
    defer allocator.free(result);

    // outer *3\r\n
    try testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    // All 3 entries use %10\r\n map format
    var count: usize = 0;
    var remaining = result;
    while (std.mem.indexOf(u8, remaining, "%10\r\n")) |pos| {
        count += 1;
        remaining = remaining[pos + 5 ..];
    }
    try testing.expectEqual(@as(usize, 3), count);
}

// ─── COMMAND (plain) RESP3 ───────────────────────────────────────────────────

test "iter388 - COMMAND plain RESP3 uses map entries" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8809");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{"COMMAND"});
    defer allocator.free(result);

    // RESP3: uses %10\r\n map format for each entry
    try testing.expect(std.mem.indexOf(u8, result, "%10\r\n") != null);
    // Map key names present
    try testing.expect(std.mem.indexOf(u8, result, "$4\r\nname\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$5\r\nflags\r\n~") != null);
    try testing.expect(std.mem.indexOf(u8, result, "$9\r\nfirst-key\r\n") != null);
    // Must NOT use RESP2 array format for entries
    try testing.expect(std.mem.indexOf(u8, result, "*10\r\n") == null);
}
