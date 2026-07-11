// Iteration 404: RESP3 map type for ACL GETUSER
//
// In RESP2, ACL GETUSER returns a flat 12-element array:
//   *12\r\n $5\r\nflags\r\n ... $9\r\nselectors\r\n *0\r\n
//
// In RESP3 (after HELLO 3), Redis 7.0+ returns a 6-pair map instead,
// matching the pattern already used by CONFIG GET (iteration 284):
//   %6\r\n $5\r\nflags\r\n ... $9\r\nselectors\r\n *0\r\n

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

test "iter404 - ACL GETUSER RESP2 returns flat 12-element array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7001");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ACL", "GETUSER", "default" });
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "*12\r\n"));
}

test "iter404 - ACL GETUSER RESP3 returns 6-pair map" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7002");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Switch to RESP3
    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    allocator.free(hello);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ACL", "GETUSER", "default" });
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "%6\r\n"));
    // Must NOT contain the RESP2 flat-array header for this response
    try testing.expect(!std.mem.startsWith(u8, result, "*12\r\n"));
}

test "iter404 - ACL GETUSER RESP3 with real ACL store user preserves nested arrays" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7003");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    allocator.free(hello);

    const setuser = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ACL", "SETUSER", "iter404user", "on", ">pw123", "~cache:*", "+@all" });
    allocator.free(setuser);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ACL", "GETUSER", "iter404user" });
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "%6\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "~cache:*") != null);
    try testing.expect(std.mem.indexOf(u8, result, "#pw123") != null);
    try testing.expect(std.mem.indexOf(u8, result, "+@all") != null);
}

test "iter404 - ACL GETUSER RESP3 nonexistent user still returns null bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7004");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const hello = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" });
    allocator.free(hello);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "ACL", "GETUSER", "nonexistent_iter404" });
    defer allocator.free(result);

    try testing.expectEqualStrings("$-1\r\n", result);
}
