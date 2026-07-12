// Iteration 407: EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT option-compatibility validation
//
// Real Redis rejects incompatible NX/XX/GT/LT combinations on the EXPIRE family
// *before* touching the key:
//   NX combined with XX, GT, or LT -> "ERR NX and XX, GT or LT options at the
//     same time are not compatible"
//   GT combined with LT            -> "ERR GT and LT options at the same time
//     are not compatible"
//
// Zoltraak previously OR'd the option flags together with no mutual-exclusivity
// check, silently accepting invalid combinations and applying data-dependent
// (undocumented) semantics instead of raising a client syntax error.

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

test "iter407 - EXPIRE NX+XX rejected with incompatibility error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7101");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407a", "v" });
    allocator.free(set_result);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "EXPIRE", "e407a", "100", "NX", "XX" });
    defer allocator.free(result);

    try testing.expectEqualStrings("-ERR NX and XX, GT or LT options at the same time are not compatible\r\n", result);

    // Key must remain without a TTL since the command was rejected up-front.
    const ttl_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "TTL", "e407a" });
    defer allocator.free(ttl_result);
    try testing.expectEqualStrings(":-1\r\n", ttl_result);
}

test "iter407 - EXPIRE NX+GT rejected with incompatibility error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7102");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407b", "v" });
    allocator.free(set_result);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "EXPIRE", "e407b", "100", "NX", "GT" });
    defer allocator.free(result);

    try testing.expectEqualStrings("-ERR NX and XX, GT or LT options at the same time are not compatible\r\n", result);
}

test "iter407 - EXPIRE GT+LT rejected with incompatibility error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7103");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407c", "v" });
    allocator.free(set_result);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "EXPIRE", "e407c", "100", "GT", "LT" });
    defer allocator.free(result);

    try testing.expectEqualStrings("-ERR GT and LT options at the same time are not compatible\r\n", result);
}

test "iter407 - EXPIRE with only NX still succeeds (regression)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7104");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407d", "v" });
    allocator.free(set_result);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "EXPIRE", "e407d", "100", "NX" });
    defer allocator.free(result);

    try testing.expectEqualStrings(":1\r\n", result);
}

test "iter407 - PEXPIRE GT+LT rejected with incompatibility error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7105");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407e", "v" });
    allocator.free(set_result);

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "PEXPIRE", "e407e", "100000", "GT", "LT" });
    defer allocator.free(result);

    try testing.expectEqualStrings("-ERR GT and LT options at the same time are not compatible\r\n", result);
}

test "iter407 - EXPIREAT NX+XX rejected with incompatibility error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7106");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407f", "v" });
    allocator.free(set_result);

    const future_ts: i64 = @divTrunc(Storage.getCurrentTimestamp(), 1000) + 1000;
    var ts_buf: [32]u8 = undefined;
    const ts_str = try std.fmt.bufPrint(&ts_buf, "{d}", .{future_ts});

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "EXPIREAT", "e407f", ts_str, "NX", "XX" });
    defer allocator.free(result);

    try testing.expectEqualStrings("-ERR NX and XX, GT or LT options at the same time are not compatible\r\n", result);
}

test "iter407 - PEXPIREAT GT+LT rejected with incompatibility error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "7107");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const set_result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "e407g", "v" });
    allocator.free(set_result);

    const future_ts_ms: i64 = Storage.getCurrentTimestamp() + 1000000;
    var ts_buf: [32]u8 = undefined;
    const ts_str = try std.fmt.bufPrint(&ts_buf, "{d}", .{future_ts_ms});

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "PEXPIREAT", "e407g", ts_str, "GT", "LT" });
    defer allocator.free(result);

    try testing.expectEqualStrings("-ERR GT and LT options at the same time are not compatible\r\n", result);
}
