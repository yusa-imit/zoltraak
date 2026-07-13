// Iteration 411: sailor v2.83.1 migration (patch release, Windows stability fixes)
//
// sailor v2.83.1 fixes three Windows-only bugs (SystemClipboard.read() trailing
// newline, env.zig empty-string test skip, readByte() permanently blocking on
// piped stdin). No new widgets, no public API changes. This iteration verifies
// the dependency bump is transparent to Zoltraak: existing sailor.tui.widgets
// symbols still resolve at compile time, and core Redis commands regress-test
// clean against the bumped dependency graph (sailor is linked into the TUI/CLI
// binaries that share the same build graph as the server).

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");
const sailor = @import("sailor");

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

test "iter411 - sailor v2.83.1 tui.widgets namespace unchanged (CandlestickChart still resolves)" {
    // Compile-time check: if sailor's widget module layout changed between
    // v2.83.0 and v2.83.1, this line fails to compile.
    const W = sailor.tui.widgets.CandlestickChart;
    try testing.expect(@sizeOf(W) >= 0);
}

test "iter411 - sailor v2.83.1 tui.widgets namespace unchanged (BoxPlot/ViolinPlot still resolve)" {
    const B = sailor.tui.widgets.BoxPlot;
    const V = sailor.tui.widgets.ViolinPlot;
    try testing.expect(@sizeOf(B) >= 0);
    try testing.expect(@sizeOf(V) >= 0);
}

test "iter411 - regression: SET/GET unaffected by sailor bump" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:1", 1, "127.0.0.1:6379");

    const set_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "SET", "iter411:k", "v" });
    defer allocator.free(set_result);
    try testing.expectEqualStrings("+OK\r\n", set_result);

    const get_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "GET", "iter411:k" });
    defer allocator.free(get_result);
    try testing.expectEqualStrings("$1\r\nv\r\n", get_result);
}

test "iter411 - regression: ZADD/ZSCORE unaffected by sailor bump" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:1", 1, "127.0.0.1:6379");

    const zadd_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "ZADD", "iter411:z", "3.5", "m" });
    defer allocator.free(zadd_result);
    try testing.expectEqualStrings(":1\r\n", zadd_result);

    const zscore_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "ZSCORE", "iter411:z", "m" });
    defer allocator.free(zscore_result);
    try testing.expectEqualStrings("$3\r\n3.5\r\n", zscore_result);
}

test "iter411 - regression: HSET/HGETALL unaffected by sailor bump" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:1", 1, "127.0.0.1:6379");

    const hset_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "HSET", "iter411:h", "f", "v" });
    defer allocator.free(hset_result);
    try testing.expectEqualStrings(":1\r\n", hset_result);

    const hgetall_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "HGETALL", "iter411:h" });
    defer allocator.free(hgetall_result);
    try testing.expectEqualStrings("*2\r\n$1\r\nf\r\n$1\r\nv\r\n", hgetall_result);
}

test "iter411 - regression: SADD/SMEMBERS unaffected by sailor bump" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:1", 1, "127.0.0.1:6379");

    const sadd_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "SADD", "iter411:s", "a" });
    defer allocator.free(sadd_result);
    try testing.expectEqualStrings(":1\r\n", sadd_result);

    const smembers_result = try execCmd(allocator, storage, &client_registry, client_id, &pubsub, &.{ "SMEMBERS", "iter411:s" });
    defer allocator.free(smembers_result);
    try testing.expectEqualStrings("*1\r\n$1\r\na\r\n", smembers_result);
}
