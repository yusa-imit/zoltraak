// Iteration 402: sailor v2.79.0 + StreamGraph TUI widget
//
// sailor v2.79.0: StreamGraph widget added (theme-river-style stacked area chart,
// layers stack symmetrically around a vertically centered baseline).
// tui_advanced.zig gains renderStreamGraph() for per-command throughput history
// visualization (GET/SET/DEL ops/sec over time).
//
// Verify no regression in existing commands.

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

// ─── sailor v2.79.0 build verification ────────────────────────────────────────

test "iter402 - sailor v2.79.0 build verification" {
    // Verify sailor v2.79.0 compiles (StreamGraph widget added — no API breaks).
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
}

test "iter402 - StreamGraph widget available in sailor v2.79.0 tui.widgets" {
    // StreamGraph lives in sailor.tui.widgets namespace.
    const sailor = @import("sailor");
    const StreamGraph = sailor.tui.widgets.StreamGraph;
    try testing.expect(@typeInfo(StreamGraph) == .@"struct");
}

test "iter402 - StreamLayer type available in sailor v2.79.0 tui.widgets" {
    const sailor = @import("sailor");
    const StreamLayer = sailor.tui.widgets.StreamLayer;
    try testing.expect(@typeInfo(StreamLayer) == .@"struct");
}

test "iter402 - StreamGraph.MAX_LAYERS equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.StreamGraph.MAX_LAYERS);
}

test "iter402 - StreamGraph.init() creates zero-layer graph" {
    const sailor = @import("sailor");
    const sg = sailor.tui.widgets.StreamGraph.init();
    try testing.expectEqual(@as(usize, 0), sg.layers.len);
}

test "iter402 - StreamGraph.layerCount() reflects withLayers" {
    const sailor = @import("sailor");
    const values_a = [_]f32{ 1.0, 2.0, 3.0 };
    const values_b = [_]f32{ 4.0, 5.0, 6.0 };
    const layers = [_]sailor.tui.widgets.StreamLayer{
        .{ .label = "A", .values = &values_a },
        .{ .label = "B", .values = &values_b },
    };
    const sg = sailor.tui.widgets.StreamGraph.init().withLayers(&layers);
    try testing.expectEqual(@as(usize, 2), sg.layerCount());
}

test "iter402 - StreamGraph.withShowLabels sets flag" {
    const sailor = @import("sailor");
    const sg = sailor.tui.widgets.StreamGraph.init().withShowLabels(false);
    try testing.expect(!sg.show_labels);
}

test "iter402 - StreamGraph.withFocused sets focused index" {
    const sailor = @import("sailor");
    const sg = sailor.tui.widgets.StreamGraph.init().withFocused(2);
    try testing.expectEqual(@as(usize, 2), sg.focused);
}

// ─── tui_advanced CommandThroughputHistory and renderStreamGraph ─────────────

test "iter402 - CommandThroughputHistory struct default values" {
    const tui_adv = @import("zoltraak").tui_advanced;
    const h = tui_adv.CommandThroughputHistory{};
    try testing.expectEqual(@as(usize, 0), h.get_samples.len);
    try testing.expectEqual(@as(usize, 0), h.set_samples.len);
    try testing.expectEqual(@as(usize, 0), h.del_samples.len);
}

test "iter402 - StreamLayer holds non-negative sample values as-is" {
    const sailor = @import("sailor");
    const samples = [_]f32{ 10.0, 20.0, 15.0 };
    var layers = [_]sailor.tui.widgets.StreamLayer{
        .{ .label = "GET", .values = &samples },
    };
    const sg = sailor.tui.widgets.StreamGraph.init().withLayers(&layers);
    try testing.expectEqual(@as(usize, 1), sg.layerCount());
    try testing.expectEqual(@as(f32, 10.0), sg.layers[0].values[0]);
    try testing.expectEqual(@as(f32, 20.0), sg.layers[0].values[1]);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter402 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10100");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs402", "1.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs402", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1.5") != null);
}

test "iter402 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10101");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h402", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h402",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter402 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10102");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "key402", "value402",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "key402",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "value402") != null);
}

test "iter402 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10103");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "counter402", "41",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter402",
    });
    defer allocator.free(resp);

    try testing.expectEqualStrings(":42\r\n", resp);
}

test "iter402 - LPUSH and LRANGE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10104");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LPUSH", "list402", "c", "b", "a",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LRANGE", "list402", "0", "-1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "b") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "c") != null);
}

test "iter402 - CLUSTER INFO still returns bulk string in RESP2" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10105");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '$');
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled:0") != null);
}
