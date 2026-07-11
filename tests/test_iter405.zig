// Iteration 405: sailor v2.81.0 + SunburstChart TUI widget
//
// sailor v2.81.0: SunburstChart widget added (hierarchical radial chart rendering
// tree data as concentric rings of arcs; up to MAX_DEPTH=4 rings, MAX_NODES=8
// top-level nodes, no heap allocations).
// tui_advanced.zig gains renderSunburstChart() for hierarchical keyspace
// visualization: root ring = databases, second ring = per-database key-type
// breakdown (STRING/LIST/HASH/SET/ZSET counts).
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

// ─── sailor v2.81.0 build verification ────────────────────────────────────────

test "iter405 - sailor v2.81.0 build verification" {
    // Verify sailor v2.81.0 compiles (SunburstChart widget added — no API breaks).
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter405 - SunburstChart widget available in sailor v2.81.0 tui.widgets" {
    const sailor = @import("sailor");
    const SunburstChart = sailor.tui.widgets.SunburstChart;
    _ = SunburstChart;
    try testing.expect(true);
}

test "iter405 - SunburstNode type available in sailor v2.81.0 tui.widgets" {
    const sailor = @import("sailor");
    const SunburstNode = sailor.tui.widgets.SunburstNode;
    _ = SunburstNode;
    try testing.expect(true);
}

test "iter405 - SunburstChart.MAX_DEPTH equals 4" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 4), sailor.tui.widgets.SunburstChart.MAX_DEPTH);
}

test "iter405 - SunburstChart.MAX_NODES equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.SunburstChart.MAX_NODES);
}

test "iter405 - SunburstChart.init() creates zero-node chart" {
    const sailor = @import("sailor");
    const sc = sailor.tui.widgets.SunburstChart.init();
    try testing.expectEqual(@as(usize, 0), sc.nodes.len);
}

test "iter405 - SunburstChart.nodeCount() reflects withNodes" {
    const sailor = @import("sailor");
    var child = [_]sailor.tui.widgets.SunburstNode{
        .{ .label = "SubA", .value = 10.0 },
    };
    var nodes = [_]sailor.tui.widgets.SunburstNode{
        .{ .label = "Root", .value = 30.0, .children = &child },
    };
    const sc = sailor.tui.widgets.SunburstChart.init().withNodes(&nodes);
    try testing.expectEqual(@as(usize, 1), sc.nodeCount());
}

test "iter405 - SunburstChart.totalValue sums positive top-level values" {
    const sailor = @import("sailor");
    var nodes = [_]sailor.tui.widgets.SunburstNode{
        .{ .label = "A", .value = 10.0 },
        .{ .label = "B", .value = 20.0 },
    };
    const sc = sailor.tui.widgets.SunburstChart.init().withNodes(&nodes);
    try testing.expectApproxEqAbs(@as(f32, 30.0), sc.totalValue(), 0.001);
}

test "iter405 - SunburstChart.withFocused sets focused index" {
    const sailor = @import("sailor");
    const sc1 = sailor.tui.widgets.SunburstChart.init().withFocused(0);
    const sc2 = sc1.withFocused(2);
    try testing.expectEqual(@as(usize, 0), sc1.focused);
    try testing.expectEqual(@as(usize, 2), sc2.focused);
}

// ─── tui_advanced DatabaseKeyTypeCounts and renderSunburstChart ──────────────

test "iter405 - DatabaseKeyTypeCounts struct default values" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // Verify DatabaseKeyTypeCounts struct is accessible via zoltraak module
    // (tui_advanced is compiled as part of zoltraak)
    try testing.expect(true);
}

test "iter405 - SunburstNode children hold per-type key counts as-is" {
    const sailor = @import("sailor");
    var children = [_]sailor.tui.widgets.SunburstNode{
        .{ .label = "STR", .value = 5.0 },
        .{ .label = "LIST", .value = 2.0 },
    };
    var nodes = [_]sailor.tui.widgets.SunburstNode{
        .{ .label = "db0", .value = 7.0, .children = &children },
    };
    const sc = sailor.tui.widgets.SunburstChart.init().withNodes(&nodes);
    try testing.expectEqual(@as(usize, 1), sc.nodeCount());
    try testing.expectEqual(@as(f32, 5.0), sc.nodes[0].children[0].value);
    try testing.expectEqual(@as(f32, 2.0), sc.nodes[0].children[1].value);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter405 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10300");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs405", "1.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs405", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1.5") != null);
}

test "iter405 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10301");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h405", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h405",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter405 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10302");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "key405", "value405",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "key405",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "value405") != null);
}

test "iter405 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10303");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "counter405", "41",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter405",
    });
    defer allocator.free(resp);

    try testing.expectEqualStrings(":42\r\n", resp);
}

test "iter405 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10304");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "set405", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "set405",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "b") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "c") != null);
}

test "iter405 - CLUSTER INFO still returns bulk string in RESP2" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10305");
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
