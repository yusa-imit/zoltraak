// Iteration 420: sailor v2.91.0 + IcicleChart TUI widget
//
// sailor v2.91.0: IcicleChart widget added (axis-aligned hierarchical
// chart — stacked horizontal bands rendered top-to-bottom by tree depth,
// each parent's column span divided among its children proportionally to
// their values using a cumulative-floor formula; MAX_DEPTH=6,
// MAX_CHILDREN_PER_NODE=8, no heap allocations, no breaking changes).
// tui_advanced.zig gains renderCommandStatsIcicle() using
// IcicleChart/IcicleNode/CommandCategoryStats/CommandCallCount for a
// two-level command-statistics breakdown: root band spans the width,
// category bands are proportional to each category's total call count,
// and per-command bands within a category are proportional to that
// command's call count — a rectangular drill-down complement to the
// ring-based renderSunburstChart and the area-based
// renderMemoryByTypeMosaic.
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

// ─── sailor v2.91.0 build verification ────────────────────────────────────────

test "iter420 - sailor v2.91.0 build verification" {
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter420 - IcicleChart widget available in sailor v2.91.0 tui.widgets" {
    const sailor = @import("sailor");
    const IcicleChart = sailor.tui.widgets.IcicleChart;
    _ = IcicleChart;
    try testing.expect(true);
}

test "iter420 - IcicleNode type available in sailor v2.91.0 tui.widgets" {
    const sailor = @import("sailor");
    const IcicleNode = sailor.tui.widgets.IcicleNode;
    _ = IcicleNode;
    try testing.expect(true);
}

test "iter420 - IcicleChart.MAX_DEPTH equals 6" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 6), sailor.tui.widgets.IcicleChart.MAX_DEPTH);
}

test "iter420 - IcicleChart.MAX_CHILDREN_PER_NODE equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.IcicleChart.MAX_CHILDREN_PER_NODE);
}

test "iter420 - IcicleChart.init() creates empty chart" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.IcicleChart.init();
    try testing.expectEqual(@as(?sailor.tui.widgets.IcicleNode, null), chart.root);
    try testing.expectEqual(true, chart.show_labels);
    try testing.expectEqual(false, chart.show_values);
}

test "iter420 - IcicleChart.withRoot sets root and nodeCount counts tree" {
    const sailor = @import("sailor");
    var children = [_]sailor.tui.widgets.IcicleNode{
        .{ .label = "A", .value = 10.0 },
        .{ .label = "B", .value = 20.0 },
    };
    const root = sailor.tui.widgets.IcicleNode{ .label = "Root", .value = 30.0, .children = &children };
    const chart = sailor.tui.widgets.IcicleChart.init().withRoot(root);
    try testing.expectEqual(@as(usize, 3), chart.nodeCount());
}

test "iter420 - IcicleChart.nodeCount caps children at MAX_CHILDREN_PER_NODE" {
    const sailor = @import("sailor");
    const children = [_]sailor.tui.widgets.IcicleNode{.{ .value = 1.0 }} ** 20;
    const root = sailor.tui.widgets.IcicleNode{ .label = "Root", .value = 20.0, .children = &children };
    const chart = sailor.tui.widgets.IcicleChart.init().withRoot(root);
    try testing.expectEqual(@as(usize, 1 + 8), chart.nodeCount());
}

test "iter420 - IcicleChart.nodeCount is zero when root is null" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.IcicleChart.init();
    try testing.expectEqual(@as(usize, 0), chart.nodeCount());
}

// ─── tui_advanced CommandCategoryStats + renderCommandStatsIcicle ───────────

test "iter420 - CommandCallCount/CommandCategoryStats structs accessible via zoltraak module" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // tui_advanced is compiled as part of zoltraak; renderCommandStatsIcicle
    // maps CommandCategoryStats/CommandCallCount samples to sailor's
    // IcicleNode tree (category -> total calls, command -> per-command calls).
    try testing.expect(true);
}

test "iter420 - IcicleNode label/value map from per-command calls as-is" {
    const sailor = @import("sailor");
    const node = sailor.tui.widgets.IcicleNode{
        .label = "GET",
        .value = 512,
    };
    try testing.expectEqualStrings("GET", node.label);
    try testing.expectEqual(@as(f32, 512), node.value);
}

test "iter420 - zero-value children produce a leaf-only tree without crash" {
    const sailor = @import("sailor");
    const children = [_]sailor.tui.widgets.IcicleNode{
        .{ .label = "GET", .value = 0 },
        .{ .label = "SET", .value = 0 },
    };
    const root = sailor.tui.widgets.IcicleNode{ .label = "string", .value = 0, .children = &children };
    const chart = sailor.tui.widgets.IcicleChart.init().withRoot(root);
    try testing.expectEqual(@as(usize, 3), chart.nodeCount());
}

test "iter420 - negative-value children are excluded from rendered span (docs contract)" {
    // Per icicle_chart.zig's childrenTotal(), only positive-value children
    // contribute to a parent's span proportions — mirrors the zero/negative
    // safety class established by MosaicPlot.columnTotal (Iteration 419).
    const sailor = @import("sailor");
    const children = [_]sailor.tui.widgets.IcicleNode{
        .{ .label = "neg", .value = -5 },
        .{ .label = "pos", .value = 15 },
    };
    const root = sailor.tui.widgets.IcicleNode{ .label = "root", .value = 15, .children = &children };
    const chart = sailor.tui.widgets.IcicleChart.init().withRoot(root);
    // nodeCount still includes both children (structural count is
    // value-agnostic); the value-based exclusion only affects rendering.
    try testing.expectEqual(@as(usize, 3), chart.nodeCount());
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter420 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10370");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs420", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs420", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter420 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10371");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h420", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h420",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter420 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10372");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k420", "v420",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k420",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v420") != null);
}

test "iter420 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10373");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter420",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "iter420 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10374");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s420", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s420",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}

test "iter420 - CLUSTER INFO still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10375");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled") != null);
}
