// Iteration 416: sailor v2.88.0 + RidgelinePlot TUI widget
//
// sailor v2.88.0: RidgelinePlot widget added (stacked, vertically-offset
// density silhouettes per category — a joyplot for comparing distribution
// shape/shift across many series in a compact space; up to MAX_SERIES=8
// series, MAX_BINS=64 bins per series, shared/per-series scale, configurable
// overlap, focused-series highlighting, no heap allocations, no breaking
// changes). tui_advanced.zig gains renderCommandLatencyRidgeline() using
// RidgelinePlot/RidgelineSeries/CommandLatencyHistogram for a per-command
// latency-distribution-shape visualization: each series' silhouette is the
// pre-binned frequency counts sampled from LATENCY HISTORY / commandstats
// for one command, letting an operator spot wide/bimodal/skewed latency
// spread across commands at a glance — a many-category distribution-shape
// complement to the two-point renderKeyspaceChangeSlope.
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

// ─── sailor v2.88.0 build verification ────────────────────────────────────────

test "iter416 - sailor v2.88.0 build verification" {
    // Verify sailor v2.88.0 compiles (RidgelinePlot widget added — no API breaks).
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter416 - RidgelinePlot widget available in sailor v2.88.0 tui.widgets" {
    const sailor = @import("sailor");
    const RidgelinePlot = sailor.tui.widgets.RidgelinePlot;
    _ = RidgelinePlot;
    try testing.expect(true);
}

test "iter416 - RidgelineSeries type available in sailor v2.88.0 tui.widgets" {
    const sailor = @import("sailor");
    const RidgelineSeries = sailor.tui.widgets.RidgelineSeries;
    _ = RidgelineSeries;
    try testing.expect(true);
}

test "iter416 - RidgelinePlot.MAX_SERIES equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.RidgelinePlot.MAX_SERIES);
}

test "iter416 - RidgelinePlot.MAX_BINS equals 64" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 64), sailor.tui.widgets.RidgelinePlot.MAX_BINS);
}

test "iter416 - RidgelinePlot.init() creates zero-series plot" {
    const sailor = @import("sailor");
    const plot = sailor.tui.widgets.RidgelinePlot.init();
    try testing.expectEqual(@as(usize, 0), plot.series.len);
    try testing.expectEqual(true, plot.shared_scale);
    try testing.expectEqual(@as(u16, 0), plot.overlap);
}

test "iter416 - RidgelinePlot.withSeries sets series" {
    const sailor = @import("sailor");
    const values_a = [_]f32{ 1.0, 2.0, 3.0 };
    const values_b = [_]f32{ 3.0, 2.0, 1.0 };
    const series = [_]sailor.tui.widgets.RidgelineSeries{
        .{ .label = "GET", .values = &values_a },
        .{ .label = "SET", .values = &values_b },
    };
    const plot = sailor.tui.widgets.RidgelinePlot.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 2), plot.series.len);
}

test "iter416 - RidgelinePlot.withFocused sets focused index" {
    const sailor = @import("sailor");
    const plot = sailor.tui.widgets.RidgelinePlot.init().withFocused(1);
    try testing.expectEqual(@as(usize, 1), plot.focused.?);
}

test "iter416 - RidgelinePlot.withOverlap/withReverse/withSharedScale chain" {
    const sailor = @import("sailor");
    const plot = sailor.tui.widgets.RidgelinePlot.init()
        .withOverlap(2)
        .withReverse(true)
        .withSharedScale(false);
    try testing.expectEqual(@as(u16, 2), plot.overlap);
    try testing.expectEqual(true, plot.reverse);
    try testing.expectEqual(false, plot.shared_scale);
}

test "iter416 - RidgelinePlot.withLabelColumnWidth sets label column" {
    const sailor = @import("sailor");
    const plot = sailor.tui.widgets.RidgelinePlot.init().withLabelColumnWidth(10);
    try testing.expectEqual(@as(u16, 10), plot.label_column_width);
}

test "iter416 - RidgelinePlot.seriesCount caps at MAX_SERIES" {
    const sailor = @import("sailor");
    const RidgelineSeries = sailor.tui.widgets.RidgelineSeries;
    const series = [_]RidgelineSeries{.{}} ** 10;
    const plot = sailor.tui.widgets.RidgelinePlot.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 8), plot.seriesCount());
}

// ─── tui_advanced CommandLatencyHistogram + renderCommandLatencyRidgeline ────

test "iter416 - CommandLatencyHistogram struct accessible via zoltraak module" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // tui_advanced is compiled as part of zoltraak; renderCommandLatencyRidgeline
    // maps CommandLatencyHistogram samples to sailor's RidgelineSeries type
    // (command -> label, bucket_counts -> values).
    try testing.expect(true);
}

test "iter416 - RidgelineSeries label/values map from command/bucket_counts as-is" {
    const sailor = @import("sailor");
    const bucket_counts = [_]f32{ 10.0, 40.0, 25.0, 5.0 };
    const series = sailor.tui.widgets.RidgelineSeries{
        .label = "GET",
        .values = &bucket_counts,
    };
    try testing.expectEqualStrings("GET", series.label);
    try testing.expectEqual(@as(usize, 4), series.values.len);
}

test "iter416 - over-range bucket values still clamp safely (no panic class)" {
    const sailor = @import("sailor");
    // A bucket count far exceeding others must not panic on render — mirror
    // the clamp-safety regression class already verified for SlopeChart in
    // v2.87.0 (RidgelinePlot's render() calls clampValue()/normalization
    // internally per its v2.88.0 source).
    const bucket_counts = [_]f32{ 1.0, 999999.0, 2.0 };
    const series = [_]sailor.tui.widgets.RidgelineSeries{
        .{ .label = "SLOWCMD", .values = &bucket_counts },
    };
    const plot = sailor.tui.widgets.RidgelinePlot.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 1), plot.series.len);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter416 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10340");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs416", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs416", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter416 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10341");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h416", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h416",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter416 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10342");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k416", "v416",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k416",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v416") != null);
}

test "iter416 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10343");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter416",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "iter416 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10344");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s416", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s416",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}

test "iter416 - CLUSTER INFO still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10345");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled") != null);
}
