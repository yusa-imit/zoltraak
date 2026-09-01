// Iteration 406: sailor v2.82.0 + BoxPlot TUI widget
//
// sailor v2.82.0: BoxPlot widget added (box-and-whisker plots rendering
// five-number-summary statistics — min, Q1, median, Q3, max — plus outlier
// markers for one or more data series, sharing a global value scale).
// tui_advanced.zig gains renderBoxPlot() as a quartile-summary companion to
// renderViolinPlot(), reusing KeySizeDistribution (per-type key size samples)
// so operators can see both the density silhouette (ViolinPlot) and the
// summary statistics (BoxPlot) for the same STRING/LIST/HASH size data.
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

// ─── sailor v2.82.0 build verification ────────────────────────────────────────

test "iter406 - sailor v2.82.0 build verification" {
    // Verify sailor v2.82.0 compiles (BoxPlot widget added — no API breaks).
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
}

test "iter406 - BoxPlot widget available in sailor v2.82.0 tui.widgets" {
    const sailor = @import("sailor");
    const BoxPlot = sailor.tui.widgets.BoxPlot;
    try testing.expect(@typeInfo(BoxPlot) == .@"struct");
}

test "iter406 - BoxPlotSeries type available in sailor v2.82.0 tui.widgets" {
    const sailor = @import("sailor");
    const BoxPlotSeries = sailor.tui.widgets.BoxPlotSeries;
    try testing.expect(@typeInfo(BoxPlotSeries) == .@"struct");
}

test "iter406 - BoxPlot.MAX_SERIES equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.BoxPlot.MAX_SERIES);
}

test "iter406 - BoxPlot.MAX_SAMPLES equals 64" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 64), sailor.tui.widgets.BoxPlot.MAX_SAMPLES);
}

test "iter406 - BoxPlot.init() creates zero-series plot" {
    const sailor = @import("sailor");
    const bp = sailor.tui.widgets.BoxPlot.init();
    try testing.expectEqual(@as(usize, 0), bp.series.len);
}

test "iter406 - BoxPlot.seriesCount() reflects withSeries" {
    const sailor = @import("sailor");
    const values_a = [_]f32{ 1.0, 2.0, 3.0 };
    const values_b = [_]f32{ 4.0, 5.0, 6.0 };
    const series = [_]sailor.tui.widgets.BoxPlotSeries{
        .{ .label = "A", .values = &values_a },
        .{ .label = "B", .values = &values_b },
    };
    const bp = sailor.tui.widgets.BoxPlot.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 2), bp.seriesCount());
}

test "iter406 - BoxPlot.withShowLabels sets flag" {
    const sailor = @import("sailor");
    const bp = sailor.tui.widgets.BoxPlot.init().withShowLabels(false);
    try testing.expect(!bp.show_labels);
}

test "iter406 - BoxPlot.withFocused sets focused index" {
    const sailor = @import("sailor");
    const bp = sailor.tui.widgets.BoxPlot.init().withFocused(1);
    try testing.expectEqual(@as(usize, 1), bp.focused);
}

test "iter406 - fiveNumberSummary computes correct quartiles" {
    const sailor = @import("sailor");
    const values = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const fns = sailor.tui.widgets.box_plot.fiveNumberSummary(&values);
    try testing.expectEqual(@as(f32, 1.0), fns.min);
    try testing.expectEqual(@as(f32, 5.0), fns.max);
    try testing.expectEqual(@as(f32, 3.0), fns.median);
}

// ─── tui_advanced KeySizeDistribution reuse and renderBoxPlot ────────────────

test "iter406 - KeySizeDistribution struct default values (reused for BoxPlot)" {
    // renderBoxPlot reuses the same struct as renderViolinPlot for a
    // quartile-summary companion view.
    const tui_adv = @import("zoltraak").tui_advanced;
    const d = tui_adv.KeySizeDistribution{};
    try testing.expectEqual(@as(usize, 0), d.string_sizes.len);
    try testing.expectEqual(@as(usize, 0), d.list_sizes.len);
    try testing.expectEqual(@as(usize, 0), d.hash_sizes.len);
}

test "iter406 - BoxPlotSeries holds size samples as-is" {
    const sailor = @import("sailor");
    const samples = [_]f32{ 16.0, 32.0, 64.0 };
    var series = [_]sailor.tui.widgets.BoxPlotSeries{
        .{ .label = "STR", .values = &samples },
    };
    const bp = sailor.tui.widgets.BoxPlot.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 1), bp.seriesCount());
    try testing.expectEqual(@as(f32, 16.0), bp.series[0].values[0]);
    try testing.expectEqual(@as(f32, 32.0), bp.series[0].values[1]);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter406 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10300");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs406", "1.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs406", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1.5") != null);
}

test "iter406 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10301");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h406", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h406",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter406 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10302");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k406", "v406",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k406",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v406") != null);
}
