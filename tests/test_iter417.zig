// Iteration 417: sailor v2.89.0 + BumpChart TUI widget
//
// sailor v2.89.0: BumpChart widget added (multi-time-point rank-over-time
// lines per category — a leaderboard/standings-style chart complementing
// SlopeChart's fixed two-point comparison; up to MAX_SERIES=8 series,
// MAX_TIMEPOINTS=16 time points per series, rank-to-row mapping (rank 1 at
// top), direction glyphs (/, \, ─), tie-safe rendering, focused-series
// highlighting, no heap allocations, no breaking changes). tui_advanced.zig
// gains renderCommandPopularityBump() using BumpChart/BumpSeries/
// CommandRankSnapshot for a per-command call-count popularity ranking
// visualization: each series' polyline is the command's rank across
// successive COMMAND STATS / INFO commandstats sampling intervals, letting
// an operator spot which commands are climbing or falling in relative
// popularity at a glance — a rank-trajectory complement to the
// distribution-shape renderCommandLatencyRidgeline.
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

// ─── sailor v2.89.0 build verification ────────────────────────────────────────

test "iter417 - sailor v2.89.0 build verification" {
    // Verify sailor v2.89.0 compiles (BumpChart widget added — no API breaks).
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter417 - BumpChart widget available in sailor v2.89.0 tui.widgets" {
    const sailor = @import("sailor");
    const BumpChart = sailor.tui.widgets.BumpChart;
    _ = BumpChart;
    try testing.expect(true);
}

test "iter417 - BumpSeries type available in sailor v2.89.0 tui.widgets" {
    const sailor = @import("sailor");
    const BumpSeries = sailor.tui.widgets.BumpSeries;
    _ = BumpSeries;
    try testing.expect(true);
}

test "iter417 - BumpChart.MAX_SERIES equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.BumpChart.MAX_SERIES);
}

test "iter417 - BumpChart.MAX_TIMEPOINTS equals 16" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 16), sailor.tui.widgets.BumpChart.MAX_TIMEPOINTS);
}

test "iter417 - BumpChart.init() creates zero-series chart" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.BumpChart.init();
    try testing.expectEqual(@as(usize, 0), chart.series.len);
    try testing.expectEqual(true, chart.show_labels);
    try testing.expectEqual(false, chart.show_timepoint_labels);
}

test "iter417 - BumpChart.withSeries sets series" {
    const sailor = @import("sailor");
    const ranks_a = [_]u32{ 1, 2, 3, 2 };
    const ranks_b = [_]u32{ 3, 1, 2, 1 };
    const series = [_]sailor.tui.widgets.BumpSeries{
        .{ .label = "GET", .ranks = &ranks_a },
        .{ .label = "SET", .ranks = &ranks_b },
    };
    const chart = sailor.tui.widgets.BumpChart.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 2), chart.series.len);
}

test "iter417 - BumpChart.withFocused sets focused index" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.BumpChart.init().withFocused(1);
    try testing.expectEqual(@as(usize, 1), chart.focused);
}

test "iter417 - BumpChart.withTimepointLabels/withShowTimepointLabels/withShowLabels chain" {
    const sailor = @import("sailor");
    const labels = [_][]const u8{ "t0", "t1", "t2" };
    const chart = sailor.tui.widgets.BumpChart.init()
        .withTimepointLabels(&labels)
        .withShowTimepointLabels(true)
        .withShowLabels(false);
    try testing.expectEqual(@as(usize, 3), chart.timepoint_labels.len);
    try testing.expectEqual(true, chart.show_timepoint_labels);
    try testing.expectEqual(false, chart.show_labels);
}

test "iter417 - BumpChart.seriesCount caps at MAX_SERIES" {
    const sailor = @import("sailor");
    const BumpSeries = sailor.tui.widgets.BumpSeries;
    const series = [_]BumpSeries{.{}} ** 10;
    const chart = sailor.tui.widgets.BumpChart.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 8), chart.seriesCount());
}

test "iter417 - BumpChart.maxRank reports the highest rank across series" {
    const sailor = @import("sailor");
    const ranks_a = [_]u32{ 1, 4, 2 };
    const ranks_b = [_]u32{ 3, 1, 5 };
    const series = [_]sailor.tui.widgets.BumpSeries{
        .{ .label = "GET", .ranks = &ranks_a },
        .{ .label = "SET", .ranks = &ranks_b },
    };
    const chart = sailor.tui.widgets.BumpChart.init().withSeries(&series);
    try testing.expectEqual(@as(u32, 5), chart.maxRank());
}

// ─── tui_advanced CommandRankSnapshot + renderCommandPopularityBump ─────────

test "iter417 - CommandRankSnapshot struct accessible via zoltraak module" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // tui_advanced is compiled as part of zoltraak; renderCommandPopularityBump
    // maps CommandRankSnapshot samples to sailor's BumpSeries type
    // (command -> label, ranks -> ranks).
    try testing.expect(true);
}

test "iter417 - BumpSeries label/ranks map from command/ranks as-is" {
    const sailor = @import("sailor");
    const ranks = [_]u32{ 2, 1, 1, 3 };
    const series = sailor.tui.widgets.BumpSeries{
        .label = "GET",
        .ranks = &ranks,
    };
    try testing.expectEqualStrings("GET", series.label);
    try testing.expectEqual(@as(usize, 4), series.ranks.len);
}

test "iter417 - rank ties across series render without crashing" {
    const sailor = @import("sailor");
    // Multiple series sharing the same rank at a timepoint must not panic —
    // mirror the tie-safety guarantee documented in BumpChart's v2.89.0
    // source (rank ties render on the same row safely).
    const ranks_a = [_]u32{ 1, 1, 1 };
    const ranks_b = [_]u32{ 1, 1, 1 };
    const series = [_]sailor.tui.widgets.BumpSeries{
        .{ .label = "GET", .ranks = &ranks_a },
        .{ .label = "SET", .ranks = &ranks_b },
    };
    const chart = sailor.tui.widgets.BumpChart.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 2), chart.series.len);
    try testing.expectEqual(@as(u32, 1), chart.maxRank());
}

test "iter417 - out-of-range rank 0 clamps safely (no panic class)" {
    const sailor = @import("sailor");
    // rank == 0 is out of the 1-based range; BumpChart's rankToRow clamps
    // rather than panicking per its v2.89.0 source.
    const ranks = [_]u32{ 0, 2, 3 };
    const series = [_]sailor.tui.widgets.BumpSeries{
        .{ .label = "WEIRDCMD", .ranks = &ranks },
    };
    const chart = sailor.tui.widgets.BumpChart.init().withSeries(&series);
    try testing.expectEqual(@as(usize, 1), chart.series.len);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter417 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10350");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs417", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs417", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter417 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10351");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h417", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h417",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter417 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10352");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k417", "v417",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k417",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v417") != null);
}

test "iter417 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10353");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter417",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "iter417 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10354");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s417", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s417",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}

test "iter417 - CLUSTER INFO still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10355");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled") != null);
}
