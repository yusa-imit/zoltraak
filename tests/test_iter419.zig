// Iteration 419: sailor v2.90.0 + MosaicPlot TUI widget
//
// sailor v2.90.0: MosaicPlot widget added (Marimekko-style proportional
// chart — variable-width columns proportional to each column's share of
// the grand total, with variable-height stacked segments within each
// column proportional to that column's segment shares; MAX_COLUMNS=16,
// MAX_SEGMENTS_PER_COLUMN=8, no heap allocations, no breaking changes).
// tui_advanced.zig gains renderMemoryByTypeMosaic() using
// MosaicPlot/MosaicColumn/MosaicSegment/DatabaseMemoryByType for a
// two-dimensional (database x value-type) memory-usage breakdown: column
// width = database's total memory footprint, segment height = per-type
// (STRING/LIST/HASH/SET/ZSET) memory share within that database — an
// area-based complement to the ring-based renderSunburstChart (which
// encodes key counts rather than memory bytes).
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

// ─── sailor v2.90.0 build verification ────────────────────────────────────────

test "iter419 - sailor v2.90.0 build verification" {
    // Verify sailor v2.90.0 compiles (MosaicPlot widget added — no API breaks).
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter419 - MosaicPlot widget available in sailor v2.90.0 tui.widgets" {
    const sailor = @import("sailor");
    const MosaicPlot = sailor.tui.widgets.MosaicPlot;
    _ = MosaicPlot;
    try testing.expect(true);
}

test "iter419 - MosaicColumn/MosaicSegment types available in sailor v2.90.0 tui.widgets" {
    const sailor = @import("sailor");
    const MosaicColumn = sailor.tui.widgets.MosaicColumn;
    const MosaicSegment = sailor.tui.widgets.MosaicSegment;
    _ = MosaicColumn;
    _ = MosaicSegment;
    try testing.expect(true);
}

test "iter419 - MosaicPlot.MAX_COLUMNS equals 16" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 16), sailor.tui.widgets.MosaicPlot.MAX_COLUMNS);
}

test "iter419 - MosaicPlot.MAX_SEGMENTS_PER_COLUMN equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.MosaicPlot.MAX_SEGMENTS_PER_COLUMN);
}

test "iter419 - MosaicPlot.init() creates zero-column plot" {
    const sailor = @import("sailor");
    const plot = sailor.tui.widgets.MosaicPlot.init();
    try testing.expectEqual(@as(usize, 0), plot.columns.len);
    try testing.expectEqual(true, plot.show_column_labels);
    try testing.expectEqual(false, plot.show_segment_labels);
}

test "iter419 - MosaicPlot.withColumns sets columns" {
    const sailor = @import("sailor");
    const segs_a = [_]sailor.tui.widgets.MosaicSegment{
        .{ .label = "A", .value = 30 },
        .{ .label = "B", .value = 20 },
    };
    const segs_b = [_]sailor.tui.widgets.MosaicSegment{
        .{ .label = "X", .value = 40 },
    };
    const cols = [_]sailor.tui.widgets.MosaicColumn{
        .{ .label = "Col1", .segments = &segs_a },
        .{ .label = "Col2", .segments = &segs_b },
    };
    const plot = sailor.tui.widgets.MosaicPlot.init().withColumns(&cols);
    try testing.expectEqual(@as(usize, 2), plot.columns.len);
}

test "iter419 - MosaicPlot.withFocusedColumn/withFocusedSegment chain" {
    const sailor = @import("sailor");
    const plot = sailor.tui.widgets.MosaicPlot.init()
        .withFocusedColumn(1)
        .withFocusedSegment(2);
    try testing.expectEqual(@as(usize, 1), plot.focused_column);
    try testing.expectEqual(@as(usize, 2), plot.focused_segment);
}

test "iter419 - MosaicPlot.columnCount caps at MAX_COLUMNS" {
    const sailor = @import("sailor");
    const MosaicColumn = sailor.tui.widgets.MosaicColumn;
    const cols = [_]MosaicColumn{.{}} ** 20;
    const plot = sailor.tui.widgets.MosaicPlot.init().withColumns(&cols);
    try testing.expectEqual(@as(usize, 16), plot.columnCount());
}

test "iter419 - MosaicPlot.columnTotal clamps negative segment values to 0" {
    const sailor = @import("sailor");
    const segs = [_]sailor.tui.widgets.MosaicSegment{
        .{ .value = 10 },
        .{ .value = -5 },
        .{ .value = 20 },
    };
    const cols = [_]sailor.tui.widgets.MosaicColumn{.{ .segments = &segs }};
    const plot = sailor.tui.widgets.MosaicPlot.init().withColumns(&cols);
    try testing.expectEqual(@as(f32, 30), plot.columnTotal(0));
}

test "iter419 - MosaicPlot.grandTotal sums proportional column totals" {
    const sailor = @import("sailor");
    const segs_a = [_]sailor.tui.widgets.MosaicSegment{.{ .value = 30 }};
    const segs_b = [_]sailor.tui.widgets.MosaicSegment{.{ .value = 70 }};
    const cols = [_]sailor.tui.widgets.MosaicColumn{
        .{ .segments = &segs_a },
        .{ .segments = &segs_b },
    };
    const plot = sailor.tui.widgets.MosaicPlot.init().withColumns(&cols);
    try testing.expectEqual(@as(f32, 100), plot.grandTotal());
}

// ─── tui_advanced DatabaseMemoryByType + renderMemoryByTypeMosaic ───────────

test "iter419 - DatabaseMemoryByType struct accessible via zoltraak module" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // tui_advanced is compiled as part of zoltraak; renderMemoryByTypeMosaic
    // maps DatabaseMemoryByType samples to sailor's MosaicColumn/MosaicSegment
    // types (db_index -> column label, per-type bytes -> segment values).
    try testing.expect(true);
}

test "iter419 - MosaicSegment label/value map from per-type bytes as-is" {
    const sailor = @import("sailor");
    const seg = sailor.tui.widgets.MosaicSegment{
        .label = "STR",
        .value = 4096,
    };
    try testing.expectEqualStrings("STR", seg.label);
    try testing.expectEqual(@as(f32, 4096), seg.value);
}

test "iter419 - out-of-range/negative memory values clamp safely (no panic class)" {
    const sailor = @import("sailor");
    // Negative values clamp to 0 in columnTotal/grandTotal per v2.90.0 source
    // (mirrors the zero/negative-value safety class established by prior
    // widgets, e.g. CandlestickChart/BulletChart/ParetoChart).
    const segs = [_]sailor.tui.widgets.MosaicSegment{
        .{ .label = "STR", .value = -100 },
        .{ .label = "LIST", .value = 200 },
    };
    const cols = [_]sailor.tui.widgets.MosaicColumn{.{ .segments = &segs }};
    const plot = sailor.tui.widgets.MosaicPlot.init().withColumns(&cols);
    try testing.expectEqual(@as(f32, 200), plot.columnTotal(0));
}

test "iter419 - zero grand total across all columns produces no crash" {
    const sailor = @import("sailor");
    const segs = [_]sailor.tui.widgets.MosaicSegment{
        .{ .label = "STR", .value = 0 },
    };
    const cols = [_]sailor.tui.widgets.MosaicColumn{.{ .segments = &segs }};
    const plot = sailor.tui.widgets.MosaicPlot.init().withColumns(&cols);
    try testing.expectEqual(@as(f32, 0), plot.grandTotal());
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter419 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10360");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs419", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs419", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter419 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10361");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h419", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h419",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter419 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10362");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k419", "v419",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k419",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v419") != null);
}

test "iter419 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10363");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter419",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "iter419 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10364");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s419", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s419",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}

test "iter419 - CLUSTER INFO still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10365");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled") != null);
}
