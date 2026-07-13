// Iteration 413: sailor v2.85.0 + v2.86.0 — ParallelCoordinates + ParetoChart TUI widgets
//
// sailor v2.85.0: ParallelCoordinates widget added (multi-dimensional data as
// parallel vertical axes connected by per-item polylines, MAX_AXES=8,
// MAX_ITEMS=16, no heap allocations, no breaking changes). Also fixes an
// axisX() off-by-one (divided by inner.width instead of inner.width - 1) that
// placed the last axis one column past the visible area for multi-axis
// layouts.
//
// sailor v2.86.0: ParetoChart widget added (descending-sorted bars overlaid
// with a cumulative-percentage line, MAX_ITEMS=32, configurable threshold
// marker default 80%, no heap allocations, no breaking changes).
//
// Both releases are additive widget-only changes with zero API breaks for
// zoltraak's existing sailor.tui.widgets.* usage. Verify no regression in
// existing commands.

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

// ─── sailor v2.85.0 + v2.86.0 build verification ─────────────────────────────

test "iter413 - sailor v2.86.0 build verification" {
    // Verify sailor v2.86.0 compiles (ParallelCoordinates + ParetoChart widgets
    // added across v2.85.0/v2.86.0 — no API breaks).
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter413 - ParallelCoordinates widget available in sailor tui.widgets" {
    const sailor = @import("sailor");
    const ParallelCoordinates = sailor.tui.widgets.ParallelCoordinates;
    _ = ParallelCoordinates;
    try testing.expect(true);
}

test "iter413 - PCAxis type available in sailor tui.widgets" {
    const sailor = @import("sailor");
    const PCAxis = sailor.tui.widgets.PCAxis;
    _ = PCAxis;
    try testing.expect(true);
}

test "iter413 - PCItem type available in sailor tui.widgets" {
    const sailor = @import("sailor");
    const PCItem = sailor.tui.widgets.PCItem;
    _ = PCItem;
    try testing.expect(true);
}

test "iter413 - ParallelCoordinates.MAX_AXES equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.ParallelCoordinates.MAX_AXES);
}

test "iter413 - ParallelCoordinates.MAX_ITEMS equals 16" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 16), sailor.tui.widgets.ParallelCoordinates.MAX_ITEMS);
}

test "iter413 - ParetoChart widget available in sailor tui.widgets" {
    const sailor = @import("sailor");
    const ParetoChart = sailor.tui.widgets.ParetoChart;
    _ = ParetoChart;
    try testing.expect(true);
}

test "iter413 - ParetoItem type available in sailor tui.widgets" {
    const sailor = @import("sailor");
    const ParetoItem = sailor.tui.widgets.ParetoItem;
    _ = ParetoItem;
    try testing.expect(true);
}

test "iter413 - ParetoChart.MAX_ITEMS equals 32" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 32), sailor.tui.widgets.ParetoChart.MAX_ITEMS);
}

test "iter413 - ParetoChart out-of-range and negative values do not panic" {
    const sailor = @import("sailor");
    // Release notes explicitly call out an out-of-range/negative-value
    // no-panic regression test class (same safety class as prior widgets).
    const items = [_]sailor.tui.widgets.ParetoItem{
        .{ .label = "A", .value = -5.0 },
        .{ .label = "B", .value = 999999.0 },
    };
    const chart = sailor.tui.widgets.ParetoChart.init().withItems(&items);
    try testing.expectEqual(@as(usize, 2), chart.itemCount());
}

test "iter413 - ParetoChart default threshold is 0.8 (80 percent)" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.ParetoChart.init();
    try testing.expect(@abs(chart.threshold - 0.8) < 0.001);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter413 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10330");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k413", "v413",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k413",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v413") != null);
}

test "iter413 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10331");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs413", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs413", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter413 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10332");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h413", "field1", "val1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h413",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter413 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10333");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s413", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s413",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}

test "iter413 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10334");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter413",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "iter413 - CLUSTER INFO still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10335");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled") != null);
}
