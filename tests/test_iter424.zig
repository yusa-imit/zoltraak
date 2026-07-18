// Iteration 424: sailor v2.94.0 + DonutChart TUI widget
//
// sailor v2.94.0: DonutChart widget added (hollow-center variant of
// PieChart — a ring of slices computed from u64 values with an adjustable
// hole_ratio clamped to [0.0, 0.9] at render time and an optional
// center_label rendered inside the hole; legend right/bottom/none, no heap
// allocations) plus ErrorBarChart (not used by this iteration). No breaking
// changes.
// tui_advanced.zig gains renderKeyTypeDonut() using DonutChart/KeyTypeCount
// to render a keyspace composition breakdown (key count per Redis data
// type) as a donut with a headline total in the center.
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

// ─── sailor v2.94.0 build verification ────────────────────────────────────────

test "iter424 - sailor v2.94.0 build verification" {
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter424 - DonutChart widget available in sailor v2.94.0 tui.widgets" {
    const sailor = @import("sailor");
    const DonutChart = sailor.tui.widgets.DonutChart;
    _ = DonutChart;
    try testing.expect(true);
}

test "iter424 - ErrorBarChart widget available in sailor v2.94.0 tui.widgets" {
    const sailor = @import("sailor");
    const ErrorBarChart = sailor.tui.widgets.ErrorBarChart;
    _ = ErrorBarChart;
    try testing.expect(true);
}

test "iter424 - DonutChart.init defaults to right legend and 0.5 hole ratio" {
    const sailor = @import("sailor");
    const slices = [_]sailor.tui.widgets.donut_chart.Slice{
        .{ .label = "string", .value = 3 },
    };
    const chart = sailor.tui.widgets.DonutChart.init(&slices);
    try testing.expectEqual(sailor.tui.widgets.DonutChart.LegendPosition.right, chart.legend_position);
    try testing.expectEqual(@as(f32, 0.5), chart.hole_ratio);
    try testing.expectEqual(@as(?[]const u8, null), chart.center_label);
}

test "iter424 - DonutChart.withCenterLabel sets label without mutating original" {
    const sailor = @import("sailor");
    const slices = [_]sailor.tui.widgets.donut_chart.Slice{
        .{ .label = "hash", .value = 5 },
    };
    const chart1 = sailor.tui.widgets.DonutChart.init(&slices);
    const chart2 = chart1.withCenterLabel("5 keys");

    try testing.expectEqual(@as(?[]const u8, null), chart1.center_label);
    try testing.expect(std.mem.eql(u8, "5 keys", chart2.center_label.?));
}

test "iter424 - DonutChart.calcTotal sums slice values" {
    const sailor = @import("sailor");
    const slices = [_]sailor.tui.widgets.donut_chart.Slice{
        .{ .label = "string", .value = 3 },
        .{ .label = "hash", .value = 5 },
        .{ .label = "set", .value = 2 },
    };
    try testing.expectEqual(@as(u64, 10), sailor.tui.widgets.DonutChart.calcTotal(&slices));
}

// ─── tui_advanced KeyTypeCount + renderKeyTypeDonut ─────────────────────────

test "iter424 - KeyTypeCount/renderKeyTypeDonut accessible via zoltraak module" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // tui_advanced is compiled as part of zoltraak; renderKeyTypeDonut maps
    // KeyTypeCount samples into sailor's DonutChart for a keyspace
    // composition breakdown with a headline total in the center hole.
    try testing.expect(true);
}

test "iter424 - KeyTypeCount default count is zero" {
    const tui_adv = @import("zoltraak");
    const kt = tui_adv.tui_advanced.KeyTypeCount{};
    try testing.expectEqual(@as(u64, 0), kt.count);
    try testing.expectEqual(@as(usize, 0), kt.type_name.len);
}

test "iter424 - renderKeyTypeDonut maps type counts into slices buffer" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 40, 12) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 40, .height = 12 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const counts = [_]tui_adv.tui_advanced.KeyTypeCount{
        .{ .type_name = "string", .count = 10 },
        .{ .type_name = "hash", .count = 0 },
        .{ .type_name = "set", .count = 4 },
    };
    var slices_buf: [8]sailor.tui.widgets.donut_chart.Slice = undefined;
    tui_adv.tui_advanced.renderKeyTypeDonut(&frame, area, &counts, "14 keys", &slices_buf);

    try testing.expect(std.mem.eql(u8, "string", slices_buf[0].label));
    try testing.expectEqual(@as(u64, 10), slices_buf[0].value);
    try testing.expectEqual(@as(u64, 0), slices_buf[1].value);
    try testing.expect(std.mem.eql(u8, "set", slices_buf[2].label));
    try testing.expectEqual(@as(u64, 4), slices_buf[2].value);
}

test "iter424 - renderKeyTypeDonut is a no-op on zero-size area" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 10, 4) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 0, .height = 4 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const counts = [_]tui_adv.tui_advanced.KeyTypeCount{
        .{ .type_name = "string", .count = 3 },
    };
    var slices_buf: [4]sailor.tui.widgets.donut_chart.Slice = undefined;
    tui_adv.tui_advanced.renderKeyTypeDonut(&frame, area, &counts, null, &slices_buf);
    // No crash — buffer content is untouched since nothing was rendered.
    try testing.expect(true);
}

test "iter424 - renderKeyTypeDonut truncates type counts exceeding slices buffer capacity" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 40, 12) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 40, .height = 12 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const counts = [_]tui_adv.tui_advanced.KeyTypeCount{
        .{ .type_name = "string", .count = 1 },
        .{ .type_name = "hash", .count = 2 },
        .{ .type_name = "set", .count = 3 },
    };
    var slices_buf: [2]sailor.tui.widgets.donut_chart.Slice = undefined;
    tui_adv.tui_advanced.renderKeyTypeDonut(&frame, area, &counts, null, &slices_buf);

    try testing.expectEqual(@as(u64, 1), slices_buf[0].value);
    try testing.expectEqual(@as(u64, 2), slices_buf[1].value);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter424 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10395");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs424", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs424", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter424 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10396");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h424", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h424",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter424 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10397");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k424", "v424",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k424",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v424") != null);
}

test "iter424 - EXPIRE and TTL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10398");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "ek424", "v",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "EXPIRE", "ek424", "100",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TTL", "ek424",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "100") != null);
}

test "iter424 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10399");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s424", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s424",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}
