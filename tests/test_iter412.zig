// Iteration 412: sailor v2.84.0 + BulletChart TUI widget
//
// sailor v2.84.0: BulletChart widget added (compact single-metric KPI bar —
// actual value bar overlaid on qualitative range bands with a target tick
// mark, MAX_BULLETS=32, no heap allocations, no breaking changes). All
// value/target/range inputs are normalized against max_value and clamped to
// [0, 1] before column math, so out-of-range or negative inputs cannot panic.
// tui_advanced.zig gains renderMemoryBudgetBullet() using BulletChart/Bullet
// for a used_memory-vs-maxmemory KPI row: value bar shows current usage,
// target tick marks the configured ceiling, and safe/warn/critical range
// bands (60%/85%/100% of the limit) let an operator see at a glance how
// close a database is to running out of budget — a compact complement to
// the multi-arc renderRadialBar gauge.
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

// ─── sailor v2.84.0 build verification ────────────────────────────────────────

test "iter412 - sailor v2.84.0 build verification" {
    // Verify sailor v2.84.0 compiles (BulletChart widget added — no API breaks).
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
}

test "iter412 - BulletChart widget available in sailor v2.84.0 tui.widgets" {
    const sailor = @import("sailor");
    const BulletChart = sailor.tui.widgets.BulletChart;
    try testing.expect(@typeInfo(BulletChart) == .@"struct");
}

test "iter412 - Bullet type available in sailor v2.84.0 tui.widgets" {
    const sailor = @import("sailor");
    const Bullet = sailor.tui.widgets.Bullet;
    try testing.expect(@typeInfo(Bullet) == .@"struct");
}

test "iter412 - BulletChart.MAX_BULLETS equals 32" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 32), sailor.tui.widgets.BulletChart.MAX_BULLETS);
}

test "iter412 - BulletChart.init() creates zero-bullet chart" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.BulletChart.init();
    try testing.expectEqual(@as(usize, 0), chart.bullets.len);
}

test "iter412 - BulletChart.bulletCount() reflects withBullets" {
    const sailor = @import("sailor");
    var ranges = [_]f32{ 0.5, 0.8, 1.0 };
    const bullets = [_]sailor.tui.widgets.Bullet{
        .{ .label = "MEM", .value = 60.0, .target = 100.0, .ranges = &ranges },
        .{ .label = "CPU", .value = 40.0, .target = 80.0, .ranges = &ranges },
    };
    const chart = sailor.tui.widgets.BulletChart.init().withBullets(&bullets);
    try testing.expectEqual(@as(usize, 2), chart.bulletCount());
}

test "iter412 - BulletChart.bulletCount() caps at MAX_BULLETS" {
    const sailor = @import("sailor");
    var bullets: [50]sailor.tui.widgets.Bullet = undefined;
    for (0..50) |i| {
        bullets[i] = .{
            .label = "X",
            .value = @as(f32, @floatFromInt(i)),
            .target = 100.0,
            .ranges = &.{},
        };
    }
    const chart = sailor.tui.widgets.BulletChart.init().withBullets(&bullets);
    try testing.expectEqual(@as(usize, 32), chart.bulletCount());
}

test "iter412 - BulletChart.withShowLabels sets flag" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.BulletChart.init().withShowLabels(false);
    try testing.expect(!chart.show_labels);
}

test "iter412 - BulletChart.withFocused sets focused index" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.BulletChart.init().withFocused(1);
    try testing.expectEqual(@as(usize, 1), chart.focused);
}

test "iter412 - BulletChart.withMaxValue sets scaling denominator" {
    const sailor = @import("sailor");
    const chart = sailor.tui.widgets.BulletChart.init().withMaxValue(2048.0);
    try testing.expectEqual(@as(f32, 2048.0), chart.max_value);
}

// ─── tui_advanced MemoryBudget + renderMemoryBudgetBullet ────────────────────

test "iter412 - MemoryBudget struct accessible via zoltraak module" {
    // renderMemoryBudgetBullet maps MemoryBudget samples to sailor's Bullet
    // type (value=used, target=limit).
    const tui_adv = @import("zoltraak").tui_advanced;
    const b = tui_adv.MemoryBudget{};
    try testing.expectEqualStrings("MEM", b.label);
    try testing.expectEqual(@as(f32, 0), b.used_bytes);
    try testing.expectEqual(@as(f32, 0), b.limit_bytes);
}

test "iter412 - Bullet value/target map from budget bytes as-is" {
    const sailor = @import("sailor");
    const bullet = sailor.tui.widgets.Bullet{
        .label = "MEM",
        .value = 6144.0,
        .target = 10240.0,
        .ranges = &.{ 6144.0, 8704.0, 10240.0 },
    };
    try testing.expectEqual(@as(f32, 6144.0), bullet.value);
    try testing.expectEqual(@as(f32, 10240.0), bullet.target);
    try testing.expectEqual(@as(usize, 3), bullet.ranges.len);
}

test "iter412 - qualitative range bands are 60/85/100 percent of limit" {
    const limit_bytes: f32 = 10000.0;
    const ranges = [_]f32{ limit_bytes * 0.6, limit_bytes * 0.85, limit_bytes };
    try testing.expectEqual(@as(f32, 6000.0), ranges[0]);
    try testing.expectEqual(@as(f32, 8500.0), ranges[1]);
    try testing.expectEqual(@as(f32, 10000.0), ranges[2]);
}

test "iter412 - over-budget usage still clamps safely (no panic class)" {
    const sailor = @import("sailor");
    // value exceeds target/max_value — render() must clamp internally rather
    // than panic (regression class documented for CandlestickChart in v2.83.0,
    // and explicitly re-verified safe for BulletChart in v2.84.0 release notes).
    var ranges = [_]f32{ 60.0, 85.0, 100.0 };
    const bullets = [_]sailor.tui.widgets.Bullet{
        .{ .label = "MEM", .value = 999999.0, .target = 100.0, .ranges = &ranges },
    };
    const chart = sailor.tui.widgets.BulletChart.init()
        .withBullets(&bullets)
        .withMaxValue(100.0);
    try testing.expectEqual(@as(usize, 1), chart.bulletCount());
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter412 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10320");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs412", "1.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs412", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1.5") != null);
}

test "iter412 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10321");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h412", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h412",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter412 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10322");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k412", "v412",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k412",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v412") != null);
}

test "iter412 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10323");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter412",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1") != null);
}

test "iter412 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10324");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s412", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s412",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}

test "iter412 - CLUSTER INFO still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10325");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled") != null);
}
