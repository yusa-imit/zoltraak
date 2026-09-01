// Iteration 423: sailor v2.93.0 + CalendarHeatmap TUI widget
//
// sailor v2.93.0: CalendarHeatmap widget added (GitHub-style contribution/
// activity heatmap — date-indexed f32 values rendered as a grid of 5-level
// intensity glyphs ' ','░','▒','▓','█', rows = day-of-week, columns = week,
// with optional month/weekday labels and focused-cell highlighting; up to
// MAX_ENTRIES=371 entries; no heap allocations) plus Calendar.setRange(), a
// convenience wrapper over setRangeStart/setRangeEnd. No breaking changes.
// tui_advanced.zig gains renderKeyActivityHeatmap() using CalendarHeatmap/
// DailyActivityCount to render a rolling window of daily command/expiry
// activity counts as a contribution-style grid.
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

// ─── sailor v2.93.0 build verification ────────────────────────────────────────

test "iter423 - sailor v2.93.0 build verification" {
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
}

test "iter423 - CalendarHeatmap widget available in sailor v2.93.0 tui.widgets" {
    const sailor = @import("sailor");
    const CalendarHeatmap = sailor.tui.widgets.CalendarHeatmap;
    try testing.expect(@typeInfo(CalendarHeatmap) == .@"struct");
}

test "iter423 - Calendar widget available in sailor v2.93.0 tui.widgets" {
    const sailor = @import("sailor");
    const Calendar = sailor.tui.widgets.Calendar;
    try testing.expect(@typeInfo(Calendar) == .@"struct");
}

test "iter423 - CalendarHeatmap.MAX_ENTRIES equals 371" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 371), sailor.tui.widgets.CalendarHeatmap.MAX_ENTRIES);
}

test "iter423 - CalendarHeatmap.init defaults to no values" {
    const sailor = @import("sailor");
    const start = sailor.tui.widgets.Calendar.Date.init(2026, 1, 1);
    const hm = sailor.tui.widgets.CalendarHeatmap.init(start);
    try testing.expectEqual(@as(usize, 0), hm.values.len);
    try testing.expectEqual(@as(u16, 2026), hm.start_date.year);
}

test "iter423 - CalendarHeatmap.withValues sets values without mutating original" {
    const sailor = @import("sailor");
    const start = sailor.tui.widgets.Calendar.Date.init(2026, 1, 1);
    var vals = [_]f32{ 1.0, 2.0, 3.0 };
    const hm1 = sailor.tui.widgets.CalendarHeatmap.init(start);
    const hm2 = hm1.withValues(&vals);
    try testing.expectEqual(@as(usize, 0), hm1.values.len);
    try testing.expectEqual(@as(usize, 3), hm2.values.len);
}

test "iter423 - Calendar.setRange sets both start and end" {
    const sailor = @import("sailor");
    const Date = sailor.tui.widgets.Calendar.Date;
    var cal = sailor.tui.widgets.Calendar.init(Date.init(2026, 1, 1));
    const start = Date.init(2026, 2, 1);
    const end = Date.init(2026, 2, 10);
    cal.setRange(start, end);
    try testing.expectEqual(@as(?Date, start), cal.range_start);
    try testing.expectEqual(@as(?Date, end), cal.range_end);
}

// ─── tui_advanced DailyActivityCount + renderKeyActivityHeatmap ─────────────

test "iter423 - DailyActivityCount/renderKeyActivityHeatmap accessible via zoltraak module" {
    // renderKeyActivityHeatmap maps DailyActivityCount samples into sailor's
    // CalendarHeatmap for a rolling command/expiry activity contribution grid.
    const tui_adv = @import("zoltraak").tui_advanced;
    const d = tui_adv.DailyActivityCount{};
    try testing.expectEqual(@as(u64, 0), d.count);
}

test "iter423 - DailyActivityCount default count is zero" {
    const tui_adv = @import("zoltraak");
    const day = tui_adv.tui_advanced.DailyActivityCount{};
    try testing.expectEqual(@as(u64, 0), day.count);
}

test "iter423 - renderKeyActivityHeatmap maps daily counts into values buffer" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 40, 10) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 40, .height = 10 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const start = sailor.tui.widgets.Calendar.Date.init(2026, 1, 1);
    const counts = [_]tui_adv.tui_advanced.DailyActivityCount{
        .{ .count = 5 },
        .{ .count = 0 },
        .{ .count = 12 },
    };
    var values_buf: [8]f32 = undefined;
    tui_adv.tui_advanced.renderKeyActivityHeatmap(&frame, area, start, &counts, null, &values_buf);

    try testing.expectEqual(@as(f32, 5.0), values_buf[0]);
    try testing.expectEqual(@as(f32, 0.0), values_buf[1]);
    try testing.expectEqual(@as(f32, 12.0), values_buf[2]);
}

test "iter423 - renderKeyActivityHeatmap is a no-op on zero-size area" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 10, 4) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 0, .height = 4 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const start = sailor.tui.widgets.Calendar.Date.init(2026, 1, 1);
    const counts = [_]tui_adv.tui_advanced.DailyActivityCount{.{ .count = 3 }};
    var values_buf: [4]f32 = undefined;
    tui_adv.tui_advanced.renderKeyActivityHeatmap(&frame, area, start, &counts, null, &values_buf);
    // Zero-width area must be a genuine no-op: buffer stays at its blank default.
    try testing.expectEqual(@as(u21, ' '), buf.getChar(0, 0));
}

test "iter423 - renderKeyActivityHeatmap truncates counts exceeding values buffer capacity" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 40, 10) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 40, .height = 10 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const start = sailor.tui.widgets.Calendar.Date.init(2026, 1, 1);
    const counts = [_]tui_adv.tui_advanced.DailyActivityCount{
        .{ .count = 1 }, .{ .count = 2 }, .{ .count = 3 },
    };
    var values_buf: [2]f32 = undefined;
    tui_adv.tui_advanced.renderKeyActivityHeatmap(&frame, area, start, &counts, null, &values_buf);

    try testing.expectEqual(@as(f32, 1.0), values_buf[0]);
    try testing.expectEqual(@as(f32, 2.0), values_buf[1]);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter423 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10390");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs423", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs423", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter423 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10391");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h423", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h423",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter423 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10392");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k423", "v423",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k423",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v423") != null);
}

test "iter423 - EXPIRE and TTL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10393");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "ek423", "v",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "EXPIRE", "ek423", "100",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TTL", "ek423",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "100") != null);
}

test "iter423 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10394");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s423", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s423",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}
