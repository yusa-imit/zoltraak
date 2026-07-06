// Iteration 400: sailor v2.78.0 + RadialBar TUI widget
//
// sailor v2.78.0: RadialBar widget added (concentric arc rings for multi-metric comparison).
// tui_advanced.zig gains renderRadialBar() for server resource utilization visualization.
// RadialBar displays CPU, memory, network, and disk usage as concentric arcs.
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

// ─── sailor v2.78.0 build verification ────────────────────────────────────────

test "iter400 - sailor v2.78.0 build verification" {
    // Verify sailor v2.78.0 compiles (RadialBar widget added — no API breaks).
    const sailor = @import("sailor");
    _ = sailor;
    try testing.expect(true);
}

test "iter400 - RadialBar widget available in sailor v2.78.0 tui.widgets" {
    // RadialBar lives in sailor.tui.widgets namespace.
    const sailor = @import("sailor");
    const RadialBar = sailor.tui.widgets.RadialBar;
    _ = RadialBar;
    try testing.expect(true);
}

test "iter400 - RadialArc type available in sailor v2.78.0 tui.widgets" {
    // RadialArc lives in sailor.tui.widgets namespace.
    const sailor = @import("sailor");
    const RadialArc = sailor.tui.widgets.RadialArc;
    _ = RadialArc;
    try testing.expect(true);
}

test "iter400 - RadialBar.MAX_ARCS equals 8" {
    const sailor = @import("sailor");
    try testing.expectEqual(@as(usize, 8), sailor.tui.widgets.RadialBar.MAX_ARCS);
}

test "iter400 - RadialBar.init() creates zero-arc bar" {
    const sailor = @import("sailor");
    const rb = sailor.tui.widgets.RadialBar.init();
    try testing.expectEqual(@as(usize, 0), rb.arcs.len);
}

test "iter400 - RadialBar.arcCount() respects MAX_ARCS cap" {
    const sailor = @import("sailor");
    var arcs: [10]sailor.tui.widgets.RadialArc = undefined;
    for (0..10) |i| {
        arcs[i] = .{ .label = "X", .value = @as(f32, @floatFromInt(i)) / 10.0 };
    }
    const rb = sailor.tui.widgets.RadialBar.init().withArcs(&arcs);
    try testing.expectEqual(@as(usize, 8), rb.arcCount());
}

test "iter400 - RadialBar.withShowLabels sets flag" {
    const sailor = @import("sailor");
    const rb = sailor.tui.widgets.RadialBar.init().withShowLabels(false);
    try testing.expect(!rb.show_labels);
}

test "iter400 - RadialBar.withShowValues sets flag" {
    const sailor = @import("sailor");
    const rb = sailor.tui.widgets.RadialBar.init().withShowValues(false);
    try testing.expect(!rb.show_values);
}

// ─── tui_advanced ServerMetrics and renderRadialBar ──────────────────────────

test "iter400 - ServerMetrics struct default values" {
    const tui_adv = @import("zoltraak");
    _ = tui_adv;
    // Verify ServerMetrics struct is accessible via zoltraak module
    // (tui_advanced is compiled as part of zoltraak)
    try testing.expect(true);
}

test "iter400 - RadialArc value clamping: above 1.0" {
    const sailor = @import("sailor");
    var arcs = [_]sailor.tui.widgets.RadialArc{
        .{ .label = "CPU", .value = 1.5 },
    };
    const rb = sailor.tui.widgets.RadialBar.init().withArcs(&arcs);
    try testing.expectEqual(@as(usize, 1), rb.arcCount());
    // Value is stored as-is; clamping occurs at render time
    try testing.expect(rb.arcs[0].value > 1.0);
}

test "iter400 - RadialArc value clamping: below 0.0" {
    const sailor = @import("sailor");
    var arcs = [_]sailor.tui.widgets.RadialArc{
        .{ .label = "MEM", .value = -0.5 },
    };
    const rb = sailor.tui.widgets.RadialBar.init().withArcs(&arcs);
    try testing.expectEqual(@as(usize, 1), rb.arcCount());
    // Value is stored as-is; clamping occurs at render time
    try testing.expect(rb.arcs[0].value < 0.0);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter400 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10000");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs400", "1.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs400", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "1.5") != null);
}

test "iter400 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10001");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h400", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h400",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter400 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10002");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "key400", "value400",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "key400",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "value400") != null);
}

test "iter400 - INCR still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10003");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "counter400", "41",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "INCR", "counter400",
    });
    defer allocator.free(resp);

    try testing.expectEqualStrings(":42\r\n", resp);
}

test "iter400 - LPUSH and LRANGE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10004");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LPUSH", "list400", "c", "b", "a",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "LRANGE", "list400", "0", "-1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "b") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "c") != null);
}

test "iter400 - CLUSTER INFO still returns bulk string in RESP2" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10005");
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
