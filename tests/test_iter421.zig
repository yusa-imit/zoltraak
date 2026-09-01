// Iteration 421: sailor v2.92.0 + ToggleSwitch TUI widget
//
// sailor v2.92.0: ToggleSwitch widget added (boolean on/off slider-style
// form control — fixed 6-cell bracketed track `[◯    ]`/`[    ◉]` with a
// sliding knob, complementing Checkbox's tick-mark convention) plus
// ToggleSwitchGroup (manages a set of switches with radio-like exclusive-
// toggle focus navigation, skipping disabled items on wrap; no breaking
// changes). tui_advanced.zig gains renderServerFlagsPanel() using
// ToggleSwitch/ToggleSwitchGroup/ServerBooleanFlag to render a panel of
// server boolean CONFIG settings (appendonly, rdbcompression,
// stop-writes-on-bgsave-error, ...) as toggle-switch rows — a slider-style
// complement to the checkbox-style widgets used elsewhere in the TUI.
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

// ─── sailor v2.92.0 build verification ────────────────────────────────────────

test "iter421 - sailor v2.92.0 build verification" {
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
}

test "iter421 - ToggleSwitch widget available in sailor v2.92.0 tui.widgets" {
    const sailor = @import("sailor");
    const ToggleSwitch = sailor.tui.widgets.ToggleSwitch;
    try testing.expect(@typeInfo(ToggleSwitch) == .@"struct");
}

test "iter421 - ToggleSwitchGroup available in sailor v2.92.0 tui.widgets" {
    const sailor = @import("sailor");
    const ToggleSwitchGroup = sailor.tui.widgets.ToggleSwitchGroup;
    try testing.expect(@typeInfo(ToggleSwitchGroup) == .@"struct");
}

test "iter421 - ToggleSwitch.init defaults to unchecked" {
    const sailor = @import("sailor");
    const ts = sailor.tui.widgets.ToggleSwitch.init("appendonly");
    try testing.expect(!ts.checked);
    try testing.expectEqualStrings("appendonly", ts.label);
}

test "iter421 - ToggleSwitch.withChecked sets state without mutating original" {
    const sailor = @import("sailor");
    const ts1 = sailor.tui.widgets.ToggleSwitch.init("rdbcompression");
    const ts2 = ts1.withChecked(true);
    try testing.expect(!ts1.checked);
    try testing.expect(ts2.checked);
}

test "iter421 - ToggleSwitchGroup.init starts focus at index 0" {
    const sailor = @import("sailor");
    var items = [_]sailor.tui.widgets.ToggleSwitch{
        sailor.tui.widgets.ToggleSwitch.init("appendonly"),
        sailor.tui.widgets.ToggleSwitch.init("rdbcompression"),
    };
    const group = sailor.tui.widgets.ToggleSwitchGroup.init(&items);
    try testing.expectEqual(@as(usize, 0), group.focused);
}

// ─── tui_advanced ServerBooleanFlag + renderServerFlagsPanel ────────────────

test "iter421 - ServerBooleanFlag/renderServerFlagsPanel accessible via zoltraak module" {
    // renderServerFlagsPanel maps ServerBooleanFlag samples (name + enabled)
    // to sailor's ToggleSwitch/ToggleSwitchGroup for a boolean-CONFIG status panel.
    const tui_adv = @import("zoltraak").tui_advanced;
    const flag = tui_adv.ServerBooleanFlag{};
    try testing.expectEqualStrings("", flag.name);
    try testing.expectEqual(false, flag.enabled);
}

test "iter421 - ServerBooleanFlag default is name empty and disabled" {
    const tui_adv = @import("zoltraak");
    const flag = tui_adv.tui_advanced.ServerBooleanFlag{};
    try testing.expectEqualStrings("", flag.name);
    try testing.expect(!flag.enabled);
}

test "iter421 - renderServerFlagsPanel maps flags into ToggleSwitch buffer" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 40, 4) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 40, .height = 4 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const flags = [_]tui_adv.tui_advanced.ServerBooleanFlag{
        .{ .name = "appendonly", .enabled = true },
        .{ .name = "rdbcompression", .enabled = false },
    };
    var switch_buf: [4]sailor.tui.widgets.ToggleSwitch = undefined;
    tui_adv.tui_advanced.renderServerFlagsPanel(&frame, area, &flags, &switch_buf);

    try testing.expectEqualStrings("appendonly", switch_buf[0].label);
    try testing.expect(switch_buf[0].checked);
    try testing.expectEqualStrings("rdbcompression", switch_buf[1].label);
    try testing.expect(!switch_buf[1].checked);
}

test "iter421 - renderServerFlagsPanel is a no-op on zero-size area" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 10, 4) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 0, .height = 4 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const flags = [_]tui_adv.tui_advanced.ServerBooleanFlag{
        .{ .name = "appendonly", .enabled = true },
    };
    var switch_buf: [4]sailor.tui.widgets.ToggleSwitch = undefined;
    tui_adv.tui_advanced.renderServerFlagsPanel(&frame, area, &flags, &switch_buf);
    // Zero-width area must be a genuine no-op: buffer stays at its blank default.
    try testing.expectEqual(@as(u21, ' '), buf.getChar(0, 0));
}

test "iter421 - renderServerFlagsPanel truncates flags exceeding buffer capacity" {
    const sailor = @import("sailor");
    const tui_adv = @import("zoltraak");

    var buf = sailor.tui.Buffer.init(std.testing.allocator, 40, 4) catch unreachable;
    defer buf.deinit();
    const area = sailor.tui.Rect{ .x = 0, .y = 0, .width = 40, .height = 4 };
    var frame = sailor.tui.Frame{ .buffer = &buf, .area = area };

    const flags = [_]tui_adv.tui_advanced.ServerBooleanFlag{
        .{ .name = "a", .enabled = true },
        .{ .name = "b", .enabled = true },
        .{ .name = "c", .enabled = true },
    };
    var switch_buf: [2]sailor.tui.widgets.ToggleSwitch = undefined;
    tui_adv.tui_advanced.renderServerFlagsPanel(&frame, area, &flags, &switch_buf);

    try testing.expectEqualStrings("a", switch_buf[0].label);
    try testing.expectEqualStrings("b", switch_buf[1].label);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter421 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10380");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs421", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs421", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter421 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10381");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h421", "field1", "val1", "field2", "val2",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h421",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter421 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10382");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "k421", "v421",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "k421",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v421") != null);
}

test "iter421 - CONFIG GET appendonly still works correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10383");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CONFIG", "GET", "appendonly",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "appendonly") != null);
}

test "iter421 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10384");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "s421", "a", "b", "c",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "s421",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "a") != null);
}
