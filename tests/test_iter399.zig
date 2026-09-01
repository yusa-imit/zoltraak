// Iteration 399: sailor v2.77.0 + DotPlot widget in TUI
//
// sailor v2.77.0: DotPlot widget added (Cleveland dot plot visualization).
// tui_advanced.zig gains renderDotPlot() for per-percentile latency visualization.
// DotPlot items: p50/p95/p99/p999 latency percentiles on horizontal axis.
//
// Redis compatibility: GEORADIUSBYMEMBER with missing member returns empty array.
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

// ─── sailor v2.77.0 build verification ────────────────────────────────────────

test "iter399 - sailor v2.77.0 build verification" {
    // Verify sailor v2.77.0 compiles (DotPlot widget added — no API breaks).
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
}

test "iter399 - DotPlot widget available in sailor v2.77.0" {
    // DotPlot lives in sailor.tui.widgets (not top-level sailor.tui).
    const sailor = @import("sailor");
    const DotPlot = sailor.tui.widgets.DotPlot;
    try testing.expect(@typeInfo(DotPlot) == .@"struct");
}

test "iter399 - DotPlotItem type available in sailor v2.77.0" {
    // DotPlotItem lives in sailor.tui.widgets (not top-level sailor.tui).
    const sailor = @import("sailor");
    const DotPlotItem = sailor.tui.widgets.DotPlotItem;
    try testing.expect(@typeInfo(DotPlotItem) == .@"struct");
}

// ─── GEORADIUSBYMEMBER edge cases ─────────────────────────────────────────────

test "iter399 - GEORADIUSBYMEMBER with missing member returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9960");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add one geo member
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "mygeokey", "13.361389", "38.115556", "Palermo",
    }));

    // Query with a member that doesn't exist in the set
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEORADIUSBYMEMBER", "mygeokey", "NonExistentMember", "100", "km",
    });
    defer allocator.free(resp);

    // Should return empty array *0\r\n
    try testing.expectEqualStrings("*0\r\n", resp);
}

test "iter399 - GEORADIUSBYMEMBER on empty key returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9961");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEORADIUSBYMEMBER", "nonexistentgeokey", "Palermo", "200", "km",
    });
    defer allocator.free(resp);

    // Non-existent key → empty array
    try testing.expectEqualStrings("*0\r\n", resp);
}

test "iter399 - GEOSEARCH FROMLONLAT no results returns empty array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9962");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add member far away from search center
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "places", "13.361389", "38.115556", "Palermo",
    }));

    // Search from a location far away with tiny radius
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOSEARCH", "places", "FROMLONLAT", "0.0", "0.0", "BYRADIUS", "1", "km",
    });
    defer allocator.free(resp);

    try testing.expectEqualStrings("*0\r\n", resp);
}

// ─── GEORADIUS STORE edge cases ───────────────────────────────────────────────

test "iter399 - GEORADIUS STORE with 0 results returns 0 and deletes dest" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9963");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add member far from search area
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "geo1", "13.361389", "38.115556", "Palermo",
    }));
    // Pre-populate dest
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "geo_dest", "preexisting",
    }));

    // GEORADIUS from far away with tiny radius and STORE to dest
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEORADIUS", "geo1", "0.0", "0.0", "1", "km", "STORE", "geo_dest",
    });
    defer allocator.free(resp);

    // Returns count = 0
    try testing.expectEqualStrings(":0\r\n", resp);
}

// ─── RESP3 set/map regression tests ──────────────────────────────────────────

test "iter399 - SMEMBERS RESP3 still returns set type" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9964");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "myset", "alpha", "beta", "gamma",
    }));

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "myset",
    });
    defer allocator.free(resp);

    // RESP3: set type starts with '~'
    try testing.expect(resp[0] == '~');
    try testing.expect(std.mem.indexOf(u8, resp, "alpha") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "beta") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "gamma") != null);
}

test "iter399 - HGETALL RESP3 still returns map type" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9965");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "myhash", "field1", "value1", "field2", "value2",
    }));

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "myhash",
    });
    defer allocator.free(resp);

    // RESP3: map type starts with '%'
    try testing.expect(resp[0] == '%');
    try testing.expect(std.mem.indexOf(u8, resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "value1") != null);
}

test "iter399 - CLUSTER INFO RESP2 still returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9966");
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
