// Iteration 398: RESP3 verbatim string for CLUSTER INFO + sailor v2.76.0
//
// Redis protocol behavior:
// - CLUSTER INFO RESP2: returns bulk string ($N\r\n...) with cluster status fields
// - CLUSTER INFO RESP3: returns verbatim string (=N\r\ntxt:...) with "txt" encoding hint
//
// sailor v2.76.0: FunnelChart widget added (library update, tui_advanced.zig uses for cache stats)

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

// ─── CLUSTER INFO RESP2 ────────────────────────────────────────────────────────

test "iter398 - CLUSTER INFO RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9850");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    // RESP2: bulk string starts with '$'
    try testing.expect(resp[0] == '$');
    try testing.expect(resp[0] != '=');
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled:0") != null);
}

test "iter398 - CLUSTER INFO RESP2 contains all required fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9851");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "cluster_state:ok") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_slots_assigned") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_known_nodes") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_total_shards:0") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_current_epoch") != null);
}

// ─── CLUSTER INFO RESP3 ────────────────────────────────────────────────────────

test "iter398 - CLUSTER INFO RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9852");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    // RESP3: verbatim string starts with '='
    try testing.expect(resp[0] == '=');
    try testing.expect(resp[0] != '$');
    // Contains "txt:" encoding hint
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_enabled:0") != null);
}

test "iter398 - CLUSTER INFO RESP3 verbatim string format is correct" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9853");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    // Format: =<len>\r\ntxt:<data>\r\n
    try testing.expect(resp[0] == '=');
    try testing.expect(std.ascii.isDigit(resp[1]));
    // Find the \r\n separator after the length
    const crlf_pos = std.mem.indexOf(u8, resp, "\r\n") orelse unreachable;
    // After \r\n, the "txt:" prefix must appear
    const after_header = resp[crlf_pos + 2 ..];
    try testing.expect(std.mem.startsWith(u8, after_header, "txt:"));
}

test "iter398 - CLUSTER INFO RESP3 contains all required fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9854");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    // All fields present in RESP3 verbatim format
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_state:ok") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_slots_assigned") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_known_nodes") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_total_shards:0") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "total_cluster_links_buffer_limit_exceeded:0") != null);
}

test "iter398 - CLUSTER INFO RESP2 vs RESP3 format differs" {
    const allocator = testing.allocator;

    // RESP2 client
    var ctx2 = try setup(allocator, "9855");
    defer ctx2.storage.deinit();
    defer ctx2.registry.deinit();
    defer ctx2.ps.deinit();

    const resp2 = try execCmd(allocator, ctx2.storage, &ctx2.registry, ctx2.client_id, &ctx2.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp2);

    // RESP3 client
    var ctx3 = try setup(allocator, "9856");
    defer ctx3.storage.deinit();
    defer ctx3.registry.deinit();
    defer ctx3.ps.deinit();

    allocator.free(try execCmd(allocator, ctx3.storage, &ctx3.registry, ctx3.client_id, &ctx3.ps, &.{
        "HELLO", "3",
    }));

    const resp3 = try execCmd(allocator, ctx3.storage, &ctx3.registry, ctx3.client_id, &ctx3.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp3);

    // Different encoding types
    try testing.expect(resp2[0] == '$'); // RESP2: bulk string
    try testing.expect(resp3[0] == '='); // RESP3: verbatim string

    // Both contain the same cluster info content
    try testing.expect(std.mem.indexOf(u8, resp2, "cluster_enabled:0") != null);
    try testing.expect(std.mem.indexOf(u8, resp3, "cluster_enabled:0") != null);
}

test "iter398 - CLUSTER INFO RESP3 cluster_enabled is first field" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9857");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    // cluster_enabled must appear before cluster_state
    const enabled_pos = std.mem.indexOf(u8, resp, "cluster_enabled:0") orelse unreachable;
    const state_pos = std.mem.indexOf(u8, resp, "cluster_state:") orelse unreachable;
    try testing.expect(enabled_pos < state_pos);
}

test "iter398 - CLUSTER INFO RESP3 switches back to RESP2 after HELLO 2" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9858");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // First upgrade to RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp3 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp3);
    try testing.expect(resp3[0] == '=');

    // Downgrade back to RESP2
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "2",
    }));

    const resp2 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp2);
    try testing.expect(resp2[0] == '$');
}

test "iter398 - CLUSTER INFO RESP3 Redis 7.x fields present" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9859");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HELLO", "3",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CLUSTER", "INFO",
    });
    defer allocator.free(resp);

    // Redis 7.x additions
    try testing.expect(std.mem.indexOf(u8, resp, "cluster_total_shards:0") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "total_cluster_links_buffer_limit_exceeded:0") != null);
}
