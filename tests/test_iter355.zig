// Iteration 355: INFO everything + missing CONFIG params
//
// 1. Fixed `INFO everything` section — previously returned empty response since
//    "EVERYTHING" was not recognized; now returns all sections (same as "all")
//    including latencystats.
// 2. Added missing RDB/AOF/replication CONFIG parameters commonly queried by
//    Redis clients: dbfilename, rdbcompression, rdbchecksum,
//    repl-diskless-sync-timeout, script-time-limit, auto-aof-rewrite-percentage,
//    auto-aof-rewrite-min-size, aof-load-truncated.
// 3. Migrated to sailor v2.39.0 (NumberInput widget).
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
    for (args, 0..) |a, i| {
        resp_args[i] = .{ .bulk_string = a };
    }
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

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9200", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ── INFO everything tests ─────────────────────────────────────────────────────

test "iter355 - INFO everything returns non-empty response" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "INFO", "everything" });
    defer allocator.free(result);

    // Should not be empty or error
    try testing.expect(result.len > 10);
    try testing.expect(std.mem.indexOf(u8, result, "-ERR") == null);
}

test "iter355 - INFO everything includes server section" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "INFO", "everything" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "# Server") != null);
    try testing.expect(std.mem.indexOf(u8, result, "redis_version:") != null);
}

test "iter355 - INFO everything includes memory section" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "INFO", "everything" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "# Memory") != null);
    try testing.expect(std.mem.indexOf(u8, result, "used_memory:") != null);
}

test "iter355 - INFO everything includes keyspace section" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "INFO", "everything" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "# Keyspace") != null);
}

// ── CONFIG parameter tests ───────────────────────────────────────────────────

test "iter355 - CONFIG GET dbfilename returns dump.rdb" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "dbfilename" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "dbfilename") != null);
    try testing.expect(std.mem.indexOf(u8, result, "dump.rdb") != null);
}

test "iter355 - CONFIG GET rdbcompression returns yes" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "rdbcompression" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "rdbcompression") != null);
    try testing.expect(std.mem.indexOf(u8, result, "yes") != null);
}

test "iter355 - CONFIG GET rdbchecksum returns yes" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "rdbchecksum" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "rdbchecksum") != null);
    try testing.expect(std.mem.indexOf(u8, result, "yes") != null);
}

test "iter355 - CONFIG GET script-time-limit returns 5000" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "script-time-limit" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "script-time-limit") != null);
    try testing.expect(std.mem.indexOf(u8, result, "5000") != null);
}

test "iter355 - CONFIG GET auto-aof-rewrite-percentage returns 100" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "auto-aof-rewrite-percentage" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "auto-aof-rewrite-percentage") != null);
    try testing.expect(std.mem.indexOf(u8, result, "100") != null);
}

test "iter355 - CONFIG GET repl-diskless-sync-timeout returns 5" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "repl-diskless-sync-timeout" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "repl-diskless-sync-timeout") != null);
    try testing.expect(std.mem.indexOf(u8, result, "5") != null);
}
