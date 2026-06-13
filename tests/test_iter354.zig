// Iteration 354: Missing CONFIG parameters
//
// Adds client-output-buffer-limit and other Redis 6/7 CONFIG parameters that
// Redis clients (redis-py, ioredis) query during initialization.
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

test "iter354 - CONFIG GET client-output-buffer-limit returns default" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "client-output-buffer-limit" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "client-output-buffer-limit") != null);
    try testing.expect(std.mem.indexOf(u8, result, "normal 0 0 0") != null);
}

test "iter354 - CONFIG SET client-output-buffer-limit updates value" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const set_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "SET", "client-output-buffer-limit", "normal 0 0 0 slave 0 0 0 pubsub 0 0 0" });
    defer allocator.free(set_result);
    try testing.expectEqualStrings("+OK\r\n", set_result);

    const get_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "client-output-buffer-limit" });
    defer allocator.free(get_result);
    try testing.expect(std.mem.indexOf(u8, get_result, "normal 0 0 0 slave 0 0 0 pubsub 0 0 0") != null);
}

test "iter354 - CONFIG GET acllog-max-entries returns 128" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "acllog-max-entries" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "acllog-max-entries") != null);
    try testing.expect(std.mem.indexOf(u8, result, "128") != null);
}

test "iter354 - CONFIG GET cluster-announce-tls-port returns 0" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "cluster-announce-tls-port" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "cluster-announce-tls-port") != null);
    try testing.expect(std.mem.indexOf(u8, result, "0") != null);
}

test "iter354 - CONFIG GET latency-history-enabled returns yes" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "latency-history-enabled" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "latency-history-enabled") != null);
    try testing.expect(std.mem.indexOf(u8, result, "yes") != null);
}

test "iter354 - CONFIG GET close-on-oom-score-adj returns no" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "close-on-oom-score-adj" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "close-on-oom-score-adj") != null);
    try testing.expect(std.mem.indexOf(u8, result, "no") != null);
}

test "iter354 - CONFIG GET * includes client-output-buffer-limit" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "CONFIG", "GET", "*" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "client-output-buffer-limit") != null);
}
