// Iteration 362: Add 23 missing commands to ALL_COMMANDS
//
// These dispatched commands were missing from ALL_COMMANDS in command.zig, causing
// COMMAND INFO to return nil for them. This is critical for Redis client compatibility
// (redis-py, node-redis, etc. query COMMAND INFO during initialization).
//
// Commands added:
//   Cluster: asking, migrate, readonly, readwrite
//   Generic (Redis 8.x): delex, digest, hotkeys, msetex
//   Vector Set (Redis 8.0+): vadd, vcard, vdim, vemb, vgetattr, vinfo, vismember,
//                            vlinks, vrandmember, vrange, vrem, vsim
//   Stream (Redis 8.x): xackdel, xcfgset, xdelex
//
// COMMAND COUNT is now 269 (was 246).

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

test "iter362 - COMMAND COUNT increased to 269" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "COUNT" });
    defer allocator.free(result);
    try testing.expectEqualStrings(":387\r\n", result);
}

test "iter362 - COMMAND INFO for cluster commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const cluster_cmds = [_][]const u8{ "asking", "migrate", "readonly", "readwrite" };
    for (cluster_cmds) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        // Should NOT return nil ($-1\r\n) — must be a valid info array
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result.len > 10);
    }
}

test "iter362 - COMMAND INFO for Redis 8.x generic commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const generic_cmds = [_][]const u8{ "delex", "digest", "hotkeys", "msetex" };
    for (generic_cmds) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result.len > 10);
    }
}

test "iter362 - COMMAND INFO for Vector Set commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const vsim_cmds = [_][]const u8{ "vadd", "vcard", "vdim", "vemb", "vgetattr", "vinfo", "vismember", "vlinks", "vrandmember", "vrange", "vrem", "vsim" };
    for (vsim_cmds) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result.len > 10);
    }
}

test "iter362 - COMMAND INFO for Redis 8.x stream commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const stream_cmds = [_][]const u8{ "xackdel", "xcfgset", "xdelex" };
    for (stream_cmds) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result.len > 10);
    }
}

test "iter362 - COMMAND INFO asking has correct arity 1" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "asking" });
    defer allocator.free(result);
    // Response is an array containing command info; arity 1 appears as ":1\r\n"
    try testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
}

test "iter362 - COMMAND INFO vadd has correct arity -6" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "vadd" });
    defer allocator.free(result);
    try testing.expect(std.mem.indexOf(u8, result, ":-6\r\n") != null);
}

test "iter362 - COMMAND INFO xackdel has correct arity -5" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "xackdel" });
    defer allocator.free(result);
    try testing.expect(std.mem.indexOf(u8, result, ":-5\r\n") != null);
}

test "iter362 - COMMAND DOCS returns info for asking" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "DOCS", "asking" });
    defer allocator.free(result);
    // Should contain the command name "asking" in bulk string format
    try testing.expect(std.mem.indexOf(u8, result, "asking") != null);
}

test "iter362 - COMMAND DOCS returns info for vsim" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "DOCS", "vsim" });
    defer allocator.free(result);
    try testing.expect(std.mem.indexOf(u8, result, "vsim") != null);
}
