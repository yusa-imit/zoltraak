// Iteration 369: Lua Script Helper Functions integration tests
//
// Tests redis.status_reply(), redis.error_reply(), redis.log(),
// redis.replicate_commands(), redis.sha1hex(), redis.setresp(),
// and redis.LOG_* constants via EVAL through the full command dispatcher.

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const ScriptStore = scripting.ScriptStore;
const transactions_mod = zoltraak.transactions_commands;

fn execCmdWithStore(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| resp_args[i] = .{ .bulk_string = a };
    const cmd = RespValue{ .array = resp_args };
    var tx = transactions_mod.TxState.init(allocator);
    defer tx.deinit();
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
        script_store,
        null,
        &databases,
        1,
    );
}

fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) ![]const u8 {
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    return execCmdWithStore(allocator, storage, client_registry, client_id, ps, &script_store, args);
}

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9201", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ── 1. redis.status_reply returns status reply ────────────────────────────────

test "EVAL redis.status_reply: returns +OK status" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.status_reply('OK')",
        "0",
    });
    defer allocator.free(result);

    // {ok = 'OK'} → +OK\r\n
    try testing.expectEqualStrings("+OK\r\n", result);
}

// ── 2. redis.status_reply with custom message ─────────────────────────────────

test "EVAL redis.status_reply: returns custom status message" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.status_reply('QUEUED')",
        "0",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("+QUEUED\r\n", result);
}

// ── 3. redis.error_reply returns error reply ──────────────────────────────────

test "EVAL redis.error_reply: returns -ERR error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.error_reply('ERR something went wrong')",
        "0",
    });
    defer allocator.free(result);

    // {err = 'ERR something went wrong'} → -ERR something went wrong\r\n
    try testing.expectEqualStrings("-ERR something went wrong\r\n", result);
}

// ── 4. redis.error_reply with WRONGTYPE ───────────────────────────────────────

test "EVAL redis.error_reply: returns WRONGTYPE error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.error_reply('WRONGTYPE Operation against a key holding the wrong kind of value')",
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

// ── 5. redis.log does not crash ───────────────────────────────────────────────

test "EVAL redis.log: runs without error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.log(redis.LOG_NOTICE, 'test message') return 1",
        "0",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings(":1\r\n", result);
}

// ── 6. redis.LOG_* constants are accessible ───────────────────────────────────

test "EVAL redis.log constants: LOG_DEBUG=0, LOG_WARNING=3" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.LOG_DEBUG + redis.LOG_VERBOSE + redis.LOG_NOTICE + redis.LOG_WARNING",
        "0",
    });
    defer allocator.free(result);

    // 0 + 1 + 2 + 3 = 6
    try testing.expectEqualStrings(":6\r\n", result);
}

// ── 7. redis.replicate_commands returns true ──────────────────────────────────

test "EVAL redis.replicate_commands: returns true (deprecated no-op)" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "local ok = redis.replicate_commands() return ok and 1 or 0",
        "0",
    });
    defer allocator.free(result);

    // replicate_commands() returns true → 1
    try testing.expectEqualStrings(":1\r\n", result);
}

// ── 8. redis.sha1hex computes correct hash ────────────────────────────────────

test "EVAL redis.sha1hex: empty string SHA1" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.sha1hex('')",
        "0",
    });
    defer allocator.free(result);

    // SHA1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
    try testing.expectEqualStrings("$40\r\nda39a3ee5e6b4b0d3255bfef95601890afd80709\r\n", result);
}

// ── 9. redis.sha1hex with known input ────────────────────────────────────────

test "EVAL redis.sha1hex: known string SHA1" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return redis.sha1hex('hello')",
        "0",
    });
    defer allocator.free(result);

    // SHA1("hello") = aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
    try testing.expectEqualStrings("$40\r\naaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d\r\n", result);
}

// ── 10. redis.setresp(2) is accepted ─────────────────────────────────────────

test "EVAL redis.setresp: version 2 accepted without error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.setresp(2) return 1",
        "0",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings(":1\r\n", result);
}

// ── 11. redis.setresp(3) is accepted ─────────────────────────────────────────

test "EVAL redis.setresp: version 3 accepted without error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.setresp(3) return 'ok'",
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "ok") != null);
}

// ── 12. redis.setresp with invalid version returns error ──────────────────────

test "EVAL redis.setresp: invalid version returns error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.setresp(4) return 1",
        "0",
    });
    defer allocator.free(result);

    // Should get an error (setresp only allows 2 or 3)
    try testing.expect(std.mem.startsWith(u8, result, "-"));
}
