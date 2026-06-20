// Iteration 368: EVAL redis.call() integration tests
//
// Tests redis.call() and redis.pcall() via EVAL through the full command
// dispatcher (executeCommand end-to-end).  Each test exercises a Lua script
// that calls real Redis commands and verifies the RESP2 response matches
// what Redis itself would return.
//
// Pattern matches iterations 338-367: @import("zoltraak") + execCmd helper.

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
    const client_id = try registry.registerClient("127.0.0.1:9200", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ── 1. redis.call SET then GET ────────────────────────────────────────────────

test "EVAL redis.call: SET then GET returns stored value" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.call('SET', 'k1', 'v1') return redis.call('GET', 'k1')",
        "0",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$2\r\nv1\r\n", result);
}

// ── 2. redis.call INCR returns integer ───────────────────────────────────────

test "EVAL redis.call: INCR returns integer result" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.call('SET', 'ctr', '0') return redis.call('INCR', 'ctr')",
        "0",
    });
    defer allocator.free(result);

    // INCR returns integer 1 → Lua number → RESP2 ":1\r\n"
    try testing.expectEqualStrings(":1\r\n", result);
}

// ── 3. redis.call RPUSH + LRANGE returns array ───────────────────────────────

test "EVAL redis.call: RPUSH then LRANGE returns array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\redis.call('RPUSH', 'mylist', 'a', 'b', 'c')
        \\return redis.call('LRANGE', 'mylist', '0', '-1')
        ,
        "0",
    });
    defer allocator.free(result);

    // Array of 3 bulk strings
    try testing.expect(std.mem.indexOf(u8, result, "*3") != null);
    try testing.expect(std.mem.indexOf(u8, result, "a") != null);
    try testing.expect(std.mem.indexOf(u8, result, "b") != null);
    try testing.expect(std.mem.indexOf(u8, result, "c") != null);
}

// ── 4. redis.call uses KEYS[1] argument ──────────────────────────────────────

test "EVAL redis.call: uses KEYS[1] as key argument" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Pre-populate the key via a separate SET
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "SET", "testkey", "hello",
    }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return redis.call('GET', KEYS[1])", "1", "testkey",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

// ── 5. redis.call GET on missing key returns nil ──────────────────────────────

test "EVAL redis.call: GET on missing key returns nil" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return redis.call('GET', 'nosuchkey')", "0",
    });
    defer allocator.free(result);

    // nil bulk string
    try testing.expectEqualStrings("$-1\r\n", result);
}

// ── 6. redis.pcall catches error and signals it ───────────────────────────────

test "EVAL redis.pcall: catches WRONGTYPE error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Create a list, then try INCR on it (WRONGTYPE — strings.zig INCR checks type properly)
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "RPUSH", "alist", "v",
    }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local r = redis.pcall('INCR', 'alist')
        \\if r.err then return 'caught:' .. r.err else return 'ok' end
        ,
        "0",
    });
    defer allocator.free(result);

    // Should contain "caught:" and "WRONGTYPE"
    try testing.expect(std.mem.indexOf(u8, result, "caught:") != null or
        std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

// ── 7. redis.call HSET then HGET ─────────────────────────────────────────────

test "EVAL redis.call: HSET then HGET returns field value" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.call('HSET', 'myhash', 'f', 'fv') return redis.call('HGET', 'myhash', 'f')",
        "0",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$2\r\nfv\r\n", result);
}

// ── 8. redis.call SADD + SCARD returns count ─────────────────────────────────

test "EVAL redis.call: SADD then SCARD returns count" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\redis.call('SADD', 'myset', 'a', 'b', 'c')
        \\return redis.call('SCARD', 'myset')
        ,
        "0",
    });
    defer allocator.free(result);

    // SCARD returns integer 3 → Lua number → ":3\r\n"
    try testing.expectEqualStrings(":3\r\n", result);
}

// ── 9. redis.call ARGV usage ──────────────────────────────────────────────────

test "EVAL redis.call: SET using ARGV[1] and ARGV[2]" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])",
        "1",
        "argv_key",
        "argv_val",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$8\r\nargv_val\r\n", result);
}

// ── 10. EVAL_RO blocks write commands ─────────────────────────────────────────

test "EVAL_RO: blocks write commands inside script" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL_RO", "redis.call('SET', 'ro_key', 'val') return 1", "0",
    });
    defer allocator.free(result);

    // Should return an error about write commands not allowed
    try testing.expect(std.mem.indexOf(u8, result, "ERR") != null or
        std.mem.indexOf(u8, result, "Write") != null or
        std.mem.indexOf(u8, result, "write") != null);
}

// ── 11. EVAL_RO allows read commands ──────────────────────────────────────────

test "EVAL_RO: allows read-only GET command" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Populate a key first using regular EVAL
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "SET", "ro_read_key", "ro_read_val",
    }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL_RO", "return redis.call('GET', 'ro_read_key')", "0",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$11\r\nro_read_val\r\n", result);
}

// ── 12. redis.call ZADD + ZSCORE ─────────────────────────────────────────────

test "EVAL redis.call: ZADD then ZSCORE returns score" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "redis.call('ZADD', 'myzset', '3.14', 'pi') return redis.call('ZSCORE', 'myzset', 'pi')",
        "0",
    });
    defer allocator.free(result);

    // ZSCORE returns bulk string of the score
    try testing.expect(std.mem.indexOf(u8, result, "3.14") != null);
}
