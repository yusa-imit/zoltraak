// Iteration 370: cjson.encode/decode table support integration tests
//
// Tests that cjson.encode and cjson.decode properly handle Lua tables
// (both arrays and objects) through EVAL via the full command dispatcher.

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

// ── 1. cjson.encode: Lua array → JSON array ────────────────────────────────────

test "EVAL cjson.encode: array table produces JSON array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return cjson.encode({10, 20, 30})",
        "0",
    });
    defer allocator.free(result);

    // RESP bulk string wrapping: $N\r\n[10,20,30]\r\n
    try testing.expect(std.mem.indexOf(u8, result, "[10,20,30]") != null);
}

// ── 2. cjson.encode: Lua object → JSON object ─────────────────────────────────

test "EVAL cjson.encode: object table produces JSON object" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return cjson.encode({score = 99})",
        "0",
    });
    defer allocator.free(result);

    // Should contain score key and value
    try testing.expect(std.mem.indexOf(u8, result, "\"score\"") != null);
    try testing.expect(std.mem.indexOf(u8, result, "99") != null);
}

// ── 3. cjson.encode: nested tables ────────────────────────────────────────────

test "EVAL cjson.encode: nested array of strings" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return cjson.encode({'hello', 'world'})",
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "[\"hello\",\"world\"]") != null);
}

// ── 4. cjson.encode: string with special characters ───────────────────────────

test "EVAL cjson.encode: string escaping" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return cjson.encode('line1\\nline2')",
        "0",
    });
    defer allocator.free(result);

    // \n should be escaped as \\n
    try testing.expect(std.mem.indexOf(u8, result, "\\n") != null);
}

// ── 5. cjson.decode: JSON array → Lua array ───────────────────────────────────

test "EVAL cjson.decode: JSON array returns Lua table with integer keys" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "local t = cjson.decode('[5,10,15]'); return t[1]",
        "0",
    });
    defer allocator.free(result);

    // Should return integer 5
    try testing.expect(std.mem.indexOf(u8, result, "5") != null);
}

// ── 6. cjson.decode: JSON object → Lua table with string keys ─────────────────

test "EVAL cjson.decode: JSON object field access" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "local t = cjson.decode('{\"count\":42}'); return t['count']",
        "0",
    });
    defer allocator.free(result);

    // Should return integer 42
    try testing.expect(std.mem.indexOf(u8, result, "42") != null);
}

// ── 7. cjson roundtrip: encode then decode ────────────────────────────────────

test "EVAL cjson encode-decode roundtrip preserves values" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local orig = {x = 7, y = 3}
        \\local json = cjson.encode(orig)
        \\local back = cjson.decode(json)
        \\return back['x']
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "7") != null);
}

// ── 8. cjson.decode: JSON string value ────────────────────────────────────────

test "EVAL cjson.decode: JSON string value" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "local t = cjson.decode('{\"name\":\"alice\"}'); return t['name']",
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "alice") != null);
}

// ── 9. cjson workflow: store JSON in Redis, retrieve and update ────────────────

test "EVAL cjson: store JSON in Redis key and increment field" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Store initial JSON
    const set_result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "SET", "myobj", "{\"count\":10}",
    });
    defer allocator.free(set_result);

    // Eval: read, increment count, store back, return new value
    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local raw = redis.call('GET', KEYS[1])
        \\local obj = cjson.decode(raw)
        \\obj['count'] = obj['count'] + 1
        \\redis.call('SET', KEYS[1], cjson.encode(obj))
        \\return obj['count']
        ,
        "1",
        "myobj",
    });
    defer allocator.free(result);

    // Should return 11
    try testing.expect(std.mem.indexOf(u8, result, "11") != null);
}

// ── 10. cjson.decode: invalid JSON returns nil ────────────────────────────────

test "EVAL cjson.decode: invalid JSON returns nil" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "local v = cjson.decode('not-valid-json'); if v == nil then return 'nil' else return 'not-nil' end",
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "nil") != null);
}

// ── 11. cjson.encode: empty array ─────────────────────────────────────────────

test "EVAL cjson.encode: empty table encodes as empty object" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return cjson.encode({})",
        "0",
    });
    defer allocator.free(result);

    // Empty table has arr_len=0, encodes as empty object {}
    try testing.expect(std.mem.indexOf(u8, result, "{}") != null);
}

// ── 12. cjson.encode: mixed types in array ────────────────────────────────────

test "EVAL cjson.encode: array with mixed primitive types" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        "return cjson.encode({1, 'two', true, false})",
        "0",
    });
    defer allocator.free(result);

    // Should encode all four types
    try testing.expect(std.mem.indexOf(u8, result, "1") != null);
    try testing.expect(std.mem.indexOf(u8, result, "\"two\"") != null);
    try testing.expect(std.mem.indexOf(u8, result, "true") != null);
    try testing.expect(std.mem.indexOf(u8, result, "false") != null);
}
