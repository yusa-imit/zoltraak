// Iteration 373: FCALL/FCALL_RO proper RESP encoding fix
//
// Previously, FCALL used luaValueToString() which produced incorrect RESP output:
//   - nil → "$3\r\nnil\r\n" (string "nil") instead of "$-1\r\n" (null)
//   - false → "$5\r\nfalse\r\n" instead of "$-1\r\n" (null, Redis convention)
//   - true → "$4\r\ntrue\r\n" instead of ":1\r\n" (integer 1, Redis convention)
//   - 42 → "$2\r\n42\r\n" (string) instead of ":42\r\n" (integer)
//   - {1,2,3} → "$7\r\n[table]\r\n" instead of "*3\r\n:1\r\n:2\r\n:3\r\n" (array)
//
// After fix, FCALL uses luaToRESP2Buf() — same as EVAL — for correct RESP encoding.

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

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
    script_store: ScriptStore,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9201", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    const script_store = ScriptStore.init(allocator);
    return .{
        .storage = storage,
        .registry = registry,
        .ps = ps,
        .client_id = client_id,
        .script_store = script_store,
    };
}

// ─── FCALL return type tests ──────────────────────────────────────────────────

test "FCALL string return → bulk string RESP encoding" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    // Load library
    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib1\nredis.register_function('strfunc', function(keys, args) return 'hello' end)",
    });
    defer allocator.free(load_result);
    try testing.expect(std.mem.indexOf(u8, load_result, "lib1") != null);

    // Call FCALL
    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "strfunc", "0",
    });
    defer allocator.free(result);
    // String "hello" → $5\r\nhello\r\n
    try testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "FCALL integer return → integer RESP encoding" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib2\nredis.register_function('intfunc', function(keys, args) return 42 end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "intfunc", "0",
    });
    defer allocator.free(result);
    // Number 42 → :42\r\n (integer), NOT "$2\r\n42\r\n" (string)
    try testing.expectEqualStrings(":42\r\n", result);
}

test "FCALL nil return → null bulk string" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib3\nredis.register_function('nilfunc', function(keys, args) return nil end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "nilfunc", "0",
    });
    defer allocator.free(result);
    // nil → $-1\r\n (null bulk string), NOT "$3\r\nnil\r\n"
    try testing.expectEqualStrings("$-1\r\n", result);
}

test "FCALL false return → null bulk string (Redis convention)" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib4\nredis.register_function('falsefunc', function(keys, args) return false end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "falsefunc", "0",
    });
    defer allocator.free(result);
    // false → $-1\r\n (null bulk string), NOT "$5\r\nfalse\r\n"
    try testing.expectEqualStrings("$-1\r\n", result);
}

test "FCALL true return → integer 1 (Redis convention)" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib5\nredis.register_function('truefunc', function(keys, args) return true end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "truefunc", "0",
    });
    defer allocator.free(result);
    // true → :1\r\n (integer 1), NOT "$4\r\ntrue\r\n"
    try testing.expectEqualStrings(":1\r\n", result);
}

test "FCALL array return → RESP array encoding" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib6\nredis.register_function('arrayfunc', function(keys, args) return {1, 2, 3} end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "arrayfunc", "0",
    });
    defer allocator.free(result);
    // {1,2,3} → *3\r\n:1\r\n:2\r\n:3\r\n, NOT "$7\r\n[table]\r\n"
    try testing.expectEqualStrings("*3\r\n:1\r\n:2\r\n:3\r\n", result);
}

test "FCALL with KEYS — reads first key" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib7\nredis.register_function('keyfunc', function(keys, args) return keys[1] end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "keyfunc", "1", "mykey",
    });
    defer allocator.free(result);
    // keys[1] = "mykey" → $5\r\nmykey\r\n
    try testing.expectEqualStrings("$5\r\nmykey\r\n", result);
}

test "FCALL error table → RESP error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib8\nredis.register_function('errfunc', function(keys, args) return redis.error_reply('my error') end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "errfunc", "0",
    });
    defer allocator.free(result);
    // error_reply("my error") → -my error\r\n
    try testing.expectEqualStrings("-my error\r\n", result);
}

test "FCALL status table → RESP simple string" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib9\nredis.register_function('okfunc', function(keys, args) return redis.status_reply('PONG') end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "okfunc", "0",
    });
    defer allocator.free(result);
    // status_reply("PONG") → +PONG\r\n
    try testing.expectEqualStrings("+PONG\r\n", result);
}

test "FCALL function not found → error response" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "nonexistent", "0",
    });
    defer allocator.free(result);
    // Should be an error response
    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "FCALL_RO string return → bulk string RESP encoding" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib10\nredis.register_function('rstrfunc', function(keys, args) return 'readonly' end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL_RO", "rstrfunc", "0",
    });
    defer allocator.free(result);
    // String "readonly" → $8\r\nreadonly\r\n
    try testing.expectEqualStrings("$8\r\nreadonly\r\n", result);
}

test "FCALL string array → mixed string array" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib11\nredis.register_function('sarrayfunc', function(keys, args) return {'hello', 'world'} end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "sarrayfunc", "0",
    });
    defer allocator.free(result);
    // {'hello', 'world'} → *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
    try testing.expectEqualStrings("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", result);
}

test "FCALL ARGV access" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.script_store.deinit();

    const load_result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FUNCTION", "LOAD",
        "#!lua name=lib12\nredis.register_function('argvfunc', function(keys, args) return args[1] end)",
    });
    defer allocator.free(load_result);

    const result = try execCmdWithStore(allocator, s.storage, &s.registry, s.client_id, &s.ps, &s.script_store, &.{
        "FCALL", "argvfunc", "0", "myarg",
    });
    defer allocator.free(result);
    // args[1] = "myarg" → $5\r\nmyarg\r\n
    try testing.expectEqualStrings("$5\r\nmyarg\r\n", result);
}
