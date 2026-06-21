// Iteration 371: struct.pack/unpack/size integration tests
//
// Tests that the struct library (lua-struct 0.2 compatible) works correctly
// for binary packing/unpacking in Lua scripts via EVAL.

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

// ── 1. struct.pack: little-endian 4-byte int ─────────────────────────────────

test "EVAL struct.pack: little-endian int produces correct bytes" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local s = struct.pack('<i', 1)
        \\return #s
        ,
        "0",
    });
    defer allocator.free(result);

    // Should be 4 bytes
    try testing.expect(std.mem.indexOf(u8, result, "4") != null);
}

// ── 2. struct.pack/unpack: integer roundtrip ─────────────────────────────────

test "EVAL struct.pack/unpack: integer roundtrip" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('<i', 42)
        \\local v, _ = struct.unpack('<i', packed)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "42") != null);
}

// ── 3. struct.pack/unpack: big-endian integer ────────────────────────────────

test "EVAL struct.pack/unpack: big-endian integer roundtrip" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('>i', 12345)
        \\local v, _ = struct.unpack('>i', packed)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "12345") != null);
}

// ── 4. struct.pack/unpack: double (8 bytes) ──────────────────────────────────

test "EVAL struct.pack/unpack: double value" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('d', 100.0)
        \\local v, _ = struct.unpack('d', packed)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "100") != null);
}

// ── 5. struct.size: correct byte count ───────────────────────────────────────

test "EVAL struct.size: calculates byte count correctly" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        // b=1, h=2, i=4, d=8 → total 15
        \\return struct.size('<bhid')
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "15") != null);
}

// ── 6. struct.pack/unpack: multiple values in sequence ───────────────────────

test "EVAL struct.pack/unpack: pack three values, unpack second" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('<iii', 10, 20, 30)
        \\local a, b, c = struct.unpack('<iii', packed)
        \\return b
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "20") != null);
}

// ── 7. struct.unpack: init position skips first value ────────────────────────

test "EVAL struct.unpack: init position parameter" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('<ii', 111, 222)
        \\local v, _ = struct.unpack('<i', packed, 5)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "222") != null);
}

// ── 8. struct.pack/unpack: byte (1 byte signed) ──────────────────────────────

test "EVAL struct.pack/unpack: signed byte" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('b', -1)
        \\local v, _ = struct.unpack('b', packed)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "-1") != null);
}

// ── 9. struct.pack: padding byte (x) ─────────────────────────────────────────

test "EVAL struct.pack: x padding byte adds zero byte" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        // bxb = byte, padding, byte → 3 bytes total
        \\local packed = struct.pack('bxb', 1, 2)
        \\return #packed
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "3") != null);
}

// ── 10. struct used for Redis-style score packing ────────────────────────────

test "EVAL struct: pack double score for sorted set emulation" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    // Simulate a pattern where scores are stored as packed doubles
    allocator.free(try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "SET", "score_key", "99.5",
    }));

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local score = tonumber(redis.call('GET', KEYS[1]))
        \\local packed = struct.pack('d', score)
        \\local unpacked, _ = struct.unpack('d', packed)
        \\if math.abs(unpacked - score) < 0.001 then
        \\  return 'ok'
        \\else
        \\  return 'fail'
        \\end
        ,
        "1",
        "score_key",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "ok") != null);
}

// ── 11. struct.pack: fixed-length string (c5) ────────────────────────────────

test "EVAL struct.pack/unpack: fixed-length string c5" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('c5', 'hello')
        \\local v, _ = struct.unpack('c5', packed)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "hello") != null);
}

// ── 12. struct.pack/unpack: unsigned short ───────────────────────────────────

test "EVAL struct.pack/unpack: unsigned short H" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL",
        \\local packed = struct.pack('<H', 65000)
        \\local v, _ = struct.unpack('<H', packed)
        \\return v
        ,
        "0",
    });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "65000") != null);
}
