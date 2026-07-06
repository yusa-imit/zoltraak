// Iteration 401: RESP3 verbatim string for DEBUG OBJECT
//
// In RESP2, DEBUG OBJECT returns a bulk string ($N\r\n...).
// In RESP3, DEBUG OBJECT now returns a verbatim string (=N\r\ntxt:...) matching
// the Redis 7.x RESP3 convention for human-readable text output (same pattern as
// CLIENT LIST, CLIENT INFO, INFO, CLUSTER INFO, LOLWUT).
//
// The protocol_version parameter was previously ignored (_: u8) in cmdDebug;
// it now drives the RESP format for the OBJECT subcommand.

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

// ─── DEBUG OBJECT RESP2 ───────────────────────────────────────────────────────

test "iter401 - DEBUG OBJECT RESP2 returns bulk string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14100");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "dbg401", "hello",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "dbg401",
    });
    defer allocator.free(resp);

    // RESP2: bulk string starts with '$'
    try testing.expect(resp[0] == '$');
    try testing.expect(std.mem.indexOf(u8, resp, "encoding:") != null);
}

test "iter401 - DEBUG OBJECT RESP2 contains key metadata fields" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14101");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "meta401", "world",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "meta401",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "refcount:1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "serializedlength:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "lru_seconds_idle:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "type:string") != null);
}

test "iter401 - DEBUG OBJECT RESP2 non-existent key returns error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14102");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "nokey401",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '-');
    try testing.expect(std.mem.indexOf(u8, resp, "no such key") != null);
}

// ─── DEBUG OBJECT RESP3 ───────────────────────────────────────────────────────

test "iter401 - DEBUG OBJECT RESP3 returns verbatim string" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14103");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "resp3key401", "value",
    }));

    // Switch to RESP3
    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "resp3key401",
    });
    defer allocator.free(resp);

    // RESP3: verbatim string starts with '='
    try testing.expect(resp[0] == '=');
}

test "iter401 - DEBUG OBJECT RESP3 verbatim string has txt: format tag" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14104");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "fmttag401", "data",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "fmttag401",
    });
    defer allocator.free(resp);

    // Verbatim string format: =N\r\ntxt:<data>\r\n
    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "txt:") != null);
}

test "iter401 - DEBUG OBJECT RESP3 verbatim string contains metadata" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14105");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "vmeta401", "testdata",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "vmeta401",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "encoding:") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "refcount:1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "type:string") != null);
}

test "iter401 - DEBUG OBJECT RESP3 for hash type shows hash encoding" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14106");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "hobj401", "f1", "v1", "f2", "v2",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "hobj401",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "type:hash") != null);
    // Small hash uses listpack encoding
    try testing.expect(std.mem.indexOf(u8, resp, "encoding:listpack") != null);
}

test "iter401 - DEBUG OBJECT RESP3 for list type shows list encoding" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14107");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RPUSH", "lobj401", "a", "b", "c",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "lobj401",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "type:list") != null);
}

test "iter401 - DEBUG OBJECT RESP3 non-existent key still returns error" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14108");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "missing401",
    });
    defer allocator.free(resp);

    // Error response is the same in RESP2 and RESP3
    try testing.expect(resp[0] == '-');
}

test "iter401 - DEBUG OBJECT RESP2 and RESP3 give same data, different framing" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14109");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "cmpkey401", "42",
    }));

    // RESP2 response
    const resp2 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "cmpkey401",
    });
    defer allocator.free(resp2);

    // Switch to RESP3
    ctx.registry.setProtocol(ctx.client_id, .RESP3);
    const resp3 = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "cmpkey401",
    });
    defer allocator.free(resp3);

    // RESP2 is bulk string, RESP3 is verbatim string
    try testing.expect(resp2[0] == '$');
    try testing.expect(resp3[0] == '=');

    // Both contain the same encoding and type info (data is the same)
    try testing.expect(std.mem.indexOf(u8, resp2, "encoding:int") != null);
    try testing.expect(std.mem.indexOf(u8, resp3, "encoding:int") != null);
    try testing.expect(std.mem.indexOf(u8, resp2, "type:string") != null);
    try testing.expect(std.mem.indexOf(u8, resp3, "type:string") != null);
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter401 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14110");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "key401r", "value401r",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "key401r",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "value401r") != null);
}

test "iter401 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14111");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h401r", "fld1", "val1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h401r",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "fld1") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "val1") != null);
}

test "iter401 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14112");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "z401r", "2.5", "member",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "z401r", "member",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter401 - DEBUG OBJECT RESP3 sorted set shows zset type" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14113");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zdbg401", "1.0", "a", "2.0", "b",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "zdbg401",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "type:zset") != null);
}

test "iter401 - DEBUG OBJECT RESP3 set type shows set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "14114");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "sdbg401", "x", "y", "z",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEBUG", "OBJECT", "sdbg401",
    });
    defer allocator.free(resp);

    try testing.expect(resp[0] == '=');
    try testing.expect(std.mem.indexOf(u8, resp, "type:set") != null);
}
