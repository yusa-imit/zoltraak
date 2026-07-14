// Iteration 415: JSON string escaping fix (object keys + full control-char escaping)
//
// src/storage/json_value.zig's JsonNode.stringify() left a `// TODO: escape
// special characters` marker even though the value-escaping switch already
// handled the common cases (quote, backslash, \n, \r, \t). Two real bugs
// remained:
//   1. Object keys were written raw (`entry.key_ptr.*`) with NO escaping at
//      all, even though keys come from parsed input and may contain quotes,
//      backslashes, or control bytes — producing invalid JSON output for
//      JSON.GET/JSON.SET round-trips whenever a key needed escaping.
//   2. Control characters without a short JSON escape (e.g. 0x01, 0x08
//      backspace, 0x0C form feed) were copied through as raw bytes instead
//      of being \u-escaped (or given their short-form \b / \f escape),
//      which is invalid per RFC 8259 (control chars 0x00-0x1F must be
//      escaped in a JSON string).
//
// Fix: both string values and object keys now go through a single
// writeEscapedJsonString() helper that escapes quotes, backslashes, \b, \f,
// \n, \r, \t, and \u00XX-escapes every other control character.
//
// Verify no regression in existing commands.

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
const JsonNode = zoltraak.json_value.JsonNode;

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

// ─── JsonNode.stringify() escaping — storage-layer unit coverage ────────────

test "iter415 - stringify escapes quotes and backslashes in object keys" {
    const allocator = testing.allocator;

    // Key unescapes to: a"b\c (raw quote + raw backslash bytes).
    const node = try JsonNode.parse(allocator, "{\"a\\\"b\\\\c\":1}");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const str = try node.stringify(allocator);
    defer allocator.free(str);

    // Must round-trip through a real JSON parser without erroring — before
    // the fix, the raw unescaped quote in the key broke the JSON structure.
    const reparsed = try JsonNode.parse(allocator, str);
    defer {
        reparsed.deinit(allocator);
        allocator.destroy(reparsed);
    }

    try testing.expect(reparsed.object.contains("a\"b\\c"));
}

test "iter415 - stringify escapes backspace and form feed with short escapes" {
    const allocator = testing.allocator;

    const node = try allocator.create(JsonNode);
    defer allocator.destroy(node);
    node.* = JsonNode{ .string = "a\x08b\x0Cc" };

    const str = try node.stringify(allocator);
    defer allocator.free(str);

    try testing.expectEqualStrings("\"a\\bb\\fc\"", str);
}

test "iter415 - stringify \\u-escapes control characters with no short form" {
    const allocator = testing.allocator;

    const node = try allocator.create(JsonNode);
    defer allocator.destroy(node);
    node.* = JsonNode{ .string = "a\x01b\x1Fc" };

    const str = try node.stringify(allocator);
    defer allocator.free(str);

    try testing.expectEqualStrings("\"a\\u0001b\\u001fc\"", str);
}

test "iter415 - stringify still escapes control characters inside object keys" {
    const allocator = testing.allocator;

    const node = try allocator.create(JsonNode);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }
    var map = std.StringHashMap(*JsonNode).init(allocator);
    const key = try allocator.dupe(u8, "a\x01b");
    const val = try allocator.create(JsonNode);
    val.* = JsonNode{ .number = 1.0 };
    try map.put(key, val);
    node.* = JsonNode{ .object = map };

    const str = try node.stringify(allocator);
    defer allocator.free(str);

    try testing.expectEqualStrings("{\"a\\u0001b\":1}", str);
}

// ─── JSON.SET / JSON.GET RESP round-trip — command-layer regression ─────────

test "iter415 - JSON.SET/JSON.GET round-trips a value whose object key needs escaping" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10340");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // The key "a\"b" requires escaping when serialized back out.
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "JSON.SET", "j415", "$", "{\"a\\\"b\":1}",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "JSON.GET", "j415",
    });
    defer allocator.free(resp);

    // Extract the RESP bulk-string payload and confirm it's valid,
    // re-parseable JSON containing the original key.
    const body_start = std.mem.indexOf(u8, resp, "\r\n").? + 2;
    const body = std.mem.trimRight(u8, resp[body_start..], "\r\n");

    const reparsed = try JsonNode.parse(allocator, body);
    defer {
        reparsed.deinit(allocator);
        allocator.destroy(reparsed);
    }
    try testing.expect(reparsed.object.contains("a\"b"));
}

// ─── Redis command regression tests ──────────────────────────────────────────

test "iter415 - ZADD and ZSCORE still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10341");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZADD", "zs415", "2.5", "member1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ZSCORE", "zs415", "member1",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "2.5") != null);
}

test "iter415 - HSET and HGETALL still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10342");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HSET", "h415", "f1", "v1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "HGETALL", "h415",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "v1") != null);
}

test "iter415 - SET and GET still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10343");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "s415", "hello",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GET", "s415",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "hello") != null);
}

test "iter415 - SADD and SMEMBERS still work correctly" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10344");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SADD", "set415", "m1",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SMEMBERS", "set415",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.indexOf(u8, resp, "m1") != null);
}
