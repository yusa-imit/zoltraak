// Iteration 441: DUMP/RESTORE type-byte collision + missing serialization fix
//
// Found via stub audit of `Storage.dumpValue`/`Storage.restoreValue` (src/storage/memory.zig),
// the backing implementation for the DUMP/RESTORE commands (and therefore AOF rewrite's
// `RESTORE`-based path for these types too): `dumpValue` tagged `.stream` as type byte 0xFF
// and `.hyperloglog` as 0xFE, but `restoreValue`'s switch read 0xFE as Stream and 0xFD as
// HyperLogLog — completely mismatched from what dumpValue actually wrote. A DUMP'd stream
// (0xFF) hit restoreValue's `else => error.UnknownDumpType` (hard failure), while a DUMP'd
// HyperLogLog (0xFE) was silently misinterpreted as a Stream payload (garbage/corruption,
// no error). JSON (dumped as 0x0F) had no restore case at all — DUMP+RESTORE of any JSON key
// always failed. Worse, `dumpValue` never implemented real serialization for timeseries,
// bloom, cuckoo, count_min_sketch, top_k, or vector_set — it wrote a bare zero-length
// placeholder for each, silently discarding all their data on every DUMP (and thus on every
// COPY-via-DUMP path or AOF rewrite that relied on it).
//
// Fixed: unified, collision-free type-byte scheme (0x00-0x0E) shared by both dumpValue and
// restoreValue, and dumpValue/restoreValue now delegate to each type's own
// `rdbSerialize`/`rdbDeserialize` (already implemented for persistence.zig's RDB format in
// prior iterations) instead of ad hoc/placeholder logic.
//
// These tests drive the real command dispatcher (JSON.SET/XADD/PFADD/BF.ADD/CF.ADD/
// CMS.INITBYDIM/TOPK.RESERVE/TDIGEST.CREATE/TS.CREATE) plus the real DUMP/RESTORE commands,
// per the project's documented "embedded tests can be dead code" pitfall — only tests routed
// through `commands.executeCommand` and registered in build.zig are guaranteed to run under
// `zig build test`.

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

/// DUMP a key and assert it's a non-null bulk string; returns the owned payload.
fn dumpKey(allocator: std.mem.Allocator, storage: *Storage, registry: *ClientRegistry, client_id: u64, ps: *PubSub, key: []const u8) ![]const u8 {
    const resp = try execCmd(allocator, storage, registry, client_id, ps, &.{ "DUMP", key });
    defer allocator.free(resp);
    try testing.expect(resp.len > 4 and resp[0] == '$');
    // Parse "$<len>\r\n<payload>\r\n"
    const nl = std.mem.indexOf(u8, resp, "\r\n").?;
    const len = try std.fmt.parseInt(usize, resp[1..nl], 10);
    const payload_start = nl + 2;
    return allocator.dupe(u8, resp[payload_start .. payload_start + len]);
}

test "iter441 - JSON key survives DUMP/RESTORE round-trip" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10441");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "JSON.SET", "myjson", "$", "{\"a\":1,\"b\":[1,2,3]}",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "myjson");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "myjson",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "myjson", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const get_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "JSON.GET", "myjson",
    });
    defer allocator.free(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "\"a\":1") != null);
}

test "iter441 - Stream key survives DUMP/RESTORE round-trip (was type-byte mismatch)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10442");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "mystream", "1-1", "field1", "value1",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "mystream", "2-1", "field2", "value2",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "mystream");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "mystream",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "mystream", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const len_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XLEN", "mystream",
    });
    defer allocator.free(len_resp);
    try testing.expectEqualStrings(":2\r\n", len_resp);
}

test "iter441 - HyperLogLog key survives DUMP/RESTORE round-trip (was silently misread as Stream)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10443");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "PFADD", "myhll", "a", "b", "c", "d", "e",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "myhll");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "myhll",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "myhll", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const count_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "PFCOUNT", "myhll",
    });
    defer allocator.free(count_resp);
    try testing.expectEqualStrings(":5\r\n", count_resp);
}

test "iter441 - Bloom filter key survives DUMP/RESTORE round-trip (was empty placeholder)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10444");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "BF.ADD", "mybloom", "hello",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "mybloom");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "mybloom",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "mybloom", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const exists_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "BF.EXISTS", "mybloom", "hello",
    });
    defer allocator.free(exists_resp);
    try testing.expectEqualStrings(":1\r\n", exists_resp);

    const missing_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "BF.EXISTS", "mybloom", "nope",
    });
    defer allocator.free(missing_resp);
    try testing.expectEqualStrings(":0\r\n", missing_resp);
}

test "iter441 - Cuckoo filter key survives DUMP/RESTORE round-trip (was empty placeholder)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10445");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CF.ADD", "mycuckoo", "hello",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "mycuckoo");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "mycuckoo",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "mycuckoo", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const exists_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CF.EXISTS", "mycuckoo", "hello",
    });
    defer allocator.free(exists_resp);
    try testing.expectEqualStrings(":1\r\n", exists_resp);
}

test "iter441 - Count-Min Sketch key survives DUMP/RESTORE round-trip (was empty placeholder)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10446");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CMS.INITBYDIM", "mycms", "100", "5",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CMS.INCRBY", "mycms", "apple", "7",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "mycms");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "mycms",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "mycms", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const query_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CMS.QUERY", "mycms", "apple",
    });
    defer allocator.free(query_resp);
    try testing.expect(std.mem.indexOf(u8, query_resp, "7") != null);
}

test "iter441 - Top-K key survives DUMP/RESTORE round-trip (was empty placeholder)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10447");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TOPK.RESERVE", "mytopk", "3",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TOPK.ADD", "mytopk", "a", "b", "a", "a", "c",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "mytopk");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "mytopk",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "mytopk", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const list_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TOPK.LIST", "mytopk",
    });
    defer allocator.free(list_resp);
    try testing.expect(std.mem.indexOf(u8, list_resp, "a") != null);
}

test "iter441 - T-Digest key survives DUMP/RESTORE round-trip (was empty placeholder)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10448");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TDIGEST.CREATE", "mytdigest",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TDIGEST.ADD", "mytdigest", "1.0", "2.0", "3.0",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "mytdigest");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "mytdigest",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "mytdigest", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const min_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TDIGEST.MIN", "mytdigest",
    });
    defer allocator.free(min_resp);
    try testing.expect(std.mem.indexOf(u8, min_resp, "1") != null);
}

test "iter441 - Time Series key survives DUMP/RESTORE round-trip (was empty placeholder)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10449");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.CREATE", "myts",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.ADD", "myts", "100", "42.5",
    }));

    const payload = try dumpKey(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, "myts");
    defer allocator.free(payload);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "DEL", "myts",
    }));

    const restore_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "RESTORE", "myts", "0", payload,
    });
    defer allocator.free(restore_resp);
    try testing.expectEqualStrings("+OK\r\n", restore_resp);

    const get_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.GET", "myts",
    });
    defer allocator.free(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "42.5") != null);
}
