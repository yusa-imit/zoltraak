// Iteration 437: RDB persistence — Time Series + Vector Set real serialization
//
// Found via stub audit of src/storage/persistence.zig: `.timeseries` and
// `.vector_set` values were written to disk as a bare 4-byte zero-length
// placeholder ("Time series/Vector set not yet implemented in persistence")
// instead of a real serialized blob. Worse, neither `load()` nor
// `loadFromBytes()` had a case for the 0xFD (time series) / 0xF7 (vector set)
// type bytes at all — any RDB payload containing a TS.* or V* key failed the
// *entire* load with `error.InvalidRdbFile`, taking every other key in the
// file down with it (not just the unsupported one).
//
// Fixed: added `TimeSeriesValue.rdbSerialize`/`rdbDeserialize`
// (src/storage/timeseries.zig) and `VectorSetValue.rdbSerialize`/
// `rdbDeserialize` (src/storage/vector.zig), following the same
// self-contained length-prefixed binary format already used by
// bloom/cuckoo/count-min-sketch/top-k/t-digest, and wired them into both
// save paths and both load paths (`load()` and `loadFromBytes()`) in
// src/storage/persistence.zig.
//
// These tests drive the real command dispatcher (TS.CREATE/TS.ADD/TS.RANGE,
// VADD/VCARD/VEMB) and the real `Persistence` save/load entry points, per
// the project's documented "embedded tests can be dead code" pitfall — only
// tests routed through `commands.executeCommand` (or public storage/
// persistence APIs) and registered in build.zig are guaranteed to actually
// run under `zig build test`.

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
const Persistence = zoltraak.persistence.Persistence;

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

test "iter437 - time series samples + labels survive a file-backed save/load round-trip" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10450");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.CREATE", "mytemp", "LABELS", "sensor", "outdoor",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.ADD", "mytemp", "1000", "21.5",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.ADD", "mytemp", "2000", "22.75",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const rdb_path = try std.fs.path.join(allocator, &.{ dir_path, "ts.rdb" });
    defer allocator.free(rdb_path);

    var databases = [_]Storage{ctx.storage.*};
    try Persistence.save(&databases, rdb_path, allocator);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    var fresh_databases = [_]Storage{fresh.*};
    const loaded = try Persistence.load(&fresh_databases, rdb_path, allocator);
    fresh.* = fresh_databases[0];
    try testing.expectEqual(@as(usize, 1), loaded);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10451", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const range_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "TS.RANGE", "mytemp", "-", "+",
    });
    defer allocator.free(range_resp);
    try testing.expect(std.mem.indexOf(u8, range_resp, "1000") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "21.5") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "2000") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "22.75") != null);
}

test "iter437 - a time series key no longer corrupts the whole RDB load (regression for the missing 0xFD load case)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10452");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "TS.ADD", "beforets", "1000", "1.0",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "afterkey", "afterval",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const rdb_path = try std.fs.path.join(allocator, &.{ dir_path, "ts_trunc.rdb" });
    defer allocator.free(rdb_path);

    var databases = [_]Storage{ctx.storage.*};
    try Persistence.save(&databases, rdb_path, allocator);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    var fresh_databases = [_]Storage{fresh.*};
    const loaded = try Persistence.load(&fresh_databases, rdb_path, allocator);
    fresh.* = fresh_databases[0];
    try testing.expectEqual(@as(usize, 2), loaded);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10453", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const get_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "GET", "afterkey",
    });
    defer allocator.free(get_resp);
    try testing.expectEqualStrings("$8\r\nafterval\r\n", get_resp);
}

test "iter437 - vector set entries + attributes round-trip through the in-memory replication path (loadFromBytes)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10454");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "v1",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "VADD", "myvec", "VALUES", "3", "4.0", "5.0", "6.0", "v2",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "trailing", "stillhere",
    }));

    var databases = [_]Storage{ctx.storage.*};
    const bytes = try Persistence.saveToBytes(&databases, allocator);
    defer allocator.free(bytes);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    var fresh_databases = [_]Storage{fresh.*};
    const loaded = try Persistence.loadFromBytes(&fresh_databases, bytes, allocator);
    fresh.* = fresh_databases[0];
    // Two top-level keys: "myvec" (a vector set holding both v1 and v2 —
    // the two VADDs above add entries to the same key, not separate keys)
    // and "trailing".
    try testing.expectEqual(@as(usize, 2), loaded);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10455", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const card_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "VCARD", "myvec",
    });
    defer allocator.free(card_resp);
    try testing.expectEqualStrings(":2\r\n", card_resp);

    const emb_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "VEMB", "myvec", "v1",
    });
    defer allocator.free(emb_resp);
    try testing.expect(std.mem.indexOf(u8, emb_resp, "1") != null);

    const trailing_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "GET", "trailing",
    });
    defer allocator.free(trailing_resp);
    try testing.expectEqualStrings("$9\r\nstillhere\r\n", trailing_resp);
}
