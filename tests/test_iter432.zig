// Iteration 432: RDB persistence — stream serialization + type-byte collision fix
//
// Found via stub audit of src/storage/persistence.zig: `.stream` was written to
// disk with type byte 0xFF, which is IDENTICAL to RDB_TYPE_EOF (0xFF) — saving
// a stream key caused `load()`/`loadFromBytes()` to treat that key's type byte
// as the end-of-file marker, silently truncating every key that followed it in
// the RDB payload (not just the stream itself). Separately, `.hyperloglog` used
// 0xFE, which is IDENTICAL to RDB_DB_SELECTOR (0xFE) — a HyperLogLog key's type
// byte was misread as "switch database" during load, corrupting the parse.
// `loadFromBytes` (the in-memory path used for replication full-sync, see
// `src/storage/replication.zig`) was also entirely missing cases for
// HyperLogLog and JSON, meaning any replica syncing from a master with such a
// key would fail the whole resync with `InvalidRdbFile`.
//
// Fixed: stream/hyperloglog now get dedicated non-colliding type bytes (5, 6),
// streams get a real entry-level serialize/deserialize (ids + flat field
// arrays + last_id/entries_added/max_deleted_entry_id bookkeeping; consumer
// groups are not yet persisted), and `loadFromBytes` gained the missing
// hyperloglog/json cases mirroring `load()`.
//
// These tests drive the real command dispatcher (SET/XADD/XLEN/XRANGE/PFADD/
// JSON.SET) and the real `Persistence` save/load entry points, per the
// project's documented "embedded tests can be dead code" pitfall — only tests
// routed through `commands.executeCommand` (or public storage/persistence
// APIs) and registered in build.zig are guaranteed to actually run under `zig
// build test`.

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

test "iter432 - stream entries survive a file-backed save/load round-trip" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10440");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "mystream", "1-1", "field1", "value1",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "mystream", "2-1", "field2", "value2", "field3", "value3",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const rdb_path = try std.fs.path.join(allocator, &.{ dir_path, "stream.rdb" });
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
    const fresh_client_id = try fresh_registry.registerClient("10441", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const len_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "XLEN", "mystream",
    });
    defer allocator.free(len_resp);
    try testing.expectEqualStrings(":2\r\n", len_resp);

    const range_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "XRANGE", "mystream", "-", "+",
    });
    defer allocator.free(range_resp);
    try testing.expect(std.mem.indexOf(u8, range_resp, "1-1") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "2-1") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "value1") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "field3") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "value3") != null);
}

test "iter432 - a stream key no longer truncates keys written after it (regression for the 0xFF/EOF collision)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10442");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Stream key first, plain string key after — with the old 0xFF type byte,
    // load() would see the stream's type byte, mistake it for RDB_TYPE_EOF,
    // and stop parsing before ever reaching "afterkey".
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "beforestream", "*", "f", "v",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "SET", "afterkey", "afterval",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const rdb_path = try std.fs.path.join(allocator, &.{ dir_path, "trunc.rdb" });
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
    const fresh_client_id = try fresh_registry.registerClient("10443", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const get_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "GET", "afterkey",
    });
    defer allocator.free(get_resp);
    try testing.expectEqualStrings("$8\r\nafterval\r\n", get_resp);
}

test "iter432 - HyperLogLog + JSON keys round-trip through the in-memory replication path (loadFromBytes)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10444");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "PFADD", "myhll", "a", "b", "c",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "JSON.SET", "myjson", "$", "{\"x\":1}",
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
    try testing.expectEqual(@as(usize, 3), loaded);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10445", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const pfcount_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "PFCOUNT", "myhll",
    });
    defer allocator.free(pfcount_resp);
    try testing.expectEqualStrings(":3\r\n", pfcount_resp);

    const json_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "JSON.GET", "myjson",
    });
    defer allocator.free(json_resp);
    try testing.expect(std.mem.indexOf(u8, json_resp, "\"x\":1") != null);

    const trailing_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "GET", "trailing",
    });
    defer allocator.free(trailing_resp);
    try testing.expectEqualStrings("$9\r\nstillhere\r\n", trailing_resp);
}
