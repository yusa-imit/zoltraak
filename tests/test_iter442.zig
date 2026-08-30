// Iteration 442: AOF rewrite (BGREWRITEAOF) data loss for stream/HyperLogLog/JSON/
// timeseries/bloom/cuckoo/count-min-sketch/top-k/t-digest/vector-set keys.
//
// Found via stub audit of src/storage/aof.zig: `Aof.rewrite()` (the routine behind
// BGREWRITEAOF that compacts the current dataset into a fresh AOF file) had a switch
// arm for `.stream` that did nothing but comment "Streams not yet implemented in AOF -
// skip for now", and a completely empty arm for `.hyperloglog`. The remaining advanced
// types (json/timeseries/bloom/cuckoo/count_min_sketch/top_k/vector_set) had comments
// claiming they were "handled by X.* commands in AOF replay" — but that's only true for
// the *incremental* AOF log; `rewrite()` throws away the old log and replaces it with a
// freshly generated one, so any key of these types was silently dropped from the
// rewritten file, i.e. running BGREWRITEAOF permanently deleted that data from the AOF
// (the in-memory copy survives until the next restart, then it's gone for good).
//
// Fixed: rewrite() now serializes these types via Storage.dumpValueLocked() (the same
// binary format DUMP/RESTORE/MIGRATE use, made lock-safe for use under rewrite()'s
// already-held storage.mutex) and emits a `RESTORE key ttl <payload> REPLACE` command;
// executeStorageCommand() (the AOF replay dispatcher) gained a RESTORE handler so those
// entries actually get replayed instead of being silently ignored as an unknown command.
//
// These tests drive the real Aof.rewrite()/Aof.replay() entry points (not the embedded
// tests in src/storage/aof.zig, which are dead code under `zig build test` per the
// project's documented pitfall) against a real Storage populated via the command
// dispatcher, matching the pattern established in tests/test_iter432.zig.

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
const Aof = zoltraak.aof.Aof;

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

const Ctx = struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,

    fn deinit(self: *Ctx) void {
        self.storage.deinit();
        self.registry.deinit();
        self.ps.deinit();
    }
};

fn setup(allocator: std.mem.Allocator, port_str: []const u8) !Ctx {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient(port_str, 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

test "iter442 - stream survives BGREWRITEAOF-style rewrite + replay (regression for silent drop)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10450");
    defer ctx.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "mystream", "1-1", "field1", "value1",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "mystream", "2-1", "field2", "value2",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const aof_path = try std.fs.path.join(allocator, &.{ dir_path, "stream.aof" });
    defer allocator.free(aof_path);

    try Aof.rewrite(ctx.storage, aof_path, allocator);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    const replayed = try Aof.replay(fresh, aof_path, allocator);
    try testing.expect(replayed >= 1);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10451", 10, "127.0.0.1:6379");
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
    try testing.expect(std.mem.indexOf(u8, range_resp, "field1") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "value1") != null);
    try testing.expect(std.mem.indexOf(u8, range_resp, "2-1") != null);
}

test "iter442 - HyperLogLog survives BGREWRITEAOF-style rewrite + replay (regression for silent drop)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10452");
    defer ctx.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "PFADD", "myhll", "a", "b", "c", "d",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const aof_path = try std.fs.path.join(allocator, &.{ dir_path, "hll.aof" });
    defer allocator.free(aof_path);

    try Aof.rewrite(ctx.storage, aof_path, allocator);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    _ = try Aof.replay(fresh, aof_path, allocator);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10453", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const pfcount_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "PFCOUNT", "myhll",
    });
    defer allocator.free(pfcount_resp);
    try testing.expectEqualStrings(":4\r\n", pfcount_resp);
}

test "iter442 - a trailing plain key after an advanced-type key is not dropped by rewrite" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10454");
    defer ctx.deinit();

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
    const aof_path = try std.fs.path.join(allocator, &.{ dir_path, "trailing.aof" });
    defer allocator.free(aof_path);

    try Aof.rewrite(ctx.storage, aof_path, allocator);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    _ = try Aof.replay(fresh, aof_path, allocator);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10455", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const get_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "GET", "afterkey",
    });
    defer allocator.free(get_resp);
    try testing.expectEqualStrings("$8\r\nafterval\r\n", get_resp);
}

test "iter442 - stream TTL is preserved across rewrite + replay" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10456");
    defer ctx.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "XADD", "ttlstream", "*", "f", "v",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "EXPIRE", "ttlstream", "10000",
    }));

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const aof_path = try std.fs.path.join(allocator, &.{ dir_path, "ttl.aof" });
    defer allocator.free(aof_path);

    try Aof.rewrite(ctx.storage, aof_path, allocator);

    const fresh = try Storage.init(allocator, 6379, "127.0.0.1");
    defer fresh.deinit();
    _ = try Aof.replay(fresh, aof_path, allocator);

    var fresh_registry = ClientRegistry.init(allocator);
    defer fresh_registry.deinit();
    const fresh_client_id = try fresh_registry.registerClient("10457", 10, "127.0.0.1:6379");
    var fresh_ps = PubSub.init(allocator);
    defer fresh_ps.deinit();

    const ttl_resp = try execCmd(allocator, fresh, &fresh_registry, fresh_client_id, &fresh_ps, &.{
        "TTL", "ttlstream",
    });
    defer allocator.free(ttl_resp);
    // Should be a positive integer close to (but not exceeding) 10000 seconds, not -1 (no ttl) or -2 (missing).
    try testing.expect(std.mem.startsWith(u8, ttl_resp, ":"));
    try testing.expect(!std.mem.eql(u8, ttl_resp, ":-1\r\n"));
    try testing.expect(!std.mem.eql(u8, ttl_resp, ":-2\r\n"));
}
