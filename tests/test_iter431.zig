// Iteration 431: ACL SAVE / ACL LOAD — real aclfile-backed persistence
//
// ACL SAVE and ACL LOAD were stubs that unconditionally returned +OK without
// ever touching disk (`src/commands/acl.zig` doc comments literally said so).
// This iteration wires them to the `aclfile` config parameter: SAVE
// serializes every user to that file (`user <name> <on|off> <nopass|>pw>
// <keys...> <+@all|-@all> <categories...> <commands...>`), and LOAD discards
// the current in-memory ACL state and replaces it from the file — validating
// every line before mutating anything, so a malformed file aborts cleanly.
//
// These tests drive the real command dispatcher (CONFIG SET / ACL SETUSER /
// ACL SAVE / ACL LOAD / ACL GETUSER) rather than calling command handlers
// directly, per the project's documented "embedded tests can be dead code"
// pitfall — only tests routed through `commands.executeCommand` and
// registered in build.zig are guaranteed to actually run under `zig build
// test`.

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

test "iter431 - ACL SAVE errors when aclfile is not configured" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10431");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "SAVE",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "ACL file") != null);
}

test "iter431 - ACL LOAD errors when aclfile is not configured" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10432");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "LOAD",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, resp, "ACL file") != null);
}

test "iter431 - ACL SAVE writes users to aclfile, ACL LOAD round-trips them" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10433");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const aclfile_path = try std.fs.path.join(allocator, &.{ dir_path, "users.acl" });
    defer allocator.free(aclfile_path);

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CONFIG", "SET", "aclfile", aclfile_path,
    }));

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "SETUSER", "alice", "on", ">secret123", "~cache:*", "+GET",
    }));

    const save_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "SAVE",
    });
    defer allocator.free(save_resp);
    try testing.expect(std.mem.eql(u8, save_resp, "+OK\r\n"));

    const contents = try tmp_dir.dir.readFileAlloc(allocator, "users.acl", 1024 * 1024);
    defer allocator.free(contents);
    try testing.expect(std.mem.indexOf(u8, contents, "user default on nopass allkeys +@all") != null);
    try testing.expect(std.mem.indexOf(u8, contents, "user alice on >secret123 ~cache:* -@all +GET") != null);

    // Disable alice and add an in-memory-only user that never touched the file.
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "SETUSER", "alice", "off",
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "SETUSER", "bob", "on",
    }));

    const load_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "LOAD",
    });
    defer allocator.free(load_resp);
    try testing.expect(std.mem.eql(u8, load_resp, "+OK\r\n"));

    const alice_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "GETUSER", "alice",
    });
    defer allocator.free(alice_resp);
    try testing.expect(std.mem.indexOf(u8, alice_resp, "on") != null);
    try testing.expect(std.mem.indexOf(u8, alice_resp, "off") == null);

    const bob_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "GETUSER", "bob",
    });
    defer allocator.free(bob_resp);
    try testing.expect(std.mem.eql(u8, bob_resp, "$-1\r\n")); // bob wasn't in the file, LOAD dropped him
}

test "iter431 - ACL LOAD rejects a malformed ACL file without mutating existing state" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "10434");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const dir_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir_path);
    const aclfile_path = try std.fs.path.join(allocator, &.{ dir_path, "bad.acl" });
    defer allocator.free(aclfile_path);
    try tmp_dir.dir.writeFile(.{ .sub_path = "bad.acl", .data = "user broken +@not-a-real-category\n" });

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "CONFIG", "SET", "aclfile", aclfile_path,
    }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "SETUSER", "carol", "on",
    }));

    const load_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "LOAD",
    });
    defer allocator.free(load_resp);
    try testing.expect(std.mem.startsWith(u8, load_resp, "-ERR"));

    const carol_resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "ACL", "GETUSER", "carol",
    });
    defer allocator.free(carol_resp);
    try testing.expect(!std.mem.eql(u8, carol_resp, "$-1\r\n")); // carol must survive the rejected load
}
