// Iteration 351: ACL LOG real violation tracking
//
// ACL LOG now returns actual NOPERM/NOAUTH violations from a ring buffer in ACLStore.
// Violations are logged when commands are denied due to missing permissions.
// ACL LOG RESET clears the buffer; ACL LOG <count> limits results (newest-first).
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
const ACLStorage = zoltraak.acl_storage;

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
    for (args, 0..) |a, i| {
        resp_args[i] = .{ .bulk_string = a };
    }
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

test "iter351 - ACL LOG empty when no violations" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(result);

    try testing.expectEqualStrings("*0\r\n", result);
}

test "iter351 - ACL LOG records NOPERM command violation" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    // admin_id is used for setup and log queries (default user)
    const admin_id = try registry.registerClient("127.0.0.1:1234", 10);
    // restricted_id is used for denied commands
    const restricted_id = try registry.registerClient("127.0.0.1:1235", 11);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Create restricted user that can only do GET
    const setuser = try execCmd(allocator, storage, &registry, admin_id, &ps, &.{
        "ACL", "SETUSER", "restricted", "on", "nopass", "-@all", "+get", "~*",
    });
    defer allocator.free(setuser);
    try testing.expectEqualStrings("+OK\r\n", setuser);

    // Authenticate restricted_id as "restricted"
    try registry.setAuthenticatedUser(restricted_id, "restricted");

    // Try to run SET as "restricted" — should be denied and logged
    const denied = try execCmd(allocator, storage, &registry, restricted_id, &ps, &.{ "SET", "mykey", "val" });
    defer allocator.free(denied);
    try testing.expect(std.mem.indexOf(u8, denied, "NOPERM") != null);

    // Query ACL LOG as admin (default user) — should have 1 entry
    const log = try execCmd(allocator, storage, &registry, admin_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log);

    try testing.expect(std.mem.startsWith(u8, log, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, log, "reason") != null);
    try testing.expect(std.mem.indexOf(u8, log, "command") != null);
    try testing.expect(std.mem.indexOf(u8, log, "SET") != null);
    try testing.expect(std.mem.indexOf(u8, log, "restricted") != null);
    try testing.expect(std.mem.indexOf(u8, log, "entry-id") != null);
}

test "iter351 - ACL LOG RESET clears violations" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:5678", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Add violation directly via ACL store
    const acl_store = storage.acl orelse return error.SkipZigTest;
    acl_store.addLogEntry(.command, "DEL", "user1", "127.0.0.1:5678");
    acl_store.addLogEntry(.key, "GET", "user2", "127.0.0.1:5679");

    // Verify entries exist
    const log_before = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log_before);
    try testing.expect(std.mem.startsWith(u8, log_before, "*2\r\n"));

    // Reset
    const reset = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG", "RESET" });
    defer allocator.free(reset);
    try testing.expectEqualStrings("+OK\r\n", reset);

    // Log should be empty now
    const log_after = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log_after);
    try testing.expectEqualStrings("*0\r\n", log_after);
}

test "iter351 - ACL LOG count limits results to newest" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:7777", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const acl_store = storage.acl orelse return error.SkipZigTest;
    acl_store.addLogEntry(.command, "GET", "u1", "10.0.0.1:1");
    acl_store.addLogEntry(.command, "SET", "u2", "10.0.0.2:2");
    acl_store.addLogEntry(.key, "DEL", "u3", "10.0.0.3:3");

    // ACL LOG 2 should return 2 entries (newest first)
    const log2 = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG", "2" });
    defer allocator.free(log2);
    try testing.expect(std.mem.startsWith(u8, log2, "*2\r\n"));

    // Newest entry (DEL by u3) should appear first
    const del_pos = std.mem.indexOf(u8, log2, "DEL") orelse return error.TestUnexpectedResult;
    const set_pos = std.mem.indexOf(u8, log2, "SET") orelse return error.TestUnexpectedResult;
    try testing.expect(del_pos < set_pos);
}

test "iter351 - ACL LOG deduplicates repeated violations" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:8888", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const acl_store = storage.acl orelse return error.SkipZigTest;
    // Same violation 3 times in a row
    acl_store.addLogEntry(.command, "SET", "bob", "127.0.0.1:8888");
    acl_store.addLogEntry(.command, "SET", "bob", "127.0.0.1:8888");
    acl_store.addLogEntry(.command, "SET", "bob", "127.0.0.1:8888");

    // Should deduplicate: still 1 entry with count=3
    const log = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log);

    try testing.expect(std.mem.startsWith(u8, log, "*1\r\n"));
    // count should be 3 (RESP integer :3)
    try testing.expect(std.mem.indexOf(u8, log, ":3\r\n") != null);
}

test "iter351 - ACL LOG entry has 20 elements (10 key-value pairs)" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:4444", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const acl_store = storage.acl orelse return error.SkipZigTest;
    acl_store.addLogEntry(.auth, "GET", "(noauth)", "127.0.0.1:4444");

    const log = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log);

    // *1\r\n followed by *20\r\n (each entry is 20 elements)
    try testing.expect(std.mem.startsWith(u8, log, "*1\r\n"));
    try testing.expect(std.mem.indexOf(u8, log, "*20\r\n") != null);

    // All 10 field names must be present
    try testing.expect(std.mem.indexOf(u8, log, "count") != null);
    try testing.expect(std.mem.indexOf(u8, log, "reason") != null);
    try testing.expect(std.mem.indexOf(u8, log, "context") != null);
    try testing.expect(std.mem.indexOf(u8, log, "object") != null);
    try testing.expect(std.mem.indexOf(u8, log, "username") != null);
    try testing.expect(std.mem.indexOf(u8, log, "age-seconds") != null);
    try testing.expect(std.mem.indexOf(u8, log, "client-info") != null);
    try testing.expect(std.mem.indexOf(u8, log, "entry-id") != null);
    try testing.expect(std.mem.indexOf(u8, log, "timestamp-created") != null);
    try testing.expect(std.mem.indexOf(u8, log, "timestamp-last-updated") != null);
}

test "iter351 - ACL LOG reason=auth for NOAUTH violations" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:5555", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const acl_store = storage.acl orelse return error.SkipZigTest;
    acl_store.addLogEntry(.auth, "SET", "(noauth)", "127.0.0.1:5555");

    const log = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log);

    try testing.expect(std.mem.indexOf(u8, log, "auth") != null);
    try testing.expect(std.mem.indexOf(u8, log, "(noauth)") != null);
}

test "iter351 - ACL LOG reason=key for key permission violations" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:6666", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const acl_store = storage.acl orelse return error.SkipZigTest;
    acl_store.addLogEntry(.key, "GET", "limited_user", "127.0.0.1:6666");

    const log = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log);

    try testing.expect(std.mem.indexOf(u8, log, "key") != null);
    try testing.expect(std.mem.indexOf(u8, log, "limited_user") != null);
}

test "iter351 - ACL LOG multiple different violations are separate entries" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:3333", 10);

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const acl_store = storage.acl orelse return error.SkipZigTest;
    // Different commands — not deduplicated
    acl_store.addLogEntry(.command, "SET", "alice", "127.0.0.1:3333");
    acl_store.addLogEntry(.command, "DEL", "alice", "127.0.0.1:3333");

    const log = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LOG" });
    defer allocator.free(log);

    // Should be 2 separate entries since "object" differs
    try testing.expect(std.mem.startsWith(u8, log, "*2\r\n"));
}
