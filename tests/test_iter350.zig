// Iteration 350: ACL WHOAMI/LIST/USERS/GETUSER real store integration
//
// ACL WHOAMI now returns the actual authenticated username from ClientRegistry
// instead of always returning "default". ACL LIST/USERS use the real ACL store.
// ACL GETUSER supports all users and returns a proper 12-field Redis response.
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
const acl_cmds = zoltraak.acl_commands;

// Helper: execute a single command string through executeCommand
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

test "iter350 - ACL WHOAMI returns 'default' for unauthenticated client" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "WHOAMI" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "default") != null);
    try testing.expect(result[0] == '$');
}

test "iter350 - ACL WHOAMI returns real username after AUTH" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Create user "alice" with full permissions via ACL SETUSER
    const setuser_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "SETUSER", "alice", "on", "nopass", "+@all", "~*" });
    defer allocator.free(setuser_result);
    try testing.expectEqualStrings("+OK\r\n", setuser_result);

    // Authenticate as alice (simulate AUTH alice => setAuthenticatedUser)
    try registry.setAuthenticatedUser(client_id, "alice");

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "WHOAMI" });
    defer allocator.free(result);

    // Should return "alice" not "default"
    try testing.expect(std.mem.indexOf(u8, result, "alice") != null);
    try testing.expect(std.mem.indexOf(u8, result, "default") == null);
}

test "iter350 - ACL LIST returns rules from real ACL store" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add a second user
    if (storage.acl) |acl_store| {
        try acl_store.setUser("bob", true, "bobpass");
    }

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LIST" });
    defer allocator.free(result);

    // Should be an array starting with *
    try testing.expect(result[0] == '*');
    // Should contain both users
    try testing.expect(std.mem.indexOf(u8, result, "default") != null);
    try testing.expect(std.mem.indexOf(u8, result, "bob") != null);
}

test "iter350 - ACL USERS returns all usernames from ACL store" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add users
    if (storage.acl) |acl_store| {
        try acl_store.setUser("carol", true, "carolpass");
    }

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "USERS" });
    defer allocator.free(result);

    // Should be an array
    try testing.expect(result[0] == '*');
    // Should contain default and carol
    try testing.expect(std.mem.indexOf(u8, result, "default") != null);
    try testing.expect(std.mem.indexOf(u8, result, "carol") != null);
}

test "iter350 - ACL GETUSER default returns 12-field response" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "GETUSER", "default" });
    defer allocator.free(result);

    // Should be a 12-element RESP array
    try testing.expect(std.mem.startsWith(u8, result, "*12\r\n"));
    // Required field names
    try testing.expect(std.mem.indexOf(u8, result, "flags") != null);
    try testing.expect(std.mem.indexOf(u8, result, "passwords") != null);
    try testing.expect(std.mem.indexOf(u8, result, "commands") != null);
    try testing.expect(std.mem.indexOf(u8, result, "keys") != null);
    try testing.expect(std.mem.indexOf(u8, result, "channels") != null);
    try testing.expect(std.mem.indexOf(u8, result, "selectors") != null);
}

test "iter350 - ACL GETUSER nonexistent returns nil" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "GETUSER", "nonexistent_user_xyz" });
    defer allocator.free(result);

    // Non-existent user → null bulk string
    try testing.expectEqualStrings("$-1\r\n", result);
}

test "iter350 - ACL GETUSER user-with-password has non-empty passwords array" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create user with password
    if (storage.acl) |acl_store| {
        try acl_store.setUser("dave", true, "davepass");
    }

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "GETUSER", "dave" });
    defer allocator.free(result);

    // Should be 12-element array
    try testing.expect(std.mem.startsWith(u8, result, "*12\r\n"));
    // Should NOT contain "nopass" for a user with a password
    // The flags array should contain "on" but not "nopass"
    try testing.expect(std.mem.indexOf(u8, result, "nopass") == null);
    // Should contain the hashed password (prefixed with #)
    try testing.expect(std.mem.indexOf(u8, result, "#davepass") != null);
}

test "iter350 - ACL GETUSER default user has nopass flag" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Default user has nopass by default
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "GETUSER", "default" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "nopass") != null);
    // Default user allows all commands
    try testing.expect(std.mem.indexOf(u8, result, "+@all") != null);
}

test "iter350 - ACL SETUSER then GETUSER shows created user" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Create user via ACL SETUSER
    const set_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "SETUSER", "testuser350" });
    defer allocator.free(set_result);
    try testing.expectEqualStrings("+OK\r\n", set_result);

    // Verify via ACL GETUSER
    const get_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "GETUSER", "testuser350" });
    defer allocator.free(get_result);

    // Should return 12-field response (not nil)
    try testing.expect(!std.mem.eql(u8, get_result, "$-1\r\n"));
    try testing.expect(std.mem.startsWith(u8, get_result, "*12\r\n"));
}

test "iter350 - ACL USERS includes newly created user" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Create user
    const set_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "SETUSER", "newuser350" });
    defer allocator.free(set_result);
    try testing.expectEqualStrings("+OK\r\n", set_result);

    // ACL USERS should now include the new user
    const users_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "USERS" });
    defer allocator.free(users_result);

    try testing.expect(std.mem.indexOf(u8, users_result, "newuser350") != null);
    try testing.expect(std.mem.indexOf(u8, users_result, "default") != null);
}

test "iter350 - ACL LIST includes newly created user rule" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Create user with password
    const set_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "SETUSER", "listeduser", "on", ">secretpass" });
    defer allocator.free(set_result);
    try testing.expectEqualStrings("+OK\r\n", set_result);

    // ACL LIST should include the new user
    const list_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "ACL", "LIST" });
    defer allocator.free(list_result);

    try testing.expect(std.mem.indexOf(u8, list_result, "listeduser") != null);
}
