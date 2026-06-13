// Iteration 349: NOAUTH enforcement + requirepass integration
//
// When requirepass is configured, unauthenticated clients must authenticate
// before running commands. AUTH with the requirepass value succeeds and
// marks the client as authenticated. AUTH, HELLO, PING, QUIT, RESET
// are always allowed without authentication.
const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const transactions = @import("zoltraak").commands;

const PubSub = zoltraak.pubsub.PubSub;

const scripting = zoltraak.scripting_storage;
const transactions_mod = zoltraak.transactions_commands;
const auth_mod = zoltraak.auth_commands;

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
        null, // aof
        ps,
        0, // subscriber_id
        &tx,
        null, // repl
        6379,
        null, // replica_stream
        null, // replica_idx
        client_registry,
        client_id,
        &script_store,
        null, // shutdown_state
        &databases,
        1,
    );
}

test "iter349 - isAuthenticated returns false for new client" {
    const allocator = testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");
    try testing.expect(!registry.isAuthenticated(client_id));
}

test "iter349 - isAuthenticated returns true after setAuthenticatedUser" {
    const allocator = testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");
    try registry.setAuthenticatedUser(client_id, "default");
    try testing.expect(registry.isAuthenticated(client_id));
}

test "iter349 - updateDefaultUserPassword sets password on default user" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const acl_store = storage.acl.?;
    // Default user has nopass
    {
        const user = acl_store.getUser("default").?;
        try testing.expect(user.password == null);
    }

    // Set password
    try acl_store.updateDefaultUserPassword("mysecret");
    {
        const user = acl_store.getUser("default").?;
        try testing.expectEqualStrings("mysecret", user.password.?);
    }

    // Clear password (empty string = nopass)
    try acl_store.updateDefaultUserPassword("");
    {
        const user = acl_store.getUser("default").?;
        try testing.expect(user.password == null);
    }
}

test "iter349 - updateRequirepass sets default user password in ACL" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.updateRequirepass("secretpass");
    const user = storage.acl.?.getUser("default").?;
    try testing.expectEqualStrings("secretpass", user.password.?);
}

test "iter349 - updateRequirepass with empty clears password" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.updateRequirepass("secretpass");
    storage.updateRequirepass("");
    const user = storage.acl.?.getUser("default").?;
    try testing.expect(user.password == null);
}

test "iter349 - AUTH with requirepass correct password succeeds and authenticates" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set requirepass
    storage.updateRequirepass("correctpass");

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    // Client is not yet authenticated
    try testing.expect(!registry.isAuthenticated(client_id));

    // AUTH with correct password
    const args = [_]RespValue{
        .{ .bulk_string = "AUTH" },
        .{ .bulk_string = "correctpass" },
    };
    const result = try auth_mod.cmdAuth(allocator, &args, storage, &registry, client_id);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
    try testing.expect(registry.isAuthenticated(client_id));
}

test "iter349 - AUTH with requirepass wrong password returns WRONGPASS" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.updateRequirepass("correctpass");

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    const args = [_]RespValue{
        .{ .bulk_string = "AUTH" },
        .{ .bulk_string = "wrongpass" },
    };
    const result = try auth_mod.cmdAuth(allocator, &args, storage, &registry, client_id);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-WRONGPASS"));
    try testing.expect(!registry.isAuthenticated(client_id));
}

test "iter349 - NOAUTH returned for unauthenticated client when requirepass set" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set requirepass via config
    try storage.config.set("requirepass", @as([]const u8, "mypassword"));
    storage.updateRequirepass("mypassword");

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Unauthenticated client tries GET
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GET", "somekey" });
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-NOAUTH"));
}

test "iter349 - PING allowed without auth even when requirepass set" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("requirepass", @as([]const u8, "mypassword"));
    storage.updateRequirepass("mypassword");

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // PING must work without auth
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{"PING"});
    defer allocator.free(result);

    try testing.expect(!std.mem.startsWith(u8, result, "-NOAUTH"));
    try testing.expect(std.mem.startsWith(u8, result, "+PONG"));
}

test "iter349 - no NOAUTH when requirepass is empty" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // requirepass is empty by default (no auth required)
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // GET should work without auth when no requirepass
    const result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GET", "key" });
    defer allocator.free(result);

    // Should NOT return NOAUTH
    try testing.expect(!std.mem.startsWith(u8, result, "-NOAUTH"));
}

test "iter349 - after successful AUTH, commands work normally" {
    const allocator = testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const password = "mypassword";
    try storage.config.set("requirepass", @as([]const u8, password));
    storage.updateRequirepass(password);

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();
    const client_id = try registry.registerClient("127.0.0.1:9999", 10, "127.0.0.1:6379");

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Authenticate
    const auth_args = [_]RespValue{
        .{ .bulk_string = "AUTH" },
        .{ .bulk_string = password },
    };
    const auth_result = try auth_mod.cmdAuth(allocator, &auth_args, storage, &registry, client_id);
    defer allocator.free(auth_result);
    try testing.expectEqualStrings("+OK\r\n", auth_result);

    // Now GET should work without NOAUTH
    const get_result = try execCmd(allocator, storage, &registry, client_id, &ps, &.{ "GET", "somekey" });
    defer allocator.free(get_result);
    try testing.expect(!std.mem.startsWith(u8, get_result, "-NOAUTH"));
    // Key doesn't exist → null bulk string
    try testing.expectEqualStrings("$-1\r\n", get_result);
}
