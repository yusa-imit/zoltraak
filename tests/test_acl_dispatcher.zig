const std = @import("std");
const protocol = @import("zoltraak").protocol.parser;
const Storage = @import("zoltraak").storage_mod.Storage;
const executeCommand = @import("zoltraak").strings.executeCommand;
const TxState = @import("zoltraak").strings.TxState;
const PubSub = @import("zoltraak").strings.PubSub;
const ClientRegistry = @import("zoltraak").strings.ClientRegistry;
const ScriptStore = @import("zoltraak").strings.ScriptStore;

const RespValue = protocol.RespValue;

// ── Integration Tests for ACL Dispatcher Integration ─────────────────────────

test "ACL dispatcher - default user can execute all commands by default" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Default user should be able to execute GET without AUTH
    const get_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "mykey" },
    } };

    const result = try executeCommand(
        allocator,
        storage,
        get_cmd,
        null, // aof
        &pubsub,
        1, // subscriber_id
        &tx,
        null, // repl
        6379,
        null, // replica_stream
        null, // replica_idx
        &client_registry,
        client_id,
        &script_store,
        null, // shutdown_state
        server.databases,
        server.num_databases,
    );
    defer allocator.free(result);

    // Should return nil (key doesn't exist), not NOPERM
    try std.testing.expect(std.mem.startsWith(u8, result, "$-1\r\n"));
}

test "ACL dispatcher - AUTH command always allowed" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // AUTH should work without prior authentication
    const auth_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "anypassword" },
    } };

    const result = try executeCommand(
        allocator,
        storage,
        auth_cmd,
        null,
        &pubsub,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "ACL dispatcher - PING command always allowed" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // PING should work without authentication
    const ping_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "PING" },
    } };

    const result = try executeCommand(
        allocator,
        storage,
        ping_cmd,
        null,
        &pubsub,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "ACL dispatcher - HELLO command always allowed" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // HELLO should work without authentication
    const hello_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "HELLO" },
        RespValue{ .bulk_string = "3" },
    } };

    const result = try executeCommand(
        allocator,
        storage,
        hello_cmd,
        null,
        &pubsub,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(result);

    // Should return protocol info, not NOPERM
    try std.testing.expect(!std.mem.startsWith(u8, result, "-NOPERM"));
}

test "ACL dispatcher - restricted user denied GET command" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create a restricted user with no permissions
    const acl_store = storage.acl.?;
    try acl_store.setUser("restricted", true, "password");
    var user = acl_store.getUser("restricted").?;
    user.all_commands_allowed = false; // Deny all by default

    // Authenticate as restricted user
    const auth_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "restricted" },
        RespValue{ .bulk_string = "password" },
    } };
    const auth_result = try executeCommand(
        allocator,
        storage,
        auth_cmd,
        null,
        &pubsub,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(auth_result);
    try std.testing.expectEqualStrings("+OK\r\n", auth_result);

    // Now try GET - should be denied
    const get_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "mykey" },
    } };

    const result = try executeCommand(
        allocator,
        storage,
        get_cmd,
        null,
        &pubsub,
        2,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(result);

    // Should return NOPERM error
    try std.testing.expect(std.mem.startsWith(u8, result, "-NOPERM"));
}

test "ACL dispatcher - user with +GET allowed GET command" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create user with +GET permission
    const acl_store = storage.acl.?;
    try acl_store.setUser("readuser", true, "password");
    var user = acl_store.getUser("readuser").?;
    try user.parsePermissionRules(allocator, "+GET");

    // Authenticate as readuser
    const auth_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "readuser" },
        RespValue{ .bulk_string = "password" },
    } };
    const auth_result = try executeCommand(
        allocator,
        storage,
        auth_cmd,
        null,
        &pubsub,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(auth_result);
    try std.testing.expectEqualStrings("+OK\r\n", auth_result);

    // GET should now work
    const get_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "mykey" },
    } };

    const result = try executeCommand(
        allocator,
        storage,
        get_cmd,
        null,
        &pubsub,
        2,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(result);

    // Should return nil (key doesn't exist), not NOPERM
    try std.testing.expect(std.mem.startsWith(u8, result, "$-1\r\n"));
}

test "ACL dispatcher - user with +@read allowed GET but denied SET" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10, "127.0.0.1:6379");

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create user with +@read category
    const acl_store = storage.acl.?;
    try acl_store.setUser("readonly", true, "password");
    var user = acl_store.getUser("readonly").?;
    try user.parsePermissionRules(allocator, "+@read");

    // Authenticate as readonly
    const auth_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "readonly" },
        RespValue{ .bulk_string = "password" },
    } };
    const auth_result = try executeCommand(
        allocator,
        storage,
        auth_cmd,
        null,
        &pubsub,
        1,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(auth_result);
    try std.testing.expectEqualStrings("+OK\r\n", auth_result);

    // GET should work (read category)
    const get_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "mykey" },
    } };
    const get_result = try executeCommand(
        allocator,
        storage,
        get_cmd,
        null,
        &pubsub,
        2,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(get_result);
    try std.testing.expect(std.mem.startsWith(u8, get_result, "$-1\r\n"));

    // SET should be denied (write category)
    const set_cmd = RespValue{ .array = &[_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "value" },
    } };
    const set_result = try executeCommand(
        allocator,
        storage,
        set_cmd,
        null,
        &pubsub,
        3,
        &tx,
        null,
        6379,
        null,
        null,
        &client_registry,
        client_id,
        &script_store,
        null,
    );
    defer allocator.free(set_result);
    try std.testing.expect(std.mem.startsWith(u8, set_result, "-NOPERM"));
}
