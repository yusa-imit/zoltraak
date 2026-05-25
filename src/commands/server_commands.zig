const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const client_mod = @import("client.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const ClientRegistry = client_mod.ClientRegistry;
const RespProtocol = client_mod.RespProtocol;
const Storage = storage_mod.Storage;

/// HELLO command - Protocol negotiation for RESP2/RESP3
/// Syntax: HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns: Map of server information (RESP3 map if proto=3, RESP2 array otherwise)
///
/// When called without arguments, returns current server info without changing protocol.
/// AUTH username password authenticates the client against the ACL store.
pub fn cmdHello(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Default: preserve current protocol version (HELLO with no args)
    const current_protocol = client_registry.getProtocol(client_id);
    var protocol_version: u8 = switch (current_protocol) {
        .RESP3 => 3,
        else => 2,
    };
    var client_name: ?[]const u8 = null;

    if (args.len > 0) {
        const proto_str = switch (args[0]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR Protocol version is not an integer or out of range"),
        };

        protocol_version = std.fmt.parseInt(u8, proto_str, 10) catch {
            return w.writeError("ERR Protocol version is not an integer or out of range");
        };

        if (protocol_version != 2 and protocol_version != 3) {
            return w.writeError("NOPROTO unsupported protocol version");
        }
    }

    // Parse optional AUTH and SETNAME options (starting at index 1)
    var i: usize = 1;
    while (i < args.len) {
        const option = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(option, "SETNAME")) {
            if (i + 1 >= args.len) return w.writeError("ERR syntax error");
            client_name = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            i += 2;
        } else if (std.ascii.eqlIgnoreCase(option, "AUTH")) {
            // AUTH requires exactly 2 args: username and password
            // Redis 6.0+ syntax: AUTH username password
            if (i + 2 >= args.len) {
                return w.writeError("ERR wrong number of arguments for 'auth' command");
            }
            const username = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            const password = switch (args[i + 2]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };

            // Validate credentials against ACL store
            const auth_err = authenticateHello(storage, client_registry, client_id, username, password);
            if (auth_err) |_| {
                // Success — client is now authenticated
            } else |err| switch (err) {
                error.WrongPass => return w.writeError("WRONGPASS invalid username-password pair or user is disabled."),
                error.AclNotInitialized => return w.writeError("ERR ACL not initialized"),
                else => return w.writeError("ERR authentication error"),
            }
            i += 3;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Update protocol version in client registry
    const resp_protocol: RespProtocol = if (protocol_version == 3) .RESP3 else .RESP2;
    client_registry.setProtocol(client_id, resp_protocol);

    // Update client name if provided
    if (client_name) |name| {
        try client_registry.setClientName(client_id, name);
    }

    // Build response based on negotiated protocol
    var response_buf = std.ArrayList(u8){ .items = &.{}, .capacity = 0 };
    errdefer response_buf.deinit(allocator);

    if (protocol_version == 3) {
        // RESP3 map type — use bulk strings for all keys and values (spec-compliant)
        try response_buf.appendSlice(allocator, "%7\r\n");
        try response_buf.appendSlice(allocator, "$6\r\nserver\r\n$8\r\nzoltraak\r\n");
        try response_buf.appendSlice(allocator, "$7\r\nversion\r\n$5\r\n0.1.0\r\n");
        try response_buf.appendSlice(allocator, "$5\r\nproto\r\n:3\r\n");

        var id_buf = std.ArrayList(u8){ .items = &.{}, .capacity = 0 };
        defer id_buf.deinit(allocator);
        try id_buf.writer(allocator).print("$2\r\nid\r\n:{d}\r\n", .{client_id});
        try response_buf.appendSlice(allocator, id_buf.items);

        try response_buf.appendSlice(allocator, "$4\r\nmode\r\n$10\r\nstandalone\r\n");
        try response_buf.appendSlice(allocator, "$4\r\nrole\r\n$6\r\nmaster\r\n");
        try response_buf.appendSlice(allocator, "$7\r\nmodules\r\n*0\r\n");
    } else {
        // RESP2 flat array (key-value pairs alternating)
        try response_buf.appendSlice(allocator, "*14\r\n");
        try response_buf.appendSlice(allocator, "$6\r\nserver\r\n$8\r\nzoltraak\r\n");
        try response_buf.appendSlice(allocator, "$7\r\nversion\r\n$5\r\n0.1.0\r\n");
        try response_buf.appendSlice(allocator, "$5\r\nproto\r\n:2\r\n");

        var id_buf = std.ArrayList(u8){ .items = &.{}, .capacity = 0 };
        defer id_buf.deinit(allocator);
        try id_buf.writer(allocator).print("$2\r\nid\r\n:{d}\r\n", .{client_id});
        try response_buf.appendSlice(allocator, id_buf.items);

        try response_buf.appendSlice(allocator, "$4\r\nmode\r\n$10\r\nstandalone\r\n");
        try response_buf.appendSlice(allocator, "$4\r\nrole\r\n$6\r\nmaster\r\n");
        try response_buf.appendSlice(allocator, "$7\r\nmodules\r\n*0\r\n");
    }

    return response_buf.toOwnedSlice(allocator);
}

/// Authenticate a user for HELLO AUTH option.
/// Returns void on success; errors on bad credentials.
fn authenticateHello(
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    username: []const u8,
    password: []const u8,
) !void {
    const acl_store = storage.acl orelse return error.AclNotInitialized;

    const user_opt = acl_store.getUser(username);
    if (user_opt == null) return error.WrongPass;

    const user = user_opt.?;
    if (!user.enabled) return error.WrongPass;

    if (user.password) |user_pwd| {
        if (!std.mem.eql(u8, password, user_pwd)) return error.WrongPass;
    }
    // nopass user: any password accepted

    try client_registry.setAuthenticatedUser(client_id, username);
}

// Embedded unit tests

test "HELLO command - default RESP2 with no args" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{};

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    // Should return RESP2 array (default protocol)
    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "zoltraak") != null);

    // Protocol unchanged (still RESP2)
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(client_id));
}

test "HELLO command - no args preserves RESP3 if already set" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // First upgrade to RESP3
    {
        const args3 = [_]RespValue{RespValue{ .bulk_string = "3" }};
        const r3 = try cmdHello(allocator, &registry, client_id, &storage, &args3);
        defer allocator.free(r3);
    }
    try std.testing.expectEqual(RespProtocol.RESP3, registry.getProtocol(client_id));

    // Now HELLO with no args — should return RESP3 map and keep RESP3
    const args = [_]RespValue{};
    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "%7\r\n"));
    try std.testing.expectEqual(RespProtocol.RESP3, registry.getProtocol(client_id));
}

test "HELLO command - negotiate RESP2" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nproto\r\n:2\r\n") != null);
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(client_id));
}

test "HELLO command - negotiate RESP3 uses bulk string keys" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "3" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    // RESP3 map header
    try std.testing.expect(std.mem.startsWith(u8, result, "%7\r\n"));
    // Keys are bulk strings (not simple strings)
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nserver\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nproto\r\n:3\r\n") != null);
    // No simple string keys (would be "+server")
    try std.testing.expect(std.mem.indexOf(u8, result, "+server\r\n") == null);
    try std.testing.expectEqual(RespProtocol.RESP3, registry.getProtocol(client_id));
}

test "HELLO command - with SETNAME" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "SETNAME" },
        RespValue{ .bulk_string = "my-client" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));

    const name = try registry.getClientName(client_id, allocator);
    try std.testing.expect(name != null);
    defer allocator.free(name.?);
    try std.testing.expectEqualStrings("my-client", name.?);
}

test "HELLO command - AUTH with nopass default user" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // default user has nopass — any password accepted
    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "default" },
        RespValue{ .bulk_string = "anypassword" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));

    // Verify authenticated_user was set to "default"
    const auth_user = try registry.getAuthenticatedUser(client_id, allocator);
    defer allocator.free(auth_user);
    try std.testing.expectEqualStrings("default", auth_user);
}

test "HELLO command - AUTH with explicit username and password" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const acl_store = storage.acl.?;
    try acl_store.setUser("alice", true, "secret");

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "alice" },
        RespValue{ .bulk_string = "secret" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));

    const auth_user = try registry.getAuthenticatedUser(client_id, allocator);
    defer allocator.free(auth_user);
    try std.testing.expectEqualStrings("alice", auth_user);
}

test "HELLO command - AUTH with wrong password returns WRONGPASS" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const acl_store = storage.acl.?;
    try acl_store.setUser("bob", true, "correct");

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "bob" },
        RespValue{ .bulk_string = "wrong" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGPASS"));
}

test "HELLO command - AUTH with nonexistent user returns WRONGPASS" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "nobody" },
        RespValue{ .bulk_string = "pass" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGPASS"));
}

test "HELLO command - AUTH with RESP3 and SETNAME combined" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "3" },
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "default" },
        RespValue{ .bulk_string = "anypassword" },
        RespValue{ .bulk_string = "SETNAME" },
        RespValue{ .bulk_string = "my-app" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    // RESP3 response
    try std.testing.expect(std.mem.startsWith(u8, result, "%7\r\n"));
    try std.testing.expectEqual(RespProtocol.RESP3, registry.getProtocol(client_id));

    // Client name set
    const name = try registry.getClientName(client_id, allocator);
    try std.testing.expect(name != null);
    defer allocator.free(name.?);
    try std.testing.expectEqualStrings("my-app", name.?);
}

test "HELLO command - AUTH missing args returns error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // AUTH with only 1 arg (missing password) — error
    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "default" }, // only username, no password
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
}

test "HELLO command - invalid protocol version" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "5" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-NOPROTO"));
}
