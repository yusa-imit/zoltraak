const std = @import("std");
const writer_mod = @import("../protocol/writer.zig");
const Writer = writer_mod.Writer;
const protocol = @import("../protocol/parser.zig");
const RespValue = protocol.RespValue;
const Storage = @import("../storage/memory.zig").Storage;
const ACLStore = @import("../storage/acl.zig").ACLStore;
const ClientRegistry = @import("./client.zig").ClientRegistry;

/// AUTH command - authenticate to the server
/// Syntax: AUTH password (Redis <6.0 legacy)
///         AUTH username password (Redis 6.0+)
pub fn cmdAuth(
    allocator: std.mem.Allocator,
    array: []const RespValue,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse arguments
    if (array.len < 2 or array.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'auth' command");
    }

    const username: []const u8 = if (array.len == 3) blk: {
        // AUTH username password
        break :blk switch (array[1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid username"),
        };
    } else "default"; // Legacy AUTH password -> default user

    const password: []const u8 = switch (array[array.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid password"),
    };

    // Get ACL store
    const acl_store = storage.acl orelse {
        return w.writeError("ERR ACL not initialized");
    };

    // Get user
    const user_opt = acl_store.getUser(username);
    if (user_opt == null) {
        return w.writeError("WRONGPASS invalid username-password pair or user is disabled.");
    }

    const user = user_opt.?;

    // Check if user is enabled
    if (!user.enabled) {
        return w.writeError("WRONGPASS invalid username-password pair or user is disabled.");
    }

    // Check password
    if (user.password) |user_pwd| {
        // User has a password - must match
        if (!std.mem.eql(u8, password, user_pwd)) {
            return w.writeError("WRONGPASS invalid username-password pair or user is disabled.");
        }
    } else {
        // User has nopass - any password accepted (legacy behavior)
        // Note: For strict Redis 6.0+ compatibility, should reject if password provided
        // But for now, accepting any password for nopass user (default behavior)
    }

    // Authentication successful - update client's authenticated user
    try client_registry.setAuthenticatedUser(client_id, username);

    return w.writeSimpleString("OK");
}

// ── Unit Tests ────────────────────────────────────────────────────────────────

test "AUTH - successful authentication with default user nopass" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "anypassword" }, // default user has nopass
    };

    const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify authenticated_user was set
    const auth_user = try client_registry.getAuthenticatedUser(client_id, allocator);
    defer allocator.free(auth_user);
    try std.testing.expectEqualStrings("default", auth_user);
}

test "AUTH - successful authentication with username and password" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    // Create a test user with password
    const acl_store = storage.acl.?;
    try acl_store.setUser("testuser", true, "secret123");

    const args = [_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "testuser" },
        RespValue{ .bulk_string = "secret123" },
    };

    const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify authenticated_user was set
    const auth_user = try client_registry.getAuthenticatedUser(client_id, allocator);
    defer allocator.free(auth_user);
    try std.testing.expectEqualStrings("testuser", auth_user);
}

test "AUTH - wrong password" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    // Create a test user with password
    const acl_store = storage.acl.?;
    try acl_store.setUser("testuser", true, "secret123");

    const args = [_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "testuser" },
        RespValue{ .bulk_string = "wrongpassword" },
    };

    const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGPASS"));
}

test "AUTH - non-existent user" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "password" },
    };

    const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGPASS"));
}

test "AUTH - disabled user" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    // Create a disabled user
    const acl_store = storage.acl.?;
    try acl_store.setUser("disableduser", false, "password");

    const args = [_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "disableduser" },
        RespValue{ .bulk_string = "password" },
    };

    const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGPASS"));
}

test "AUTH - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    // No arguments
    {
        const args = [_]RespValue{
            RespValue{ .bulk_string = "AUTH" },
        };
        const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
    }

    // Too many arguments
    {
        const args = [_]RespValue{
            RespValue{ .bulk_string = "AUTH" },
            RespValue{ .bulk_string = "user" },
            RespValue{ .bulk_string = "pass" },
            RespValue{ .bulk_string = "extra" },
        };
        const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
    }
}

test "AUTH - legacy single argument form with default user" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const client_id = try client_registry.registerClient("127.0.0.1:54321", 10);

    // Legacy AUTH password (should work with default user's nopass)
    const args = [_]RespValue{
        RespValue{ .bulk_string = "AUTH" },
        RespValue{ .bulk_string = "anypassword" },
    };

    const result = try cmdAuth(allocator, &args, storage, &client_registry, client_id);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}
