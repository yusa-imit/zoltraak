const std = @import("std");
const client_mod = @import("zoltraak");

const ClientRegistry = client_mod.ClientRegistry;
const ClientInfo = client_mod.ClientInfo;

test "tracking table - basic key tracking" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Register two clients
    const client1_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");
    const client2_id = try registry.registerClient("127.0.0.1:2000", 20, "127.0.0.1:6379");

    // Enable tracking for both clients
    try registry.setTracking(client1_id, true, -1, false, false, false, false, &[_][]const u8{});
    try registry.setTracking(client2_id, true, -1, false, false, false, false, &[_][]const u8{});

    // Both clients access key "mykey"
    try registry.trackKeyAccess(client1_id, "mykey");
    try registry.trackKeyAccess(client2_id, "mykey");

    // Modify "mykey" as client1, get invalidation messages
    const messages = try registry.getInvalidationMessages("mykey", client1_id, allocator);
    defer {
        for (messages) |*msg| {
            var m = msg.*;
            m.deinit(allocator);
        }
        allocator.free(messages);
    }

    // Both clients should receive invalidation (NOLOOP not set)
    try std.testing.expectEqual(@as(usize, 2), messages.len);

    // Verify message contents
    var found_client1 = false;
    var found_client2 = false;
    for (messages) |msg| {
        try std.testing.expectEqualStrings("mykey", msg.key);
        if (msg.client_id == client1_id) found_client1 = true;
        if (msg.client_id == client2_id) found_client2 = true;
    }
    try std.testing.expect(found_client1);
    try std.testing.expect(found_client2);
}

test "tracking table - NOLOOP mode" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");
    const client2_id = try registry.registerClient("127.0.0.1:2000", 20, "127.0.0.1:6379");

    // Client1 enables tracking with NOLOOP
    try registry.setTracking(client1_id, true, -1, false, false, false, true, &[_][]const u8{});
    // Client2 enables tracking without NOLOOP
    try registry.setTracking(client2_id, true, -1, false, false, false, false, &[_][]const u8{});

    try registry.trackKeyAccess(client1_id, "mykey");
    try registry.trackKeyAccess(client2_id, "mykey");

    // Client1 modifies the key
    const messages = try registry.getInvalidationMessages("mykey", client1_id, allocator);
    defer {
        for (messages) |*msg| {
            var m = msg.*;
            m.deinit(allocator);
        }
        allocator.free(messages);
    }

    // Only client2 should receive invalidation (client1 has NOLOOP)
    try std.testing.expectEqual(@as(usize, 1), messages.len);
    try std.testing.expectEqual(client2_id, messages[0].client_id);
}

test "tracking table - REDIRECT mode" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");
    const client2_id = try registry.registerClient("127.0.0.1:2000", 20, "127.0.0.1:6379");
    const client3_id = try registry.registerClient("127.0.0.1:3000", 30, "127.0.0.1:6379");

    // Client1 redirects to client3
    try registry.setTracking(client1_id, true, @intCast(client3_id), false, false, false, false, &[_][]const u8{});
    // Client2 no redirect
    try registry.setTracking(client2_id, true, -1, false, false, false, false, &[_][]const u8{});

    try registry.trackKeyAccess(client1_id, "mykey");
    try registry.trackKeyAccess(client2_id, "mykey");

    const messages = try registry.getInvalidationMessages("mykey", client3_id, allocator);
    defer {
        for (messages) |*msg| {
            var m = msg.*;
            m.deinit(allocator);
        }
        allocator.free(messages);
    }

    // Client3 should receive invalidation for client1 (redirect)
    // Client2 should receive invalidation directly
    try std.testing.expectEqual(@as(usize, 2), messages.len);

    var found_redirect = false;
    var found_direct = false;
    for (messages) |msg| {
        if (msg.client_id == client3_id) found_redirect = true;
        if (msg.client_id == client2_id) found_direct = true;
    }
    try std.testing.expect(found_redirect);
    try std.testing.expect(found_direct);
}

test "tracking table - BCAST mode with PREFIX" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");

    // Enable BCAST with prefix "user:"
    const prefixes = [_][]const u8{"user:"};
    try registry.setTracking(client1_id, true, -1, true, false, false, false, &prefixes);

    // Track access to matching key
    try registry.trackKeyAccess(client1_id, "user:123");
    try registry.trackKeyAccess(client1_id, "post:456"); // Should not be tracked

    // Modify matching key
    const messages1 = try registry.getInvalidationMessages("user:123", client1_id, allocator);
    defer {
        for (messages1) |*msg| {
            var m = msg.*;
            m.deinit(allocator);
        }
        allocator.free(messages1);
    }
    try std.testing.expectEqual(@as(usize, 1), messages1.len);

    // Modify non-matching key - no invalidation
    const messages2 = try registry.getInvalidationMessages("post:456", client1_id, allocator);
    defer allocator.free(messages2);
    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}

test "tracking table - OPTIN mode" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");

    // Enable OPTIN mode
    try registry.setTracking(client_id, true, -1, false, true, false, false, &[_][]const u8{});

    // Access without explicit caching - should not track
    try registry.trackKeyAccess(client_id, "key1");

    // Enable caching for next command
    registry.setTrackingNextCache(client_id, true);
    try registry.trackKeyAccess(client_id, "key2");

    // Verify key1 not tracked, key2 tracked
    const messages1 = try registry.getInvalidationMessages("key1", client_id, allocator);
    defer allocator.free(messages1);
    try std.testing.expectEqual(@as(usize, 0), messages1.len);

    const messages2 = try registry.getInvalidationMessages("key2", client_id, allocator);
    defer {
        for (messages2) |*msg| {
            var m = msg.*;
            m.deinit(allocator);
        }
        allocator.free(messages2);
    }
    try std.testing.expectEqual(@as(usize, 1), messages2.len);
}

test "tracking table - OPTOUT mode" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");

    // Enable OPTOUT mode
    try registry.setTracking(client_id, true, -1, false, false, true, false, &[_][]const u8{});

    // Access with default - should track
    try registry.trackKeyAccess(client_id, "key1");

    // Disable caching for next command
    registry.setTrackingNextCache(client_id, false);
    try registry.trackKeyAccess(client_id, "key2");

    // Verify key1 tracked, key2 not tracked
    const messages1 = try registry.getInvalidationMessages("key1", client_id, allocator);
    defer {
        for (messages1) |*msg| {
            var m = msg.*;
            m.deinit(allocator);
        }
        allocator.free(messages1);
    }
    try std.testing.expectEqual(@as(usize, 1), messages1.len);

    const messages2 = try registry.getInvalidationMessages("key2", client_id, allocator);
    defer allocator.free(messages2);
    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}

test "tracking table - removeKeyFromTracking" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");
    try registry.setTracking(client_id, true, -1, false, false, false, false, &[_][]const u8{});

    try registry.trackKeyAccess(client_id, "mykey");

    // Remove key from tracking
    registry.removeKeyFromTracking("mykey");

    // Should no longer receive invalidations
    const messages = try registry.getInvalidationMessages("mykey", client_id, allocator);
    defer allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 0), messages.len);
}

test "tracking table - removeClientFromTracking" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");
    try registry.setTracking(client_id, true, -1, false, false, false, false, &[_][]const u8{});

    try registry.trackKeyAccess(client_id, "key1");
    try registry.trackKeyAccess(client_id, "key2");

    // Remove client from all tracking
    registry.removeClientFromTracking(client_id);

    // Should no longer receive invalidations
    const messages1 = try registry.getInvalidationMessages("key1", client_id, allocator);
    defer allocator.free(messages1);
    try std.testing.expectEqual(@as(usize, 0), messages1.len);

    const messages2 = try registry.getInvalidationMessages("key2", client_id, allocator);
    defer allocator.free(messages2);
    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}

test "tracking table - max size enforcement" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Set very small limit for testing
    registry.tracking_table_max_keys = 2;

    const client_id = try registry.registerClient("127.0.0.1:1000", 10, "127.0.0.1:6379");
    try registry.setTracking(client_id, true, -1, false, false, false, false, &[_][]const u8{});

    // Track 3 keys (exceeds limit of 2)
    try registry.trackKeyAccess(client_id, "key1");
    try registry.trackKeyAccess(client_id, "key2");
    try registry.trackKeyAccess(client_id, "key3"); // Should evict one

    // Table should have at most 2 entries
    try std.testing.expect(registry.tracking_table.count() <= 2);
}
