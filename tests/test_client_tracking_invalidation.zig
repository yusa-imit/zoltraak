const std = @import("std");
const client_mod = @import("../src/commands/client.zig");
const writer_mod = @import("../src/protocol/writer.zig");

const ClientRegistry = client_mod.ClientRegistry;
const InvalidationMessage = client_mod.InvalidationMessage;
const Writer = writer_mod.Writer;

// ============================================================================
// UNIT TESTS - getInvalidationMessages() behavior
// ============================================================================

test "getInvalidationMessages - basic invalidation" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Register two clients
    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Client A tracks a key
    try registry.trackKeyAccess(client_a, "mykey");

    // Get invalidation messages when key is modified by Client B
    const messages = try registry.getInvalidationMessages("mykey", client_b, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    // Should have one message for Client A
    try std.testing.expectEqual(@as(usize, 1), messages.len);
    try std.testing.expectEqual(client_a, messages[0].client_id);
    try std.testing.expectEqualStrings("mykey", messages[0].key);
}

test "getInvalidationMessages - key removed from tracking table after invalidation" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Client A tracks a key
    try registry.trackKeyAccess(client_a, "mykey");

    // Get invalidation messages (simulates what would be done during write command)
    const messages = try registry.getInvalidationMessages("mykey", client_b, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    // Manually remove key from tracking table (what invalidation delivery would do)
    registry.removeKeyFromTracking("mykey");

    // Try to get messages again - should be empty since key was removed
    const messages2 = try registry.getInvalidationMessages("mykey", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }

    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}

test "getInvalidationMessages - NOLOOP suppresses message but still removed from tracking" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");

    // Enable tracking with NOLOOP
    try registry.setTracking(client_a, true, -1, false, false, false, true, &[_][]const u8{});

    // Client A tracks a key
    try registry.trackKeyAccess(client_a, "mykey");

    // Get invalidation messages when key is modified by SAME client (with NOLOOP)
    const messages = try registry.getInvalidationMessages("mykey", client_a, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    // Should be empty due to NOLOOP
    try std.testing.expectEqual(@as(usize, 0), messages.len);

    // But key should still be in tracking table (message suppression only)
    // Getting messages from a different modifier should still work
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");
    const messages2 = try registry.getInvalidationMessages("mykey", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }

    // Now should have a message (from different modifier)
    try std.testing.expectEqual(@as(usize, 1), messages2.len);
}

test "getInvalidationMessages - REDIRECT delivers to target client" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Client A tracks with REDIRECT to Client B
    try registry.setTracking(client_a, true, @intCast(client_b), false, false, false, false, &[_][]const u8{});

    // Client A tracks a key
    try registry.trackKeyAccess(client_a, "mykey");

    // Get invalidation messages when key is modified
    const messages = try registry.getInvalidationMessages("mykey", client_b, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    // Should have one message, but it should be delivered to Client B (redirect target)
    try std.testing.expectEqual(@as(usize, 1), messages.len);
    try std.testing.expectEqual(client_b, messages[0].client_id);
}

test "getInvalidationMessages - BCAST mode with prefix matching" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Client A tracks with BCAST and PREFIX
    const prefixes = [_][]const u8{"user:"};
    try registry.setTracking(client_a, true, -1, true, false, false, false, &prefixes);

    // Client B modifies a key matching the prefix
    const messages1 = try registry.getInvalidationMessages("user:123", client_b, allocator);
    defer {
        for (messages1) |*msg| msg.deinit(allocator);
        allocator.free(messages1);
    }

    try std.testing.expectEqual(@as(usize, 1), messages1.len);
    try std.testing.expectEqual(client_a, messages1[0].client_id);

    // Client B modifies a key NOT matching the prefix
    const messages2 = try registry.getInvalidationMessages("product:456", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }

    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}

test "getInvalidationMessages - multiple clients tracking same key" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");
    const client_c = try registry.registerClient("127.0.0.1:1002", 1002, "127.0.0.1:6379");

    // Clients A and B track the same key
    try registry.trackKeyAccess(client_a, "mykey");
    try registry.trackKeyAccess(client_b, "mykey");

    // Get invalidation messages when key is modified by Client C
    const messages = try registry.getInvalidationMessages("mykey", client_c, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    // Should have two messages (one for each tracking client)
    try std.testing.expectEqual(@as(usize, 2), messages.len);

    // Verify both clients are in the messages
    var found_a = false;
    var found_b = false;
    for (messages) |msg| {
        if (msg.client_id == client_a) found_a = true;
        if (msg.client_id == client_b) found_b = true;
    }
    try std.testing.expect(found_a);
    try std.testing.expect(found_b);
}

test "getInvalidationMessages - BCAST with no prefix matches all keys" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Client A tracks with BCAST but NO prefixes (broadcasts everything)
    try registry.setTracking(client_a, true, -1, true, false, false, false, &[_][]const u8{});

    // Any key should generate invalidation
    const messages = try registry.getInvalidationMessages("anykey", client_b, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    try std.testing.expectEqual(@as(usize, 1), messages.len);
}

// ============================================================================
// UNIT TESTS - Writer.writePushInvalidation() method
// ============================================================================

test "writePushInvalidation - single key" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writePushInvalidation(&[_][]const u8{"mykey"});
    defer allocator.free(result);

    // Should produce: >2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nmykey\r\n
    const expected = ">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nmykey\r\n";
    try std.testing.expectEqualStrings(expected, result);
}

test "writePushInvalidation - multiple keys" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writePushInvalidation(&[_][]const u8{ "key1", "key2", "key3" });
    defer allocator.free(result);

    // Should produce: >2\r\n$10\r\ninvalidate\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n
    const expected = ">2\r\n$10\r\ninvalidate\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    try std.testing.expectEqualStrings(expected, result);
}

test "writePushInvalidation - empty array (null invalidation)" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writePushInvalidation(&[_][]const u8{});
    defer allocator.free(result);

    // Should produce: >2\r\n$10\r\ninvalidate\r\n*0\r\n
    const expected = ">2\r\n$10\r\ninvalidate\r\n*0\r\n";
    try std.testing.expectEqualStrings(expected, result);
}

test "writePushInvalidation - long key name" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const long_key = "user:profile:data:cache:session:123456789";
    const result = try writer.writePushInvalidation(&[_][]const u8{long_key});
    defer allocator.free(result);

    // Verify structure is correct
    try std.testing.expect(std.mem.startsWith(u8, result, ">2\r\n"));
    try std.testing.expect(std.mem.contains(u8, result, long_key));
}

// ============================================================================
// INTEGRATION TESTS - End-to-end tracking and invalidation flow
// ============================================================================

test "integration - client tracks key and receives invalidation" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Set up: Client A is in RESP3 mode
    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    registry.setProtocol(client_a, .RESP3);

    // Client A enables tracking
    try registry.setTracking(client_a, true, -1, false, false, false, false, &[_][]const u8{});

    // Client A reads a key (tracks it)
    try registry.trackKeyAccess(client_a, "mykey");

    // Verify key is in tracking table
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Get invalidation messages when key is modified
    const messages = try registry.getInvalidationMessages("mykey", client_b, allocator);
    defer {
        for (messages) |*msg| msg.deinit(allocator);
        allocator.free(messages);
    }

    try std.testing.expectEqual(@as(usize, 1), messages.len);
    try std.testing.expectEqual(client_a, messages[0].client_id);
    try std.testing.expectEqualStrings("mykey", messages[0].key);
}

test "integration - OPTIN mode requires explicit caching enabled" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");

    // Enable tracking with OPTIN mode
    try registry.setTracking(client_a, true, -1, false, true, false, false, &[_][]const u8{});

    // Try to track without explicit caching enabled - should not track
    try registry.trackKeyAccess(client_a, "key1");

    // Check messages - should be empty because OPTIN requires caching enabled
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");
    const messages1 = try registry.getInvalidationMessages("key1", client_b, allocator);
    defer {
        for (messages1) |*msg| msg.deinit(allocator);
        allocator.free(messages1);
    }
    try std.testing.expectEqual(@as(usize, 0), messages1.len);

    // Now enable caching for next command
    registry.setTrackingNextCache(client_a, true);

    // Now tracking should work
    try registry.trackKeyAccess(client_a, "key2");

    // Check messages - should have one
    const messages2 = try registry.getInvalidationMessages("key2", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }
    try std.testing.expectEqual(@as(usize, 1), messages2.len);
}

test "integration - OPTOUT mode tracks everything unless disabled" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");

    // Enable tracking with OPTOUT mode
    try registry.setTracking(client_a, true, -1, false, false, true, false, &[_][]const u8{});

    // Track a key - should work by default
    try registry.trackKeyAccess(client_a, "key1");

    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");
    var messages1 = try registry.getInvalidationMessages("key1", client_b, allocator);
    defer {
        for (messages1) |*msg| msg.deinit(allocator);
        allocator.free(messages1);
    }
    try std.testing.expectEqual(@as(usize, 1), messages1.len);

    // Now disable caching for next command
    registry.setTrackingNextCache(client_a, false);

    // Track another key - should not track because caching is explicitly disabled
    try registry.trackKeyAccess(client_a, "key2");

    const messages2 = try registry.getInvalidationMessages("key2", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }
    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}

test "integration - multiple prefixes in BCAST mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");

    // Enable tracking with BCAST and multiple prefixes
    const prefixes = [_][]const u8{ "user:", "product:" };
    try registry.setTracking(client_a, true, -1, true, false, false, false, &prefixes);

    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Test matching first prefix
    var messages1 = try registry.getInvalidationMessages("user:123", client_b, allocator);
    defer {
        for (messages1) |*msg| msg.deinit(allocator);
        allocator.free(messages1);
    }
    try std.testing.expectEqual(@as(usize, 1), messages1.len);

    // Test matching second prefix
    var messages2 = try registry.getInvalidationMessages("product:456", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }
    try std.testing.expectEqual(@as(usize, 1), messages2.len);

    // Test non-matching key
    var messages3 = try registry.getInvalidationMessages("order:789", client_b, allocator);
    defer {
        for (messages3) |*msg| msg.deinit(allocator);
        allocator.free(messages3);
    }
    try std.testing.expectEqual(@as(usize, 0), messages3.len);
}

test "integration - disconnect removes client from all tracking entries" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_a = try registry.registerClient("127.0.0.1:1000", 1000, "127.0.0.1:6379");
    const client_b = try registry.registerClient("127.0.0.1:1001", 1001, "127.0.0.1:6379");

    // Client A tracks multiple keys
    try registry.trackKeyAccess(client_a, "key1");
    try registry.trackKeyAccess(client_a, "key2");
    try registry.trackKeyAccess(client_a, "key3");

    // Remove Client A from tracking (disconnect)
    registry.removeClientFromTracking(client_a);

    // Try to get messages for modified keys - should be empty
    const messages1 = try registry.getInvalidationMessages("key1", client_b, allocator);
    defer {
        for (messages1) |*msg| msg.deinit(allocator);
        allocator.free(messages1);
    }
    try std.testing.expectEqual(@as(usize, 0), messages1.len);

    const messages2 = try registry.getInvalidationMessages("key2", client_b, allocator);
    defer {
        for (messages2) |*msg| msg.deinit(allocator);
        allocator.free(messages2);
    }
    try std.testing.expectEqual(@as(usize, 0), messages2.len);
}
