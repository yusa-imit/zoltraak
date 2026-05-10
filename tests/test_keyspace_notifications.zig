const std = @import("std");
const testing = std.testing;
const parser = @import("zoltraak").protocol.parser;
const commands = @import("zoltraak").commands;
const memory = @import("zoltraak").storage.memory;
const Server = @import("zoltraak").Server;
const notifications_mod = @import("zoltraak").storage.notifications;

test "keyspace notifications - SET command with KEg flags" {
    const allocator = testing.allocator;

    // Start server
    var server = try Server.init(allocator, "127.0.0.1", 6389);
    defer server.deinit();

    // Enable keyspace and keyevent notifications for generic commands
    const config_cmd = "*3\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$24\r\nnotify-keyspace-events\r\n$3\r\nKEg\r\n";
    var config_result = try server.handleCommand(1, config_cmd);
    defer config_result.deinit(allocator);

    // Subscribe to keyspace channel
    const subscribe_cmd = "*2\r\n$9\r\nSUBSCRIBE\r\n$21\r\n__keyspace@0__:mykey\r\n";
    var subscribe_result = try server.handleCommand(2, subscribe_cmd);
    defer subscribe_result.deinit(allocator);

    // SET mykey value
    const set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n";
    var set_result = try server.handleCommand(1, set_cmd);
    defer set_result.deinit(allocator);

    // Subscriber 2 should receive notification
    const pending = server.pubsub.pendingMessages(2);
    try testing.expect(pending > 0);
}

test "keyspace notifications - DEL command with KE flags" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6390);
    defer server.deinit();

    // Enable keyevent notifications only
    const config_cmd = "*3\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$24\r\nnotify-keyspace-events\r\n$3\r\nKEg\r\n";
    var config_result = try server.handleCommand(1, config_cmd);
    defer config_result.deinit(allocator);

    // SET a key first
    const set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n";
    var set_result = try server.handleCommand(1, set_cmd);
    defer set_result.deinit(allocator);

    // Subscribe to keyevent channel for "del" event
    const subscribe_cmd = "*2\r\n$9\r\nSUBSCRIBE\r\n$18\r\n__keyevent@0__:del\r\n";
    var subscribe_result = try server.handleCommand(2, subscribe_cmd);
    defer subscribe_result.deinit(allocator);

    // DEL mykey
    const del_cmd = "*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
    var del_result = try server.handleCommand(1, del_cmd);
    defer del_result.deinit(allocator);

    // Subscriber 2 should receive notification with key name
    const pending = server.pubsub.pendingMessages(2);
    try testing.expect(pending > 0);
}

test "parseNotificationFlags - comprehensive" {
    try testing.expect(notifications_mod.parseNotificationFlags("KEA") != 0);
    try testing.expect(notifications_mod.parseNotificationFlags("Kg") != 0);
    try testing.expect(notifications_mod.parseNotificationFlags("") == 0);
    try testing.expect(notifications_mod.parseNotificationFlags("K$lsh") != 0);
}

test "keyspace notifications - disabled by default" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6391);
    defer server.deinit();

    // Subscribe to keyspace channel
    const subscribe_cmd = "*2\r\n$9\r\nSUBSCRIBE\r\n$21\r\n__keyspace@0__:mykey\r\n";
    var subscribe_result = try server.handleCommand(2, subscribe_cmd);
    defer subscribe_result.deinit(allocator);

    // SET mykey value (no CONFIG SET, notifications disabled)
    const set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n";
    var set_result = try server.handleCommand(1, set_cmd);
    defer set_result.deinit(allocator);

    // Subscriber 2 should NOT receive notification
    const pending = server.pubsub.pendingMessages(2);
    try testing.expect(pending == 0);
}

test "CONFIG GET notify-keyspace-events" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6392);
    defer server.deinit();

    const cmd = "*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$24\r\nnotify-keyspace-events\r\n";
    var result = try server.handleCommand(1, cmd);
    defer result.deinit(allocator);

    // Should return empty string (disabled by default)
    try testing.expect(result.items.len > 0);
}

test "CONFIG SET notify-keyspace-events - atomic flag update" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6393);
    defer server.deinit();

    // Verify flags are 0 initially (disabled by default)
    const initial_flags = server.storage.notification_flags.load(.acquire);
    try testing.expectEqual(@as(u16, 0), initial_flags);

    // Set notify-keyspace-events to "KEA" (all flags)
    const set_cmd = "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$24\r\nnotify-keyspace-events\r\n$3\r\nKEA\r\n";
    var set_result = try server.handleCommand(1, set_cmd);
    defer set_result.deinit(allocator);

    // Verify flags are now non-zero (KEA should enable all flags)
    const flags_after_set = server.storage.notification_flags.load(.acquire);
    try testing.expect(flags_after_set != 0);

    // Disable notifications by setting empty string
    const disable_cmd = "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$24\r\nnotify-keyspace-events\r\n$0\r\n\r\n";
    var disable_result = try server.handleCommand(1, disable_cmd);
    defer disable_result.deinit(allocator);

    // Verify flags are back to 0
    const flags_after_disable = server.storage.notification_flags.load(.acquire);
    try testing.expectEqual(@as(u16, 0), flags_after_disable);
}

test "CONFIG SET notify-keyspace-events - specific flags" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6394);
    defer server.deinit();

    // Set specific flags: K (keyspace), g (generic)
    const set_cmd = "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$24\r\nnotify-keyspace-events\r\n$2\r\nKg\r\n";
    var set_result = try server.handleCommand(1, set_cmd);
    defer set_result.deinit(allocator);

    // Verify flags are set
    const flags = server.storage.notification_flags.load(.acquire);
    try testing.expect(flags != 0);

    // Verify the flags match what parseNotificationFlags would return for "Kg"
    const expected_flags = notifications_mod.parseNotificationFlags("Kg");
    try testing.expectEqual(expected_flags, flags);
}
