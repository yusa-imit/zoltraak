// Iteration 361: GETDEL/GETSET/SETNX keyspace notification fix
//
// These three deprecated-but-still-supported commands were missing keyspace
// notifications. This iteration adds:
//   GETDEL  → fires "del" on __keyevent@{db}__:del when key existed
//   GETSET  → fires "set" on __keyevent@{db}__:set always
//   SETNX   → fires "set" on __keyevent@{db}__:set only when key was set (returned 1)
//
// Also fixes a pre-existing memory leak in notifyKeyspaceEvent where the
// string returned by config.getAsString("notify-keyspace-events") was
// never freed.

const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const commands = zoltraak.commands;

// ─────────────────────────────────────────────────────────────────────────────
// GETDEL tests
// ─────────────────────────────────────────────────────────────────────────────

test "GETDEL returns value and fires del notification when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.config.set("notify-keyspace-events", @as([]const u8, "KEA"));
    _ = try ps.subscribe(1, "__keyevent@0__:del");

    try storage.set("mykey", "hello", null);

    const args = [_]RespValue{
        .{ .bulk_string = "GETDEL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try commands.cmdGetdel(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
    try std.testing.expect(storage.get("mykey") == null);
    // Notification must have been delivered to subscriber 1
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(1).len);
}

test "GETDEL returns nil and fires NO notification when key does not exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.config.set("notify-keyspace-events", @as([]const u8, "KEA"));
    _ = try ps.subscribe(2, "__keyevent@0__:del");

    const args = [_]RespValue{
        .{ .bulk_string = "GETDEL" },
        .{ .bulk_string = "nonexistent" },
    };
    const result = try commands.cmdGetdel(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    // No notification when key doesn't exist
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(2).len);
}

test "GETDEL fires no notification when keyspace events are disabled" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Leave notify-keyspace-events as default (empty = disabled)
    _ = try ps.subscribe(3, "__keyevent@0__:del");
    try storage.set("mykey", "val", null);

    const args = [_]RespValue{
        .{ .bulk_string = "GETDEL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try commands.cmdGetdel(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$3\r\nval\r\n", result);
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(3).len);
}

// ─────────────────────────────────────────────────────────────────────────────
// GETSET tests
// ─────────────────────────────────────────────────────────────────────────────

test "GETSET returns old value, sets new value, fires set notification" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.config.set("notify-keyspace-events", @as([]const u8, "KEA"));
    _ = try ps.subscribe(10, "__keyevent@0__:set");

    try storage.set("counter", "100", null);

    const args = [_]RespValue{
        .{ .bulk_string = "GETSET" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "200" },
    };
    const result = try commands.cmdGetset(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$3\r\n100\r\n", result);
    try std.testing.expectEqualStrings("200", storage.get("counter").?);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(10).len);
}

test "GETSET fires set notification even when key did not exist (returns nil)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.config.set("notify-keyspace-events", @as([]const u8, "KEA"));
    _ = try ps.subscribe(11, "__keyevent@0__:set");

    const args = [_]RespValue{
        .{ .bulk_string = "GETSET" },
        .{ .bulk_string = "brand_new" },
        .{ .bulk_string = "value" },
    };
    const result = try commands.cmdGetset(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expectEqualStrings("value", storage.get("brand_new").?);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(11).len);
}

// ─────────────────────────────────────────────────────────────────────────────
// SETNX tests
// ─────────────────────────────────────────────────────────────────────────────

test "SETNX returns 1 and fires set notification when key is new" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.config.set("notify-keyspace-events", @as([]const u8, "KEA"));
    _ = try ps.subscribe(20, "__keyevent@0__:set");

    const args = [_]RespValue{
        .{ .bulk_string = "SETNX" },
        .{ .bulk_string = "lock" },
        .{ .bulk_string = "1" },
    };
    const result = try commands.cmdSetnx(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(20).len);
}

test "SETNX returns 0 and fires NO notification when key already exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.config.set("notify-keyspace-events", @as([]const u8, "KEA"));
    _ = try ps.subscribe(21, "__keyevent@0__:set");

    try storage.set("existing", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "SETNX" },
        .{ .bulk_string = "existing" },
        .{ .bulk_string = "new" },
    };
    const result = try commands.cmdSetnx(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
    try std.testing.expectEqualStrings("value", storage.get("existing").?);
    // No notification — key was NOT set
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(21).len);
}

test "SETNX correct idempotent behavior: 1 then 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "SETNX" },
        .{ .bulk_string = "mylock" },
        .{ .bulk_string = "token" },
    };

    const r1 = try commands.cmdSetnx(allocator, storage, &args, &ps, 0);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    const r2 = try commands.cmdSetnx(allocator, storage, &args, &ps, 0);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":0\r\n", r2);
}
