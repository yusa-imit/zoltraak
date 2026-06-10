const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const PubSub = zoltraak.pubsub.PubSub;
const keys_cmds = zoltraak.keys_commands;

// Iteration 345: RENAME same-key fix + sailor v2.30.0 migration
//
// Bug: RENAME key key (where src == dst) triggered a use-after-free and
// double-free in storage.rename().  Root cause: fetchRemove(newkey) freed
// the value the shallow src_value copy still pointed to; then the code
// tried to read src_entry.key_ptr.* (dangling) and free it a second time.
//
// Fix (storage layer): early-return when key == newkey, after verifying
// the key exists and is not expired.
//
// Fix (command layer): early-return in cmdRename when key == newkey,
// skipping the storage call AND the del/set keyspace notifications
// that would otherwise fire for a no-op rename.
//
// Sailor v2.30.0: new Accordion widget (no API changes in existing modules).

// ── RENAME same-key tests ─────────────────────────────────────────────────────

test "RENAME key key - returns OK without corrupting value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    var ps = zoltraak.pubsub.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdRename(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    // Must return +OK
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Key must still exist with original value
    const val = storage.get("mykey");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("hello", val.?);
}

test "RENAME nonexistent key key - returns ERR" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var ps = zoltraak.pubsub.PubSub.init(allocator);
    defer ps.deinit();

    // Key does not exist; RENAME nonexistent nonexistent should error
    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try keys_cmds.cmdRename(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expect(result[0] == '-');
}

test "RENAME key key - preserves TTL" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const expire_at = Storage.getCurrentTimestamp() + 10_000;
    try storage.set("mykey", "world", expire_at);

    var ps = zoltraak.pubsub.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdRename(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // TTL should still be set (> 0)
    const ttl = storage.getTtlMs("mykey");
    try std.testing.expect(ttl > 0);
}

test "RENAME key key - storage rename is a no-op" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k", "v", null);

    // Direct storage call: must not panic or corrupt memory
    try storage.rename("k", "k");

    // Key still intact
    const val = storage.get("k");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("v", val.?);
}

test "RENAME key key - storage rename on nonexistent key returns NoSuchKey" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = storage.rename("ghost", "ghost");
    try std.testing.expectError(error.NoSuchKey, result);
}

test "RENAME normal case still works after fix" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("src", "data", null);

    var ps = zoltraak.pubsub.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "src" },
        .{ .bulk_string = "dst" },
    };
    const result = try keys_cmds.cmdRename(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    try std.testing.expect(storage.get("src") == null);
    const val = storage.get("dst");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("data", val.?);
}

test "RENAMENX key key - returns 0 (destination exists)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k", "v", null);

    var ps = zoltraak.pubsub.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "RENAMENX" },
        .{ .bulk_string = "k" },
        .{ .bulk_string = "k" },
    };
    const result = try keys_cmds.cmdRenamenx(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    // Should return 0 because destination (same key) already exists
    try std.testing.expectEqualStrings(":0\r\n", result);

    // Key must remain intact
    const val = storage.get("k");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("v", val.?);
}
