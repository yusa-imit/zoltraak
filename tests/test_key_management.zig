const std = @import("std");
const testing = std.testing;
const storage_mod = @import("../src/storage/memory.zig");
const protocol = @import("../src/protocol/parser.zig");
const strings = @import("../src/commands/strings.zig");
const keys_cmds = @import("../src/commands/keys.zig");

const Storage = storage_mod.Storage;
const RespValue = protocol.RespValue;

/// Integration tests for key management commands (Iteration 21)
/// Tests TTL, EXPIRE, TYPE, KEYS, RENAME and related commands

test "TTL - returns -2 for missing key" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try keys_cmds.cmdTtl(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":-2\r\n", result);
}

test "TTL - returns -1 for key with no expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdTtl(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":-1\r\n", result);
}

test "TTL - returns seconds for key with expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_10s = Storage.getCurrentTimestamp() + 10000;
    try store.set("mykey", "value", in_10s);

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdTtl(allocator, store, &args);
    defer allocator.free(result);

    // Should return between 9 and 10 seconds
    try testing.expect(result[0] == ':');
    try testing.expect(result.len >= 3); // e.g. ":10\r\n"
}

test "PTTL - returns milliseconds for key with expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_5s = Storage.getCurrentTimestamp() + 5000;
    try store.set("mykey", "value", in_5s);

    const args = [_]RespValue{
        .{ .bulk_string = "PTTL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdPttl(allocator, store, &args);
    defer allocator.free(result);

    // Should return ~5000ms
    try testing.expect(result[0] == ':');
}

test "EXPIRE - sets expiry in seconds" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRE" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "10" },
    };
    const result = try keys_cmds.cmdExpire(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    // Verify TTL is set
    const ttl = store.getTtlMs("mykey");
    try testing.expect(ttl > 0);
    try testing.expect(ttl <= 10000);
}

test "PEXPIRE - sets expiry in milliseconds" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "PEXPIRE" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "5000" },
    };
    const result = try keys_cmds.cmdPexpire(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    // Verify TTL is set
    const ttl = store.getTtlMs("mykey");
    try testing.expect(ttl > 0);
    try testing.expect(ttl <= 5000);
}

test "EXPIRE - NX flag only sets if no expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_10s = Storage.getCurrentTimestamp() + 10000;
    try store.set("mykey", "value", in_10s);

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRE" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "NX" },
    };
    const result = try keys_cmds.cmdExpire(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":0\r\n", result);
}

test "EXPIRE - XX flag only sets if has expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRE" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "5" },
        .{ .bulk_string = "XX" },
    };
    const result = try keys_cmds.cmdExpire(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":0\r\n", result);
}

test "EXPIREAT - sets absolute timestamp in seconds" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const now_sec = @divTrunc(Storage.getCurrentTimestamp(), 1000);
    const timestamp = now_sec + 10;

    var buf: [32]u8 = undefined;
    const ts_str = try std.fmt.bufPrint(&buf, "{d}", .{timestamp});

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIREAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = ts_str },
    };
    const result = try keys_cmds.cmdExpireat(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);
}

test "PEXPIREAT - sets absolute timestamp in milliseconds" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const timestamp_ms = Storage.getCurrentTimestamp() + 10000;

    var buf: [32]u8 = undefined;
    const ts_str = try std.fmt.bufPrint(&buf, "{d}", .{timestamp_ms});

    const args = [_]RespValue{
        .{ .bulk_string = "PEXPIREAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = ts_str },
    };
    const result = try keys_cmds.cmdPexpireat(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);
}

test "PERSIST - removes expiry from key" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_10s = Storage.getCurrentTimestamp() + 10000;
    try store.set("mykey", "value", in_10s);

    const args = [_]RespValue{
        .{ .bulk_string = "PERSIST" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdPersist(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    // Verify expiry is removed
    const ttl = store.getTtlMs("mykey");
    try testing.expectEqual(@as(i64, -1), ttl);
}

test "PERSIST - returns 0 for key without expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "PERSIST" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdPersist(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":0\r\n", result);
}

test "EXPIRETIME - returns absolute timestamp in seconds" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_10s = Storage.getCurrentTimestamp() + 10000;
    try store.set("mykey", "value", in_10s);

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRETIME" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdExpiretime(allocator, store, &args);
    defer allocator.free(result);

    try testing.expect(result[0] == ':');
}

test "PEXPIRETIME - returns absolute timestamp in milliseconds" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_10s = Storage.getCurrentTimestamp() + 10000;
    try store.set("mykey", "value", in_10s);

    const args = [_]RespValue{
        .{ .bulk_string = "PEXPIRETIME" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdPexpiretime(allocator, store, &args);
    defer allocator.free(result);

    try testing.expect(result[0] == ':');
}

test "TYPE - returns correct type for string" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdType(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+string\r\n", result);
}

test "TYPE - returns correct type for list" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const elements = [_][]const u8{"val1"};
    _ = try store.lpush("mylist", &elements, null);

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "mylist" },
    };
    const result = try keys_cmds.cmdType(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+list\r\n", result);
}

test "TYPE - returns correct type for set" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const members = [_][]const u8{"member1"};
    _ = try store.sadd("myset", &members, null);

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "myset" },
    };
    const result = try keys_cmds.cmdType(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+set\r\n", result);
}

test "TYPE - returns correct type for hash" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const fields = [_][]const u8{"field1"};
    const values = [_][]const u8{"value1"};
    _ = try store.hset("myhash", &fields, &values, null);

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "myhash" },
    };
    const result = try keys_cmds.cmdType(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+hash\r\n", result);
}

test "TYPE - returns correct type for sorted set" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"member1"};
    _ = try store.zadd("myzset", &scores, &members, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "myzset" },
    };
    const result = try keys_cmds.cmdType(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+zset\r\n", result);
}

test "TYPE - returns none for missing key" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try keys_cmds.cmdType(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+none\r\n", result);
}

test "KEYS - returns all keys with * pattern" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("key1", "val1", null);
    try store.set("key2", "val2", null);
    try store.set("key3", "val3", null);

    const args = [_]RespValue{
        .{ .bulk_string = "KEYS" },
        .{ .bulk_string = "*" },
    };
    const result = try keys_cmds.cmdKeys(allocator, store, &args);
    defer allocator.free(result);

    try testing.expect(result[0] == '*');
    try testing.expect(result[1] == '3');
}

test "KEYS - filters with glob pattern" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("user:1", "alice", null);
    try store.set("user:2", "bob", null);
    try store.set("session:1", "token", null);

    const args = [_]RespValue{
        .{ .bulk_string = "KEYS" },
        .{ .bulk_string = "user:*" },
    };
    const result = try keys_cmds.cmdKeys(allocator, store, &args);
    defer allocator.free(result);

    try testing.expect(result[0] == '*');
    try testing.expect(result[1] == '2');
}

test "RENAME - renames key successfully" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("oldkey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "oldkey" },
        .{ .bulk_string = "newkey" },
    };
    const result = try keys_cmds.cmdRename(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+OK\r\n", result);

    // Verify oldkey is gone, newkey exists
    try testing.expect(store.get("oldkey") == null);
    try testing.expect(store.get("newkey") != null);
    try testing.expectEqualStrings("value", store.get("newkey").?);
}

test "RENAME - preserves expiry" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const in_10s = Storage.getCurrentTimestamp() + 10000;
    try store.set("oldkey", "value", in_10s);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "oldkey" },
        .{ .bulk_string = "newkey" },
    };
    const result = try keys_cmds.cmdRename(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+OK\r\n", result);

    // Verify expiry is preserved
    const ttl = store.getTtlMs("newkey");
    try testing.expect(ttl > 0);
}

test "RENAMENX - succeeds when newkey doesn't exist" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("oldkey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAMENX" },
        .{ .bulk_string = "oldkey" },
        .{ .bulk_string = "newkey" },
    };
    const result = try keys_cmds.cmdRenamenx(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);
}

test "RENAMENX - fails when newkey exists" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("oldkey", "value1", null);
    try store.set("newkey", "value2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAMENX" },
        .{ .bulk_string = "oldkey" },
        .{ .bulk_string = "newkey" },
    };
    const result = try keys_cmds.cmdRenamenx(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":0\r\n", result);

    // Verify oldkey still exists
    try testing.expectEqualStrings("value1", store.get("oldkey").?);
    try testing.expectEqualStrings("value2", store.get("newkey").?);
}

test "RANDOMKEY - returns null for empty database" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "RANDOMKEY" },
    };
    const result = try keys_cmds.cmdRandomkey(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("$-1\r\n", result);
}

test "RANDOMKEY - returns a key from database" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("key1", "val1", null);
    try store.set("key2", "val2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RANDOMKEY" },
    };
    const result = try keys_cmds.cmdRandomkey(allocator, store, &args);
    defer allocator.free(result);

    // Should be a bulk string with a key name
    try testing.expect(result[0] == '$');
}

test "UNLINK - deletes keys" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("key1", "val1", null);
    try store.set("key2", "val2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "UNLINK" },
        .{ .bulk_string = "key1" },
        .{ .bulk_string = "key2" },
    };
    const result = try keys_cmds.cmdUnlink(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":2\r\n", result);

    // Verify keys are deleted
    try testing.expect(store.get("key1") == null);
    try testing.expect(store.get("key2") == null);
}

// ── DUMP/RESTORE/COPY/TOUCH/MOVE integration tests ──────────────────────────

test "DUMP - serializes string value" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "hello world", null);

    const args = [_]RespValue{
        .{ .bulk_string = "DUMP" },
        .{ .bulk_string = "mykey" },
    };
    const result = try keys_cmds.cmdDump(allocator, store, &args);
    defer allocator.free(result);

    // Should return a bulk string with serialized data
    try testing.expect(result[0] == '$');
    try testing.expect(result.len > 10); // Should have some data
}

test "DUMP - returns null for non-existent key" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "DUMP" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try keys_cmds.cmdDump(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("$-1\r\n", result);
}

test "RESTORE - deserializes value" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    // First dump a value
    try store.set("source", "test value", null);
    const dump = (try store.dumpValue(allocator, "source")).?;
    defer allocator.free(dump);

    // Restore to new key
    const args = [_]RespValue{
        .{ .bulk_string = "RESTORE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "0" }, // No TTL
        .{ .bulk_string = dump },
    };
    const result = try keys_cmds.cmdRestore(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+OK\r\n", result);

    // Verify restored value
    const value = store.get("dest").?;
    try testing.expectEqualStrings("test value", value);
}

test "RESTORE - with REPLACE option" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("source", "new value", null);
    try store.set("dest", "old value", null);

    const dump = (try store.dumpValue(allocator, "source")).?;
    defer allocator.free(dump);

    const args = [_]RespValue{
        .{ .bulk_string = "RESTORE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = dump },
        .{ .bulk_string = "REPLACE" },
    };
    const result = try keys_cmds.cmdRestore(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings("+OK\r\n", result);

    const value = store.get("dest").?;
    try testing.expectEqualStrings("new value", value);
}

test "RESTORE - fails without REPLACE" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("source", "value", null);
    try store.set("dest", "existing", null);

    const dump = (try store.dumpValue(allocator, "source")).?;
    defer allocator.free(dump);

    const args = [_]RespValue{
        .{ .bulk_string = "RESTORE" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = dump },
    };
    const result = try keys_cmds.cmdRestore(allocator, store, &args);
    defer allocator.free(result);
    try testing.expect(result[0] == '-'); // Error
}

test "COPY - copies key" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("source", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
    };
    const result = try keys_cmds.cmdCopy(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    // Verify both keys exist
    try testing.expectEqualStrings("value", store.get("source").?);
    try testing.expectEqualStrings("value", store.get("dest").?);
}

test "COPY - returns 0 if destination exists" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("source", "src_val", null);
    try store.set("dest", "dest_val", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
    };
    const result = try keys_cmds.cmdCopy(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":0\r\n", result);

    // Destination unchanged
    try testing.expectEqualStrings("dest_val", store.get("dest").?);
}

test "COPY - with REPLACE overwrites" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("source", "new", null);
    try store.set("dest", "old", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "REPLACE" },
    };
    const result = try keys_cmds.cmdCopy(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":1\r\n", result);

    try testing.expectEqualStrings("new", store.get("dest").?);
}

test "TOUCH - counts existing keys" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("key1", "a", null);
    try store.set("key2", "b", null);

    const args = [_]RespValue{
        .{ .bulk_string = "TOUCH" },
        .{ .bulk_string = "key1" },
        .{ .bulk_string = "key2" },
        .{ .bulk_string = "key3" }, // doesn't exist
    };
    const result = try keys_cmds.cmdTouch(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":2\r\n", result);
}

test "MOVE - always returns 0 (single DB)" {
    const allocator = testing.allocator;
    const store = try Storage.init(allocator);
    defer store.deinit();

    try store.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "MOVE" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "1" }, // DB index
    };
    const result = try keys_cmds.cmdMove(allocator, store, &args);
    defer allocator.free(result);
    try testing.expectEqualStrings(":0\r\n", result);
}
