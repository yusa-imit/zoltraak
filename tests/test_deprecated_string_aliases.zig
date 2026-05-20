const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const RespValue = protocol.RespValue;
const server_mod = @import("../src/server.zig");
const Storage = @import("../src/storage/memory.zig").Storage;

// SETEX key seconds value → SET key value EX seconds

test "SETEX - basic set with expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // SETEX mykey 10 "Hello"
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETEX" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "Hello" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify value was set
    const val = try storage.get("mykey");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("Hello", val.?.string);

    // Verify TTL is set
    const ttl = try storage.ttl("mykey");
    try std.testing.expect(ttl > 0 and ttl <= 10);
}

test "SETEX - identical behavior to SET with EX" {
    const allocator = std.testing.allocator;
    const storage1 = try Storage.init(allocator);
    defer storage1.deinit();
    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    // SET key1 value1 EX 60
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "60" },
    };
    const result_set = try server_mod.dispatchCommand(allocator, storage1, null, null, null, null, null, 0, &args_set);
    defer allocator.free(result_set);

    // SETEX key1 60 value1
    const args_setex = [_]RespValue{
        RespValue{ .bulk_string = "SETEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "60" },
        RespValue{ .bulk_string = "value1" },
    };
    const result_setex = try server_mod.dispatchCommand(allocator, storage2, null, null, null, null, null, 0, &args_setex);
    defer allocator.free(result_setex);

    // Byte-for-byte identical responses
    try std.testing.expectEqualStrings(result_set, result_setex);
}

test "SETEX - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETEX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "10" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

test "SETEX - invalid expiry value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETEX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "notanumber" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

// PSETEX key milliseconds value → SET key value PX milliseconds

test "PSETEX - basic set with millisecond expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // PSETEX mykey 5000 "World"
    const args = [_]RespValue{
        RespValue{ .bulk_string = "PSETEX" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "5000" },
        RespValue{ .bulk_string = "World" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    const val = try storage.get("mykey");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("World", val.?.string);

    const pttl = try storage.pttl("mykey");
    try std.testing.expect(pttl > 0 and pttl <= 5000);
}

test "PSETEX - identical behavior to SET with PX" {
    const allocator = std.testing.allocator;
    const storage1 = try Storage.init(allocator);
    defer storage1.deinit();
    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    // SET key2 value2 PX 3000
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "value2" },
        RespValue{ .bulk_string = "PX" },
        RespValue{ .bulk_string = "3000" },
    };
    const result_set = try server_mod.dispatchCommand(allocator, storage1, null, null, null, null, null, 0, &args_set);
    defer allocator.free(result_set);

    // PSETEX key2 3000 value2
    const args_psetex = [_]RespValue{
        RespValue{ .bulk_string = "PSETEX" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "3000" },
        RespValue{ .bulk_string = "value2" },
    };
    const result_psetex = try server_mod.dispatchCommand(allocator, storage2, null, null, null, null, null, 0, &args_psetex);
    defer allocator.free(result_psetex);

    try std.testing.expectEqualStrings(result_set, result_psetex);
}

test "PSETEX - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PSETEX" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

// SETNX key value → SET key value NX

test "SETNX - set if not exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // SETNX newkey "first"
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "SETNX" },
        RespValue{ .bulk_string = "newkey" },
        RespValue{ .bulk_string = "first" },
    };
    const result1 = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args1);
    defer allocator.free(result1);

    try std.testing.expectEqualStrings(":1\r\n", result1);

    // SETNX newkey "second" (should fail)
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "SETNX" },
        RespValue{ .bulk_string = "newkey" },
        RespValue{ .bulk_string = "second" },
    };
    const result2 = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args2);
    defer allocator.free(result2);

    try std.testing.expectEqualStrings(":0\r\n", result2);

    // Verify value unchanged
    const val = try storage.get("newkey");
    try std.testing.expectEqualStrings("first", val.?.string);
}

test "SETNX - identical behavior to SET with NX" {
    const allocator = std.testing.allocator;
    const storage1 = try Storage.init(allocator);
    defer storage1.deinit();
    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    // SET key3 value3 NX
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key3" },
        RespValue{ .bulk_string = "value3" },
        RespValue{ .bulk_string = "NX" },
    };
    const result_set = try server_mod.dispatchCommand(allocator, storage1, null, null, null, null, null, 0, &args_set);
    defer allocator.free(result_set);

    // SETNX key3 value3
    const args_setnx = [_]RespValue{
        RespValue{ .bulk_string = "SETNX" },
        RespValue{ .bulk_string = "key3" },
        RespValue{ .bulk_string = "value3" },
    };
    const result_setnx = try server_mod.dispatchCommand(allocator, storage2, null, null, null, null, null, 0, &args_setnx);
    defer allocator.free(result_setnx);

    try std.testing.expectEqualStrings(result_set, result_setnx);
}

test "SETNX - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETNX" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

// GETSET key value → SET key value GET

test "GETSET - get old value and set new" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set initial value
    try storage.set("counter", "100", null);

    // GETSET counter "200"
    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "200" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$3\r\n100\r\n", result);

    // Verify new value
    const val = try storage.get("counter");
    try std.testing.expectEqualStrings("200", val.?.string);
}

test "GETSET - returns nil for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // GETSET nonexistent "value"
    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);

    // Verify value was set
    const val = try storage.get("nonexistent");
    try std.testing.expectEqualStrings("value", val.?.string);
}

test "GETSET - identical behavior to SET with GET" {
    const allocator = std.testing.allocator;
    const storage1 = try Storage.init(allocator);
    defer storage1.deinit();
    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    try storage1.set("key4", "old1", null);
    try storage2.set("key4", "old2", null);

    // SET key4 new1 GET
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key4" },
        RespValue{ .bulk_string = "new1" },
        RespValue{ .bulk_string = "GET" },
    };
    const result_set = try server_mod.dispatchCommand(allocator, storage1, null, null, null, null, null, 0, &args_set);
    defer allocator.free(result_set);

    // GETSET key4 new2
    const args_getset = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "key4" },
        RespValue{ .bulk_string = "new2" },
    };
    const result_getset = try server_mod.dispatchCommand(allocator, storage2, null, null, null, null, null, 0, &args_getset);
    defer allocator.free(result_getset);

    // Both should return bulk string format (not exact match due to different old values)
    try std.testing.expect(std.mem.startsWith(u8, result_set, "$"));
    try std.testing.expect(std.mem.startsWith(u8, result_getset, "$"));
}

test "GETSET - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

test "GETSET - WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a list
    try storage.lpush("mylist", &[_][]const u8{"element"});

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}
