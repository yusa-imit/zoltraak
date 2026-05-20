const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const RespValue = protocol.RespValue;
const server_mod = @import("../src/server.zig");
const Storage = @import("../src/storage/memory.zig").Storage;

test "SUBSTR - basic substring extraction" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // SET mykey "This is a string"
    try storage.set("mykey", "This is a string", null);

    // SUBSTR mykey 0 3
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "3" },
    };
    const result1 = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args1);
    defer allocator.free(result1);
    try std.testing.expectEqualStrings("$4\r\nThis\r\n", result1);

    // SUBSTR mykey -3 -1
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "-3" },
        RespValue{ .bulk_string = "-1" },
    };
    const result2 = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args2);
    defer allocator.free(result2);
    try std.testing.expectEqualStrings("$3\r\ning\r\n", result2);
}

test "SUBSTR - identical behavior to GETRANGE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("testkey", "Hello World", null);

    // GETRANGE testkey 0 4
    const args_getrange = [_]RespValue{
        RespValue{ .bulk_string = "GETRANGE" },
        RespValue{ .bulk_string = "testkey" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "4" },
    };
    const result_getrange = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args_getrange);
    defer allocator.free(result_getrange);

    // SUBSTR testkey 0 4
    const args_substr = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "testkey" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "4" },
    };
    const result_substr = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args_substr);
    defer allocator.free(result_substr);

    // Byte-for-byte identical
    try std.testing.expectEqualStrings(result_getrange, result_substr);
}

test "SUBSTR - non-existent key returns empty string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "5" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "SUBSTR - out of range indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("short", "Hi", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "short" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "20" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "SUBSTR - WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a list
    try storage.lpush("mylist", &[_][]const u8{"element"});

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "5" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "SUBSTR - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

test "SUBSTR - negative indices from end" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("alphabet", "ABCDEFGHIJ", null);

    // Last 3 characters
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "alphabet" },
        RespValue{ .bulk_string = "-3" },
        RespValue{ .bulk_string = "-1" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$3\r\nHIJ\r\n", result);
}

test "SUBSTR - zero-length string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("empty", "", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "empty" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "5" },
    };
    const result = try server_mod.dispatchCommand(allocator, storage, null, null, null, null, null, 0, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}
