const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const server_mod = @import("../src/server.zig");
const storage_mod = @import("../src/storage/memory.zig");

const RespValue = protocol.RespValue;
const Server = server_mod.Server;
const Storage = storage_mod.Storage;

/// Helper to parse RESP response
fn parseResp(allocator: std.mem.Allocator, data: []const u8) !RespValue {
    var parser = protocol.Parser.init(allocator);
    defer parser.deinit();
    return try parser.parse(data);
}

/// Helper to deinitialize RespValue recursively
fn deinitRespValue(allocator: std.mem.Allocator, value: RespValue) void {
    switch (value) {
        .array => |arr| {
            for (arr) |item| {
                deinitRespValue(allocator, item);
            }
            allocator.free(arr);
        },
        .bulk_string => |s| allocator.free(s),
        else => {},
    }
}

test "BF.SCANDUMP basic single chunk" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_scandump.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Create filter
    const reserve_cmd = "*4\r\n$10\r\nBF.RESERVE\r\n$6\r\nmybloom\r\n$4\r\n0.01\r\n$3\r\n100\r\n";
    const reserve_resp = try server.handleCommand(reserve_cmd);
    defer allocator.free(reserve_resp);

    // Add items
    const add_cmd = "*3\r\n$6\r\nBF.ADD\r\n$7\r\nmybloom\r\n$5\r\nitem1\r\n";
    const add_resp = try server.handleCommand(add_cmd);
    defer allocator.free(add_resp);

    // Scandump with iterator=0
    const scandump_cmd = "*3\r\n$11\r\nBF.SCANDUMP\r\n$7\r\nmybloom\r\n$1\r\n0\r\n";
    const scandump_resp = try server.handleCommand(scandump_cmd);
    defer allocator.free(scandump_resp);

    const parsed = try parseResp(allocator, scandump_resp);
    defer deinitRespValue(allocator, parsed);

    // Should return array [iterator, data]
    try std.testing.expect(parsed == .array);
    try std.testing.expectEqual(@as(usize, 2), parsed.array.len);

    // For small filter, iterator should be 0 (complete)
    try std.testing.expect(parsed.array[0] == .integer);
    try std.testing.expectEqual(@as(i64, 0), parsed.array[0].integer);

    // Data should be non-null
    try std.testing.expect(parsed.array[1] == .bulk_string);
}

test "BF.SCANDUMP nonexistent key" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_scandump_nokey.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Scandump nonexistent key
    const scandump_cmd = "*3\r\n$11\r\nBF.SCANDUMP\r\n$7\r\nmissing\r\n$1\r\n0\r\n";
    const scandump_resp = try server.handleCommand(scandump_cmd);
    defer allocator.free(scandump_resp);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, scandump_resp, "ERR") != null);
}

test "BF.SCANDUMP wrong type" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_scandump_wrongtype.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Create string key
    const set_cmd = "*3\r\n$3\r\nSET\r\n$7\r\nnotbloom\r\n$5\r\nvalue\r\n";
    const set_resp = try server.handleCommand(set_cmd);
    defer allocator.free(set_resp);

    // Scandump on string key
    const scandump_cmd = "*3\r\n$11\r\nBF.SCANDUMP\r\n$8\r\nnotbloom\r\n$1\r\n0\r\n";
    const scandump_resp = try server.handleCommand(scandump_cmd);
    defer allocator.free(scandump_resp);

    // Should return WRONGTYPE
    try std.testing.expect(std.mem.indexOf(u8, scandump_resp, "WRONGTYPE") != null);
}

test "BF.LOADCHUNK basic restore" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_loadchunk.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Create and dump filter
    const reserve_cmd = "*4\r\n$10\r\nBF.RESERVE\r\n$7\r\nsource\r\n$4\r\n0.01\r\n$3\r\n100\r\n";
    const reserve_resp = try server.handleCommand(reserve_cmd);
    defer allocator.free(reserve_resp);

    const add_cmd = "*3\r\n$6\r\nBF.ADD\r\n$6\r\nsource\r\n$5\r\napple\r\n";
    const add_resp = try server.handleCommand(add_cmd);
    defer allocator.free(add_resp);

    const scandump_cmd = "*3\r\n$11\r\nBF.SCANDUMP\r\n$6\r\nsource\r\n$1\r\n0\r\n";
    const scandump_resp = try server.handleCommand(scandump_cmd);
    defer allocator.free(scandump_resp);

    const parsed = try parseResp(allocator, scandump_resp);
    defer deinitRespValue(allocator, parsed);

    // Extract data chunk
    const data_chunk = parsed.array[1].bulk_string;

    // Load into new key
    var loadchunk_cmd_buf: [4096]u8 = undefined;
    const loadchunk_cmd = try std.fmt.bufPrint(&loadchunk_cmd_buf, "*4\r\n$12\r\nBF.LOADCHUNK\r\n$4\r\ndest\r\n$1\r\n0\r\n${d}\r\n", .{data_chunk.len});

    var full_cmd = std.ArrayList(u8).init(allocator);
    defer full_cmd.deinit(allocator);
    try full_cmd.appendSlice(allocator, loadchunk_cmd);
    try full_cmd.appendSlice(allocator, data_chunk);
    try full_cmd.appendSlice(allocator, "\r\n");

    const loadchunk_resp = try server.handleCommand(full_cmd.items);
    defer allocator.free(loadchunk_resp);

    // Should return OK
    try std.testing.expect(std.mem.indexOf(u8, loadchunk_resp, "+OK") != null);

    // Verify item exists in restored filter
    const exists_cmd = "*3\r\n$9\r\nBF.EXISTS\r\n$4\r\ndest\r\n$5\r\napple\r\n";
    const exists_resp = try server.handleCommand(exists_cmd);
    defer allocator.free(exists_resp);

    const exists_parsed = try parseResp(allocator, exists_resp);
    try std.testing.expectEqual(@as(i64, 1), exists_parsed.integer);
}

test "BF.SCANDUMP/LOADCHUNK round-trip preserves items" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_roundtrip.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Create filter with multiple items
    const reserve_cmd = "*4\r\n$10\r\nBF.RESERVE\r\n$6\r\nfilter1\r\n$4\r\n0.01\r\n$3\r\n100\r\n";
    const reserve_resp = try server.handleCommand(reserve_cmd);
    defer allocator.free(reserve_resp);

    const items = [_][]const u8{ "apple", "banana", "cherry", "date", "elderberry" };
    for (items) |item| {
        var add_cmd_buf: [256]u8 = undefined;
        const add_cmd = try std.fmt.bufPrint(&add_cmd_buf, "*3\r\n$6\r\nBF.ADD\r\n$7\r\nfilter1\r\n${d}\r\n{s}\r\n", .{ item.len, item });
        const add_resp = try server.handleCommand(add_cmd);
        defer allocator.free(add_resp);
    }

    // Scandump
    const scandump_cmd = "*3\r\n$11\r\nBF.SCANDUMP\r\n$7\r\nfilter1\r\n$1\r\n0\r\n";
    const scandump_resp = try server.handleCommand(scandump_cmd);
    defer allocator.free(scandump_resp);

    const parsed = try parseResp(allocator, scandump_resp);
    defer deinitRespValue(allocator, parsed);

    const data_chunk = parsed.array[1].bulk_string;

    // Loadchunk to new key
    var loadchunk_cmd_buf: [4096]u8 = undefined;
    const loadchunk_cmd = try std.fmt.bufPrint(&loadchunk_cmd_buf, "*4\r\n$12\r\nBF.LOADCHUNK\r\n$7\r\nfilter2\r\n$1\r\n0\r\n${d}\r\n", .{data_chunk.len});

    var full_cmd = std.ArrayList(u8).init(allocator);
    defer full_cmd.deinit(allocator);
    try full_cmd.appendSlice(allocator, loadchunk_cmd);
    try full_cmd.appendSlice(allocator, data_chunk);
    try full_cmd.appendSlice(allocator, "\r\n");

    const loadchunk_resp = try server.handleCommand(full_cmd.items);
    defer allocator.free(loadchunk_resp);

    // Verify all items exist in restored filter
    for (items) |item| {
        var exists_cmd_buf: [256]u8 = undefined;
        const exists_cmd = try std.fmt.bufPrint(&exists_cmd_buf, "*3\r\n$9\r\nBF.EXISTS\r\n$7\r\nfilter2\r\n${d}\r\n{s}\r\n", .{ item.len, item });
        const exists_resp = try server.handleCommand(exists_cmd);
        defer allocator.free(exists_resp);

        const exists_parsed = try parseResp(allocator, exists_resp);
        try std.testing.expectEqual(@as(i64, 1), exists_parsed.integer);
    }

    // Verify non-item doesn't exist
    const not_exists_cmd = "*3\r\n$9\r\nBF.EXISTS\r\n$7\r\nfilter2\r\n$5\r\ngrape\r\n";
    const not_exists_resp = try server.handleCommand(not_exists_cmd);
    defer allocator.free(not_exists_resp);

    const not_exists_parsed = try parseResp(allocator, not_exists_resp);
    try std.testing.expectEqual(@as(i64, 0), not_exists_parsed.integer);
}

test "BF.LOADCHUNK arity validation" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_loadchunk_arity.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Too few arguments
    const cmd = "*2\r\n$12\r\nBF.LOADCHUNK\r\n$3\r\nkey\r\n";
    const resp = try server.handleCommand(cmd);
    defer allocator.free(resp);

    try std.testing.expect(std.mem.indexOf(u8, resp, "ERR") != null);
}

test "BF.SCANDUMP arity validation" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_scandump_arity.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Too few arguments
    const cmd = "*2\r\n$11\r\nBF.SCANDUMP\r\n$3\r\nkey\r\n";
    const resp = try server.handleCommand(cmd);
    defer allocator.free(resp);

    try std.testing.expect(std.mem.indexOf(u8, resp, "ERR") != null);
}

test "BF.SCANDUMP invalid iterator" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "/tmp/zoltraak_test_scandump_baditer.rdb");
    defer storage.deinit();

    var server = try Server.init(allocator, storage, "127.0.0.1", 0);
    defer server.deinit();

    // Create filter
    const reserve_cmd = "*4\r\n$10\r\nBF.RESERVE\r\n$6\r\nmybloom\r\n$4\r\n0.01\r\n$3\r\n100\r\n";
    const reserve_resp = try server.handleCommand(reserve_cmd);
    defer allocator.free(reserve_resp);

    // Scandump with invalid iterator
    const scandump_cmd = "*3\r\n$11\r\nBF.SCANDUMP\r\n$7\r\nmybloom\r\n$7\r\ninvalid\r\n";
    const scandump_resp = try server.handleCommand(scandump_cmd);
    defer allocator.free(scandump_resp);

    try std.testing.expect(std.mem.indexOf(u8, scandump_resp, "ERR") != null);
}
